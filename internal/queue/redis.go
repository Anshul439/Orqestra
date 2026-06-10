package queue

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	gredis "github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	client            *gredis.Client
	name              string
	timeout           time.Duration
	readyKey          string
	processingKey     string
	delayedKey        string
	payloadKey        string
	queuedSetKey      string
	schedulerInterval time.Duration
}

func NewRedisQueue(
	client *gredis.Client,
	name string,
	timeout time.Duration,
) *RedisQueue {

	return &RedisQueue{
		client:            client,
		name:              name,
		timeout:           timeout,
		readyKey:          name + ":ready",
		processingKey:     name + ":processing",
		delayedKey:        name + ":delayed",
		payloadKey:        name + ":payloads",
		queuedSetKey:      name + ":queued",
		schedulerInterval: time.Second,
	}
}

// Start launches the background scheduler that promotes delayed jobs to the ready queue.
func (q *RedisQueue) Start(ctx context.Context) {
	ticker := time.NewTicker(q.schedulerInterval)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				if err := q.promoteDueJobs(ctx); err != nil {
					if ctx.Err() != nil {
						return
					}
				}
			}
		}
	}()
}

func (q *RedisQueue) Recover(ctx context.Context) error {
	jobIDs, err := q.client.LRange(
		ctx,
		q.processingKey,
		0,
		-1,
	).Result()

	if err != nil {
		return err
	}

	if len(jobIDs) == 0 {
		return nil
	}

	pipe := q.client.TxPipeline()

	pipe.Del(ctx, q.processingKey)

	for _, jobID := range jobIDs {
		pipe.RPush(
			ctx,
			q.readyKey,
			jobID,
		)
	}

	_, err = pipe.Exec(ctx)

	return err
}

func (q *RedisQueue) Enqueue(
	ctx context.Context,
	job Job,
) error {
	payload, err := json.Marshal(job)

	if err != nil {
		return err
	}

	jobID := strconv.Itoa(job.ID)

	added, err := q.client.SAdd(
		ctx,
		q.queuedSetKey,
		jobID,
	).Result()

	if err != nil {
		return err
	}

	pipe := q.client.TxPipeline()

	pipe.HSet(
		ctx,
		q.payloadKey,
		jobID,
		payload,
	)

	// only push to ready list if not already queued (SAdd returns 0 on duplicate)
	if added == 1 {
		pipe.LPush(
			ctx,
			q.readyKey,
			jobID,
		)
	}

	_, err = pipe.Exec(ctx)

	return err
}

func (q *RedisQueue) Consume(
	ctx context.Context,
) (Job, error) {
	for {
		jobID, err := q.client.BRPopLPush(
			ctx,
			q.readyKey,
			q.processingKey,
			q.timeout,
		).Result()

		if err == nil {
			payload, getErr := q.client.HGet(
				ctx,
				q.payloadKey,
				jobID,
			).Result()

			if getErr != nil {
				return Job{}, getErr
			}

			var job Job

			err = json.Unmarshal(
				[]byte(payload),
				&job,
			)

			if err != nil {
				return Job{}, err
			}

			return job, nil
		}

		if errors.Is(err, context.Canceled) {
			return Job{}, ctx.Err()
		}

		// gredis.Nil means BRPopLPush timed out (no jobs), not an actual error
		if errors.Is(err, gredis.Nil) {
			if ctx.Err() != nil {
				return Job{}, ctx.Err()
			}

			continue
		}

		if ctx.Err() != nil {
			return Job{}, ctx.Err()
		}

		return Job{}, err
	}
}

func (q *RedisQueue) Ack(
	ctx context.Context,
	job Job,
) error {
	jobID := strconv.Itoa(job.ID)

	pipe := q.client.TxPipeline()

	pipe.LRem(
		ctx,
		q.processingKey,
		1,
		jobID,
	)
	pipe.SRem(
		ctx,
		q.queuedSetKey,
		jobID,
	)
	pipe.HDel(
		ctx,
		q.payloadKey,
		jobID,
	)
	pipe.ZRem(
		ctx,
		q.delayedKey,
		jobID,
	)

	_, err := pipe.Exec(ctx)

	return err
}

func (q *RedisQueue) Retry(
	ctx context.Context,
	job Job,
	delay time.Duration,
) error {
	payload, err := json.Marshal(job)

	if err != nil {
		return err
	}

	jobID := strconv.Itoa(job.ID)
	// score is Unix milliseconds — used by the sorted set to order jobs by execution time.
	score := float64(
		time.Now().Add(delay).UnixMilli(),
	)

	pipe := q.client.TxPipeline()

	pipe.HSet(
		ctx,
		q.payloadKey,
		jobID,
		payload,
	)
	pipe.LRem(
		ctx,
		q.processingKey,
		1,
		jobID,
	)
	pipe.SAdd(
		ctx,
		q.queuedSetKey,
		jobID,
	)
	pipe.ZAdd(
		ctx,
		q.delayedKey,
		gredis.Z{
			Score:  score,
			Member: jobID,
		},
	)

	_, err = pipe.Exec(ctx)

	return err
}

func (q *RedisQueue) Fail(
	ctx context.Context,
	job Job,
) error {
	jobID := strconv.Itoa(job.ID)

	pipe := q.client.TxPipeline()

	pipe.LRem(
		ctx,
		q.processingKey,
		1,
		jobID,
	)
	pipe.SRem(
		ctx,
		q.queuedSetKey,
		jobID,
	)
	pipe.HDel(
		ctx,
		q.payloadKey,
		jobID,
	)
	pipe.ZRem(
		ctx,
		q.delayedKey,
		jobID,
	)

	_, err := pipe.Exec(ctx)

	return err
}

func (q *RedisQueue) Cancel(ctx context.Context, job Job) error {
	jobID := strconv.Itoa(job.ID)

	pipe := q.client.TxPipeline()

	pipe.LRem(ctx, q.readyKey, 1, jobID)      // remove from main queue (if pending)
	pipe.LRem(ctx, q.processingKey, 1, jobID) // remove from processing (if running)
	pipe.SRem(ctx, q.queuedSetKey, jobID)
	pipe.HDel(ctx, q.payloadKey, jobID)
	pipe.ZRem(ctx, q.delayedKey, jobID)

	_, err := pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) promoteDueJobs(
	ctx context.Context,
) error {
	now := float64(time.Now().UnixMilli())

	jobIDs, err := q.client.ZRangeByScore(
		ctx,
		q.delayedKey,
		&gredis.ZRangeBy{
			Min: "-inf",
			Max: strconv.FormatFloat(now, 'f', 0, 64),
		},
	).Result()

	if err != nil {
		return err
	}

	if len(jobIDs) == 0 {
		return nil
	}

	pipe := q.client.TxPipeline()

	for _, jobID := range jobIDs {
		pipe.ZRem(
			ctx,
			q.delayedKey,
			jobID,
		)
		pipe.LPush(
			ctx,
			q.readyKey,
			jobID,
		)
	}

	_, err = pipe.Exec(ctx)

	return err
}

func (q *RedisQueue) Close() error {
	return q.client.Close()
}
