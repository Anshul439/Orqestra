package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pollInterval = 2 * time.Second

// Start runs the outbox relay loop, polling the outbox table every 2 seconds and
// forwarding unprocessed entries to the Redis queue. Blocks until ctx is cancelled.
func Start(ctx context.Context, pool *pgxpool.Pool, q queue.Queue) {
	log := slog.Default()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := relay(ctx, pool, q, log); err != nil {
				log.Error("outbox relay error", slog.String("error", err.Error()))
			}
		}
	}
}

func relay(ctx context.Context, pool *pgxpool.Pool, q queue.Queue, log *slog.Logger) error {
	entries, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		return err
	}

	for _, e := range entries {
		job := queue.Job{
			ID:         e.JobID,
			Type:       e.Type,
			Payload:    e.Payload,
			MaxRetries: e.MaxRetries,
		}
		if err := q.Enqueue(ctx, job); err != nil {
			log.Error("failed to enqueue job from outbox",
				slog.Int("job_id", e.JobID),
				slog.String("error", err.Error()),
			)
			continue
		}
		if err := db.MarkOutboxProcessed(ctx, pool, e.ID); err != nil {
			log.Error("failed to mark outbox entry processed",
				slog.Int("outbox_id", e.ID),
				slog.String("error", err.Error()),
			)
		}
	}
	return nil
}
