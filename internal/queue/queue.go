package queue

import (
	"context"
	"time"
)

type Queue interface {
	Start(ctx context.Context)
	// Recover re-queues jobs left in processing from a previous crash.
	Recover(ctx context.Context) error
	Enqueue(ctx context.Context, job Job) error
	// Consume blocks until a job is available or ctx is cancelled.
	Consume(ctx context.Context) (Job, error)
	Ack(ctx context.Context, job Job) error
	Retry(ctx context.Context, job Job, delay time.Duration) error
	Fail(ctx context.Context, job Job) error
	Cancel(ctx context.Context, job Job) error
	Close() error
}
