package outbox

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/queue"
	"github.com/Anshul439/Orqestra/internal/testutil"
)

// mockQueue captures calls to Enqueue and optionally fails.
type mockQueue struct {
	mu       sync.Mutex
	enqueued []queue.Job
	failNext bool
}

func (m *mockQueue) Enqueue(_ context.Context, job queue.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return errors.New("redis unavailable")
	}
	m.enqueued = append(m.enqueued, job)
	return nil
}

func (m *mockQueue) Start(_ context.Context)                                     {}
func (m *mockQueue) Recover(_ context.Context) error                             { return nil }
func (m *mockQueue) Consume(_ context.Context) (queue.Job, error)                { return queue.Job{}, nil }
func (m *mockQueue) Ack(_ context.Context, _ queue.Job) error                    { return nil }
func (m *mockQueue) Retry(_ context.Context, _ queue.Job, _ time.Duration) error { return nil }
func (m *mockQueue) Fail(_ context.Context, _ queue.Job) error                   { return nil }
func (m *mockQueue) Cancel(_ context.Context, _ queue.Job) error                 { return nil }
func (m *mockQueue) Close() error                                                { return nil }

func TestRelay_PushesUnprocessedToRedis(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	jobID, err := db.InsertJob(ctx, pool, 0, "shell", `{"command":"echo relay"}`)
	if err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	mq := &mockQueue{}
	if err := relay(ctx, pool, mq, slog.Default()); err != nil {
		t.Fatalf("relay: %v", err)
	}

	mq.mu.Lock()
	defer mq.mu.Unlock()

	if len(mq.enqueued) != 1 || mq.enqueued[0].ID != jobID {
		t.Errorf("expected job %d enqueued, got %+v", jobID, mq.enqueued)
	}

	remaining, _ := db.GetUnprocessedOutbox(ctx, pool)
	if len(remaining) != 0 {
		t.Errorf("expected empty outbox after relay, got %d entries", len(remaining))
	}
}

func TestRelay_LeavesUnprocessedOnEnqueueFailure(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	db.InsertJob(ctx, pool, 0, "shell", `{}`)

	mq := &mockQueue{failNext: true}
	if err := relay(ctx, pool, mq, slog.Default()); err != nil {
		t.Fatalf("relay: %v", err)
	}

	remaining, _ := db.GetUnprocessedOutbox(ctx, pool)
	if len(remaining) != 1 {
		t.Errorf("expected 1 unprocessed entry after failed enqueue, got %d", len(remaining))
	}
}

func TestRelay_SkipsAlreadyProcessed(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	db.InsertJob(ctx, pool, 0, "shell", `{}`)

	entries, _ := db.GetUnprocessedOutbox(ctx, pool)
	db.MarkOutboxProcessed(ctx, pool, entries[0].ID)

	mq := &mockQueue{}
	relay(ctx, pool, mq, slog.Default())

	mq.mu.Lock()
	defer mq.mu.Unlock()

	if len(mq.enqueued) != 0 {
		t.Errorf("relay should not re-enqueue already-processed entry, got %d enqueues", len(mq.enqueued))
	}
}
