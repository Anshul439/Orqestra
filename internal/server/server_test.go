package server

import (
	"context"
	"testing"
	"time"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/queue"
	"github.com/Anshul439/Orqestra/internal/service"
	"github.com/Anshul439/Orqestra/internal/testutil"
	pb "github.com/Anshul439/Orqestra/proto"
)

type fakeQueue struct {
	acked    []queue.Job
	retried  []queue.Job
	failed   []queue.Job
	canceled []queue.Job
}

func (f *fakeQueue) Start(context.Context) {}

func (f *fakeQueue) Recover(context.Context) error {
	return nil
}

func (f *fakeQueue) Enqueue(context.Context, queue.Job) error {
	return nil
}

func (f *fakeQueue) Consume(context.Context) (queue.Job, error) {
	return queue.Job{}, context.Canceled
}

func (f *fakeQueue) Ack(_ context.Context, job queue.Job) error {
	f.acked = append(f.acked, job)
	return nil
}

func (f *fakeQueue) Retry(_ context.Context, job queue.Job, _ time.Duration) error {
	f.retried = append(f.retried, job)
	return nil
}

func (f *fakeQueue) Fail(_ context.Context, job queue.Job) error {
	f.failed = append(f.failed, job)
	return nil
}

func (f *fakeQueue) Cancel(_ context.Context, job queue.Job) error {
	f.canceled = append(f.canceled, job)
	return nil
}

func (f *fakeQueue) Close() error {
	return nil
}

func TestRetryDelay(t *testing.T) {
	cases := []struct {
		retryCount int
		want       time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
	}
	for _, c := range cases {
		got := retryDelay(c.retryCount)
		if got != c.want {
			t.Errorf("retryDelay(%d) = %v, want %v", c.retryCount, got, c.want)
		}
	}
}

func TestRecordHeartbeatRejectsNonOwner(t *testing.T) {
	s := &Server{}
	jobID := 42
	workerID := "worker-a"
	stale := time.Now().Add(-time.Minute)

	s.owner.Store(jobID, workerID)
	s.lastSeen.Store(jobID, stale)

	if ok := s.recordHeartbeat(jobID, "worker-b"); ok {
		t.Fatal("recordHeartbeat accepted heartbeat from non-owner worker")
	}

	got, ok := s.lastSeen.Load(jobID)
	if !ok {
		t.Fatal("lastSeen entry unexpectedly deleted")
	}
	if !got.(time.Time).Equal(stale) {
		t.Fatalf("lastSeen changed for non-owner heartbeat: got %v want %v", got, stale)
	}
}

func TestHandleResultIgnoresNonOwnerWorker(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	jobID, err := db.InsertJob(ctx, pool, 1, "shell", `{"command":"echo test"}`)
	if err != nil {
		t.Fatalf("InsertJob: %v", err)
	}
	if err := db.UpdateJobState(pool, jobID, "running", 0); err != nil {
		t.Fatalf("UpdateJobState: %v", err)
	}

	q := &fakeQueue{}
	s := &Server{db: pool, queue: q}
	s.owner.Store(jobID, "worker-owner")
	s.lastSeen.Store(jobID, time.Now())

	s.handleResult(ctx, &pb.TaskResult{
		JobId:   int32(jobID),
		Success: true,
	}, "worker-stale")

	row, err := db.GetJob(pool, jobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if row.Status != "running" {
		t.Fatalf("job status changed after stale result: got %q want %q", row.Status, "running")
	}
	if len(q.acked) != 0 {
		t.Fatalf("expected no ack for stale result, got %d", len(q.acked))
	}
}

func TestCancelJobClearsOwnership(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	jobID, err := db.InsertJob(ctx, pool, 1, "shell", `{"command":"echo test"}`)
	if err != nil {
		t.Fatalf("InsertJob: %v", err)
	}
	if err := db.UpdateJobState(pool, jobID, "running", 0); err != nil {
		t.Fatalf("UpdateJobState: %v", err)
	}

	q := &fakeQueue{}
	s := &Server{db: pool, queue: q, jobs: service.NewJobService(pool, q)}
	s.owner.Store(jobID, "worker-owner")
	s.lastSeen.Store(jobID, time.Now())

	resp, err := s.CancelJob(ctx, &pb.CancelJobRequest{JobId: int32(jobID)})
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if resp.Status != "cancelled" {
		t.Fatalf("CancelJob status = %q, want %q", resp.Status, "cancelled")
	}

	row, err := db.GetJob(pool, jobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if row.Status != "cancelled" {
		t.Fatalf("job status = %q, want %q", row.Status, "cancelled")
	}
	if _, ok := s.owner.Load(jobID); ok {
		t.Fatal("owner entry was not cleared on cancel")
	}
	if _, ok := s.lastSeen.Load(jobID); ok {
		t.Fatal("lastSeen entry was not cleared on cancel")
	}
	if len(q.canceled) != 1 || q.canceled[0].ID != jobID {
		t.Fatalf("expected one queue cancel for job %d, got %+v", jobID, q.canceled)
	}
}

func TestReaperReclaimsAndShieldRejectsStaleResult(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	jobID, err := db.InsertJob(ctx, pool, 3, "shell", `{"command":"sleep 60"}`)
	if err != nil {
		t.Fatalf("InsertJob: %v", err)
	}
	if err := db.UpdateJobState(pool, jobID, "running", 0); err != nil {
		t.Fatalf("UpdateJobState: %v", err)
	}

	q := &fakeQueue{}
	s := &Server{db: pool, queue: q}

	// Worker A owns the job but its heartbeat is stale.
	s.owner.Store(jobID, "worker-a")
	s.lastSeen.Store(jobID, time.Now().Add(-time.Minute))

	s.reaperTick(time.Now())

	if len(q.retried) != 1 || q.retried[0].ID != jobID {
		t.Fatalf("reaper did not re-queue job %d, got %+v", jobID, q.retried)
	}
	if _, ok := s.owner.Load(jobID); ok {
		t.Fatal("reaper did not clear owner after reclaim")
	}
	row, err := db.GetJob(pool, jobID)
	if err != nil {
		t.Fatalf("GetJob after reaper: %v", err)
	}
	if row.Status != "pending" {
		t.Fatalf("reaper did not reset job to pending, got %q", row.Status)
	}

	// Worker B takes over and completes the job.
	if err := db.UpdateJobState(pool, jobID, "running", 0); err != nil {
		t.Fatalf("UpdateJobState for worker-b: %v", err)
	}
	s.owner.Store(jobID, "worker-b")
	s.lastSeen.Store(jobID, time.Now())

	s.handleResult(ctx, &pb.TaskResult{JobId: int32(jobID), Success: true}, "worker-b")

	row, err = db.GetJob(pool, jobID)
	if err != nil {
		t.Fatalf("GetJob after worker-b: %v", err)
	}
	if row.Status != "completed" {
		t.Fatalf("worker-b result not accepted: got %q want %q", row.Status, "completed")
	}
	if len(q.acked) != 1 {
		t.Fatalf("expected 1 ack from worker-b, got %d", len(q.acked))
	}

	// Worker A wakes up late and submits its stale result — must be rejected.
	s.handleResult(ctx, &pb.TaskResult{JobId: int32(jobID), Success: true}, "worker-a")

	if len(q.acked) != 1 {
		t.Fatalf("shield failed: worker-a stale result was accepted (acked %d times)", len(q.acked))
	}
	row, err = db.GetJob(pool, jobID)
	if err != nil {
		t.Fatalf("GetJob after zombie worker-a: %v", err)
	}
	if row.Status != "completed" {
		t.Fatalf("zombie worker-a corrupted job status: got %q", row.Status)
	}
}
