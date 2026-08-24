package db_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/testutil"
)

func TestGetUnprocessedOutbox_ReturnsOnlyUnprocessed(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()

	id1, _ := db.InsertJob(ctx, pool, 0, "shell", `{}`)
	id2, _ := db.InsertJob(ctx, pool, 0, "shell", `{}`)

	entries, tx, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		t.Fatalf("GetUnprocessedOutbox: %v", err)
	}
	for _, e := range entries {
		if e.JobID == id1 {
			db.MarkOutboxProcessed(ctx, tx, e.ID)
		}
	}
	tx.Commit(ctx)

	remaining, tx2, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		t.Fatalf("GetUnprocessedOutbox: %v", err)
	}
	defer tx2.Commit(ctx)

	if len(remaining) != 1 || remaining[0].JobID != id2 {
		t.Errorf("expected only job %d unprocessed, got %+v", id2, remaining)
	}
}

func TestGetUnprocessedOutbox_JoinsJobData(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	_, err := db.InsertJob(ctx, pool, 5, "shell", `{"command":"echo hi"}`)
	if err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	entries, tx, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		t.Fatalf("GetUnprocessedOutbox: %v", err)
	}
	defer tx.Rollback(ctx)

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	e := entries[0]
	if e.Type != "shell" {
		t.Errorf("Type: got %q, want %q", e.Type, "shell")
	}
	if e.MaxRetries != 5 {
		t.Errorf("MaxRetries: got %d, want %d", e.MaxRetries, 5)
	}
}

func TestMarkOutboxProcessed(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	db.InsertJob(ctx, pool, 0, "shell", `{}`)

	entries, tx, _ := db.GetUnprocessedOutbox(ctx, pool)
	if len(entries) != 1 {
		t.Fatalf("expected 1 unprocessed entry")
	}

	if err := db.MarkOutboxProcessed(ctx, tx, entries[0].ID); err != nil {
		t.Fatalf("MarkOutboxProcessed: %v", err)
	}
	tx.Commit(ctx)

	after, tx2, _ := db.GetUnprocessedOutbox(ctx, pool)
	defer tx2.Commit(ctx)
	if len(after) != 0 {
		t.Errorf("expected empty outbox after marking processed, got %d entries", len(after))
	}
}

func TestCancelOutboxEntry_MarksProcessed(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	jobID, _ := db.InsertJob(ctx, pool, 0, "shell", `{}`)

	if err := db.CancelOutboxEntry(ctx, pool, jobID); err != nil {
		t.Fatalf("CancelOutboxEntry: %v", err)
	}

	entries, tx, _ := db.GetUnprocessedOutbox(ctx, pool)
	defer tx.Commit(ctx)
	if len(entries) != 0 {
		t.Error("expected outbox to be cancelled (processed), still has unprocessed entries")
	}
}

func TestCancelOutboxEntry_AlreadyProcessed_IsNoOp(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	jobID, _ := db.InsertJob(ctx, pool, 0, "shell", `{}`)

	entries, tx, _ := db.GetUnprocessedOutbox(ctx, pool)
	db.MarkOutboxProcessed(ctx, tx, entries[0].ID)
	tx.Commit(ctx)

	if err := db.CancelOutboxEntry(ctx, pool, jobID); err != nil {
		t.Fatalf("CancelOutboxEntry on already-processed entry: %v", err)
	}
}

func TestInsertWorkflowStep_CreatesOutboxEntry(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs", "workflow_runs")

	ctx := context.Background()

	runID, err := db.CreateWorkflowRun(pool, "test-wf", 2)
	if err != nil {
		t.Fatalf("CreateWorkflowRun: %v", err)
	}

	jobID, err := db.InsertWorkflowStep(ctx, pool, runID, 0, 0, `{"command":"echo step0"}`)
	if err != nil {
		t.Fatalf("InsertWorkflowStep: %v", err)
	}

	entries, tx, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		t.Fatalf("GetUnprocessedOutbox: %v", err)
	}
	defer tx.Commit(ctx)

	if len(entries) != 1 || entries[0].JobID != jobID {
		t.Errorf("expected outbox entry for workflow step %d, got %+v", jobID, entries)
	}
}

func TestGetUnprocessedOutbox_SkipLockedPreventsRace(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	db.InsertJob(ctx, pool, 0, "shell", `{}`)

	entriesA, txA, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		t.Fatalf("caller A GetUnprocessedOutbox: %v", err)
	}
	if len(entriesA) != 1 {
		t.Fatalf("caller A: expected 1 entry, got %d", len(entriesA))
	}

	// Keep txA open to hold the lock, then run B concurrently.
	// B must see 0 rows — SKIP LOCKED skips anything already locked by A.
	var entriesB []db.OutboxEntry
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var txB interface {
			Commit(context.Context) error
		}
		entriesB, txB, err = db.GetUnprocessedOutbox(ctx, pool)
		if txB != nil {
			txB.Commit(ctx)
		}
	}()
	wg.Wait()

	txA.Rollback(ctx)

	if len(entriesB) != 0 {
		t.Errorf("SKIP LOCKED failed: caller B saw %d entries that should have been locked by caller A", len(entriesB))
	}
}

func TestCleanupProcessedOutbox(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()

	id1, _ := db.InsertJob(ctx, pool, 0, "shell", `{}`)
	id2, _ := db.InsertJob(ctx, pool, 0, "shell", `{}`)
	id3, _ := db.InsertJob(ctx, pool, 0, "shell", `{}`)
	_ = id3

	entries, tx, _ := db.GetUnprocessedOutbox(ctx, pool)
	var outboxID1, outboxID2 int
	for _, e := range entries {
		if e.JobID == id1 {
			outboxID1 = e.ID
			db.MarkOutboxProcessed(ctx, tx, e.ID)
		}
		if e.JobID == id2 {
			outboxID2 = e.ID
			db.MarkOutboxProcessed(ctx, tx, e.ID)
		}
	}
	tx.Commit(ctx)

	// Backdate id1 so it falls outside the 24h cleanup window.
	_, err := pool.Exec(ctx,
		"UPDATE job_outbox SET processed_at = NOW() - interval '25 hours' WHERE id = $1",
		outboxID1,
	)
	if err != nil {
		t.Fatalf("failed to backdate processed_at: %v", err)
	}

	deleted, err := db.CleanupProcessedOutbox(ctx, pool, 24*time.Hour)
	if err != nil {
		t.Fatalf("CleanupProcessedOutbox: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted row, got %d", deleted)
	}

	var count int
	pool.QueryRow(ctx, "SELECT COUNT(*) FROM job_outbox WHERE id = $1", outboxID1).Scan(&count)
	if count != 0 {
		t.Errorf("expected outboxID1 row to be deleted")
	}

	pool.QueryRow(ctx, "SELECT COUNT(*) FROM job_outbox WHERE id = $1", outboxID2).Scan(&count)
	if count != 1 {
		t.Errorf("expected outboxID2 row to remain")
	}

	unprocessed, tx2, _ := db.GetUnprocessedOutbox(ctx, pool)
	defer tx2.Commit(ctx)
	if len(unprocessed) != 1 || unprocessed[0].JobID != id3 {
		t.Errorf("expected id3 to remain unprocessed, got %+v", unprocessed)
	}
}
