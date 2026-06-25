package db_test

import (
	"context"
	"testing"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/testutil"
)

func TestGetUnprocessedOutbox_ReturnsOnlyUnprocessed(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()

	id1, _ := db.InsertJob(ctx, pool, 0, "shell", `{}`)
	id2, _ := db.InsertJob(ctx, pool, 0, "shell", `{}`)

	entries, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		t.Fatalf("GetUnprocessedOutbox: %v", err)
	}
	for _, e := range entries {
		if e.JobID == id1 {
			db.MarkOutboxProcessed(ctx, pool, e.ID)
		}
	}

	remaining, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		t.Fatalf("GetUnprocessedOutbox: %v", err)
	}
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

	entries, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		t.Fatalf("GetUnprocessedOutbox: %v", err)
	}
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

	entries, _ := db.GetUnprocessedOutbox(ctx, pool)
	if len(entries) != 1 {
		t.Fatalf("expected 1 unprocessed entry")
	}

	if err := db.MarkOutboxProcessed(ctx, pool, entries[0].ID); err != nil {
		t.Fatalf("MarkOutboxProcessed: %v", err)
	}

	after, _ := db.GetUnprocessedOutbox(ctx, pool)
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

	entries, _ := db.GetUnprocessedOutbox(ctx, pool)
	if len(entries) != 0 {
		t.Error("expected outbox to be cancelled (processed), still has unprocessed entries")
	}
}

func TestCancelOutboxEntry_AlreadyProcessed_IsNoOp(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	jobID, _ := db.InsertJob(ctx, pool, 0, "shell", `{}`)

	entries, _ := db.GetUnprocessedOutbox(ctx, pool)
	db.MarkOutboxProcessed(ctx, pool, entries[0].ID)

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

	jobID, err := db.InsertWorkflowStep(ctx, pool, runID, 0, `{"command":"echo step0"}`)
	if err != nil {
		t.Fatalf("InsertWorkflowStep: %v", err)
	}

	entries, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		t.Fatalf("GetUnprocessedOutbox: %v", err)
	}
	if len(entries) != 1 || entries[0].JobID != jobID {
		t.Errorf("expected outbox entry for workflow step %d, got %+v", jobID, entries)
	}
}
