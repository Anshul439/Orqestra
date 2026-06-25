package db_test

import (
	"context"
	"testing"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/testutil"
)

func TestInsertJob_CreatesJobAndOutboxEntry(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	jobID, err := db.InsertJob(ctx, pool, 3, "shell", `{"command":"echo test"}`)
	if err != nil {
		t.Fatalf("InsertJob: %v", err)
	}
	if jobID <= 0 {
		t.Fatalf("expected valid job ID, got %d", jobID)
	}

	entries, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		t.Fatalf("GetUnprocessedOutbox: %v", err)
	}
	if len(entries) != 1 || entries[0].JobID != jobID {
		t.Errorf("expected one outbox entry for job %d, got %+v", jobID, entries)
	}
}

func TestGetJob(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	jobID, err := db.InsertJob(ctx, pool, 2, "shell", `{"command":"ls"}`)
	if err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	row, err := db.GetJob(pool, jobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if row.ID != jobID {
		t.Errorf("ID: got %d, want %d", row.ID, jobID)
	}
	if row.Status != "pending" {
		t.Errorf("Status: got %q, want %q", row.Status, "pending")
	}
	if row.MaxRetries != 2 {
		t.Errorf("MaxRetries: got %d, want %d", row.MaxRetries, 2)
	}
}

func TestUpdateJobState(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	jobID, err := db.InsertJob(ctx, pool, 3, "shell", `{}`)
	if err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	if err := db.UpdateJobState(pool, jobID, "running", 0); err != nil {
		t.Fatalf("UpdateJobState: %v", err)
	}

	row, err := db.GetJob(pool, jobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if row.Status != "running" {
		t.Errorf("Status: got %q, want %q", row.Status, "running")
	}
}

func TestListJobs_FilterByStatus(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()
	id1, _ := db.InsertJob(ctx, pool, 0, "shell", `{}`)
	id2, _ := db.InsertJob(ctx, pool, 0, "shell", `{}`)

	db.UpdateJobState(pool, id1, "completed", 0)
	_ = id2 // stays pending

	pending, err := db.ListJobs(pool, "pending")
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != id2 {
		t.Errorf("expected 1 pending job (id=%d), got %+v", id2, pending)
	}

	completed, err := db.ListJobs(pool, "completed")
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(completed) != 1 || completed[0].ID != id1 {
		t.Errorf("expected 1 completed job (id=%d), got %+v", id1, completed)
	}
}

func TestResetRunningJobs_CreatesFreshOutboxEntries(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs")

	ctx := context.Background()

	// Simulate a job that has been dispatched and is currently running.
	jobID, err := db.InsertJob(ctx, pool, 3, "shell", `{}`)
	if err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	entries, _ := db.GetUnprocessedOutbox(ctx, pool)
	db.MarkOutboxProcessed(ctx, pool, entries[0].ID)

	db.UpdateJobState(pool, jobID, "running", 0)

	// Simulate server crash and restart
	if err := db.ResetRunningJobs(pool); err != nil {
		t.Fatalf("ResetRunningJobs: %v", err)
	}

	row, err := db.GetJob(pool, jobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if row.Status != "pending" {
		t.Errorf("Status after reset: got %q, want %q", row.Status, "pending")
	}

	fresh, err := db.GetUnprocessedOutbox(ctx, pool)
	if err != nil {
		t.Fatalf("GetUnprocessedOutbox: %v", err)
	}
	if len(fresh) != 1 || fresh[0].JobID != jobID {
		t.Errorf("expected fresh outbox entry for job %d, got %+v", jobID, fresh)
	}
}
