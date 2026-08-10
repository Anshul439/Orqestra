package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobService struct {
	db    *pgxpool.Pool
	queue queue.Queue
}

func NewJobService(pool *pgxpool.Pool, q queue.Queue) *JobService {
	return &JobService{db: pool, queue: q}
}

func (s *JobService) SubmitJob(ctx context.Context, maxRetries int, jobType, payload string) (int, error) {
	return db.InsertJob(ctx, s.db, maxRetries, jobType, payload)
}

func (s *JobService) GetJob(_ context.Context, jobID int) (db.JobRow, error) {
	return db.GetJob(s.db, jobID)
}

func (s *JobService) ListJobs(_ context.Context, status string) ([]db.JobRow, error) {
	return db.ListJobs(s.db, status)
}

func (s *JobService) CancelJob(ctx context.Context, jobID int) error {
	row, err := db.GetJob(s.db, jobID)
	if err != nil {
		return err
	}

	switch row.Status {
	case "completed", "failed", "cancelled":
		return fmt.Errorf("job %d cannot be cancelled: status is %s", jobID, row.Status)
	}

	if err := db.UpdateJobState(s.db, jobID, "cancelled", row.RetryCount); err != nil {
		return err
	}

	if err := db.CancelOutboxEntry(ctx, s.db, jobID); err != nil {
		slog.Default().Error("CancelJob: failed to cancel outbox entry",
			slog.Int("job_id", jobID), slog.String("error", err.Error()))
	}

	s.queue.Cancel(ctx, queue.Job{ID: jobID})
	return nil
}
