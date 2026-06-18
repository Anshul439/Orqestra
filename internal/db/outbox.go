package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxEntry struct {
	ID         int
	JobID      int
	Type       string
	Payload    string
	MaxRetries int
}

func InsertJob(
	ctx context.Context,
	pool *pgxpool.Pool,
	maxRetries int,
	jobType string,
	payload string,
) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var jobID int
	err = tx.QueryRow(ctx, `
		INSERT INTO jobs (status, retry_count, max_retries, type, payload)
		VALUES ('pending', 0, $1, $2, $3)
		RETURNING id`,
		maxRetries, jobType, payload,
	).Scan(&jobID)
	if err != nil {
		return 0, err
	}

	if _, err = tx.Exec(ctx, `INSERT INTO job_outbox (job_id) VALUES ($1)`, jobID); err != nil {
		return 0, err
	}

	return jobID, tx.Commit(ctx)
}

func InsertWorkflowStep(
	ctx context.Context,
	pool *pgxpool.Pool,
	workflowRunID, stepIndex int,
	payload string,
) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var jobID int
	err = tx.QueryRow(ctx, `
		INSERT INTO jobs (status, retry_count, max_retries, type, payload, workflow_run_id, step_index)
		VALUES ('pending', 0, 0, 'shell', $1, $2, $3)
		RETURNING id`,
		payload, workflowRunID, stepIndex,
	).Scan(&jobID)
	if err != nil {
		return 0, err
	}

	if _, err = tx.Exec(ctx, `INSERT INTO job_outbox (job_id) VALUES ($1)`, jobID); err != nil {
		return 0, err
	}

	return jobID, tx.Commit(ctx)
}

func GetUnprocessedOutbox(ctx context.Context, pool *pgxpool.Pool) ([]OutboxEntry, error) {
	rows, err := pool.Query(ctx, `
		SELECT o.id, o.job_id, j.type, j.payload, j.max_retries
		FROM job_outbox o
		JOIN jobs j ON j.id = o.job_id
		WHERE o.processed = FALSE
		ORDER BY o.created_at`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		if err := rows.Scan(&e.ID, &e.JobID, &e.Type, &e.Payload, &e.MaxRetries); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func MarkOutboxProcessed(ctx context.Context, pool *pgxpool.Pool, outboxID int) error {
	_, err := pool.Exec(ctx, `
		UPDATE job_outbox
		SET processed = TRUE, processed_at = NOW()
		WHERE id = $1`,
		outboxID,
	)
	return err
}

func CancelOutboxEntry(ctx context.Context, pool *pgxpool.Pool, jobID int) error {
	_, err := pool.Exec(ctx, `
		UPDATE job_outbox
		SET processed = TRUE, processed_at = NOW()
		WHERE job_id = $1 AND processed = FALSE`,
		jobID,
	)
	return err
}
