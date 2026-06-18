package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func UpdateJobState(
	conn *pgxpool.Pool,
	jobID int,
	state string,
	retryCount int,

) error {

	query := `
		UPDATE jobs
		SET
			status = $1,
			retry_count = $2,
			updated_at = NOW()
		WHERE id = $3
	`

	_, err := conn.Exec(
		context.Background(),
		query,
		state,
		retryCount,
		jobID,
	)

	return err
}

func ResetRunningJobs(conn *pgxpool.Pool) error {
	_, err := conn.Exec(context.Background(), `
		WITH reset AS (
			UPDATE jobs
			SET status = 'pending', updated_at = NOW()
			WHERE status = 'running'
			RETURNING id
		)
		INSERT INTO job_outbox (job_id)
		SELECT r.id FROM reset r
		WHERE NOT EXISTS (
			SELECT 1 FROM job_outbox o
			WHERE o.job_id = r.id AND o.processed = FALSE
		)
	`)
	return err
}

// JobRow is the DB representation of a job.
// Distinct from queue.Job, which is the lightweight in-memory struct used by the queue and workers.
type JobRow struct {
	ID            int
	Status        string
	RetryCount    int
	MaxRetries    int
	Type          string
	Payload       string
	WorkflowRunID *int // nil for regular jobs
	StepIndex     *int // nil for regular jobs
}

func GetJob(conn *pgxpool.Pool, jobID int) (JobRow, error) {
	var row JobRow
	query := `SELECT id, status, retry_count, max_retries, type, payload, workflow_run_id, step_index
	          FROM jobs WHERE id = $1`
	err := conn.QueryRow(context.Background(), query, jobID).
		Scan(&row.ID, &row.Status, &row.RetryCount, &row.MaxRetries, &row.Type, &row.Payload,
			&row.WorkflowRunID, &row.StepIndex)
	return row, err
}

func ListJobs(db *pgxpool.Pool, status string) ([]JobRow, error) {
	query := `SELECT id, status, retry_count, max_retries, type, payload, workflow_run_id, step_index FROM jobs`
	args := []any{}

	if status != "" {
		query += ` WHERE status = $1`
		args = append(args, status)
	}

	query += ` ORDER BY id DESC`

	rows, err := db.Query(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []JobRow
	for rows.Next() {
		var j JobRow
		if err := rows.Scan(&j.ID, &j.Status, &j.RetryCount, &j.MaxRetries, &j.Type, &j.Payload,
			&j.WorkflowRunID, &j.StepIndex); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}
