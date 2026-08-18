package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Single UPDATE keeps status + output in sync — no partial-write window.
func CompleteJob(conn *pgxpool.Pool, jobID int, output string) error {
	_, err := conn.Exec(context.Background(), `
		UPDATE jobs
		SET status = 'completed', output = $1, updated_at = NOW()
		WHERE id = $2
	`, output, jobID)
	return err
}

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

// ResetRunningJobs resets any jobs left in the "running" state back to "pending" and
// creates fresh outbox entries for them. Called on startup to recover jobs that were
// in-flight when the server last crashed or restarted.
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

type JobRow struct {
	ID            int    `json:"id"`
	Status        string `json:"status"`
	RetryCount    int    `json:"retry_count"`
	MaxRetries    int    `json:"max_retries"`
	Type          string `json:"type"`
	Payload       string `json:"payload"`
	Output        string `json:"output"`
	WorkflowRunID *int   `json:"-"`
	StepIndex     *int   `json:"-"`
}

func GetJob(conn *pgxpool.Pool, jobID int) (JobRow, error) {
	var row JobRow
	query := `SELECT id, status, retry_count, max_retries, type, payload, COALESCE(output, ''), workflow_run_id, step_index
	          FROM jobs WHERE id = $1`
	err := conn.QueryRow(context.Background(), query, jobID).
		Scan(&row.ID, &row.Status, &row.RetryCount, &row.MaxRetries, &row.Type, &row.Payload,
			&row.Output, &row.WorkflowRunID, &row.StepIndex)
	return row, err
}

func ListJobs(db *pgxpool.Pool, status string) ([]JobRow, error) {
	query := `SELECT id, status, retry_count, max_retries, type, payload, COALESCE(output, ''), workflow_run_id, step_index FROM jobs`
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
			&j.Output, &j.WorkflowRunID, &j.StepIndex); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}
