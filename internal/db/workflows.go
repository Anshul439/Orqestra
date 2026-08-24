package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type WorkflowRunRow struct {
	ID           int    `json:"id"`
	WorkflowName string `json:"workflow_name"`
	Status       string `json:"status"`
	CurrentStep  int    `json:"current_step"`
	TotalSteps   int    `json:"total_steps"`
}

func CreateWorkflowRun(conn *pgxpool.Pool, name string, totalSteps int) (int, error) {
	var id int
	err := conn.QueryRow(
		context.Background(),
		`INSERT INTO workflow_runs (workflow_name, total_steps) VALUES ($1, $2) RETURNING id`,
		name, totalSteps,
	).Scan(&id)
	return id, err
}

func GetWorkflowRun(conn *pgxpool.Pool, id int) (WorkflowRunRow, error) {
	var row WorkflowRunRow
	err := conn.QueryRow(
		context.Background(),
		`SELECT id, workflow_name, status, current_step, total_steps FROM workflow_runs WHERE id = $1`,
		id,
	).Scan(&row.ID, &row.WorkflowName, &row.Status, &row.CurrentStep, &row.TotalSteps)
	return row, err
}

// AdvanceWorkflowRun increments the completed-step counter and returns the new
// count, total_steps, and workflow_name in one round-trip so the caller can
// detect completion and look up the workflow definition without a second query.
func AdvanceWorkflowRun(conn *pgxpool.Pool, id int) (completedCount, totalSteps int, workflowName string, err error) {
	err = conn.QueryRow(
		context.Background(),
		`UPDATE workflow_runs SET current_step = current_step + 1 WHERE id = $1 RETURNING current_step, total_steps, workflow_name`,
		id,
	).Scan(&completedCount, &totalSteps, &workflowName)
	return
}

func CompleteWorkflowRun(conn *pgxpool.Pool, id int) error {
	_, err := conn.Exec(
		context.Background(),
		`UPDATE workflow_runs SET status = 'completed' WHERE id = $1`,
		id,
	)
	return err
}

func FailWorkflowRun(conn *pgxpool.Pool, id int) error {
	_, err := conn.Exec(
		context.Background(),
		`UPDATE workflow_runs SET status = 'failed' WHERE id = $1`,
		id,
	)
	return err
}

func GetCompletedStepIndices(conn *pgxpool.Pool, runID int) (map[int]bool, error) {
	rows, err := conn.Query(
		context.Background(),
		`SELECT step_index FROM jobs WHERE workflow_run_id = $1 AND status = 'completed' AND step_index IS NOT NULL`,
		runID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int]bool)
	for rows.Next() {
		var idx int
		if err := rows.Scan(&idx); err != nil {
			return nil, err
		}
		result[idx] = true
	}
	return result, rows.Err()
}

func GetSubmittedStepIndices(conn *pgxpool.Pool, runID int) (map[int]bool, error) {
	rows, err := conn.Query(
		context.Background(),
		`SELECT step_index FROM jobs WHERE workflow_run_id = $1 AND step_index IS NOT NULL`,
		runID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int]bool)
	for rows.Next() {
		var idx int
		if err := rows.Scan(&idx); err != nil {
			return nil, err
		}
		result[idx] = true
	}
	return result, rows.Err()
}
