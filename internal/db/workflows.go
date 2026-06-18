package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WorkflowRunRow is the DB representation of a workflow run.
type WorkflowRunRow struct {
	ID           int
	WorkflowName string
	Status       string
	CurrentStep  int
	TotalSteps   int
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

// AdvanceWorkflowRun increments current_step by 1.
func AdvanceWorkflowRun(conn *pgxpool.Pool, id int) error {
	_, err := conn.Exec(
		context.Background(),
		`UPDATE workflow_runs SET current_step = current_step + 1 WHERE id = $1`,
		id,
	)
	return err
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
