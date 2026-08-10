package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/workflow"
	"github.com/jackc/pgx/v5/pgxpool"
)

type WorkflowService struct {
	db       *pgxpool.Pool
	registry *workflow.Registry
}

func NewWorkflowService(pool *pgxpool.Pool, registry *workflow.Registry) *WorkflowService {
	return &WorkflowService{db: pool, registry: registry}
}

func (s *WorkflowService) TriggerWorkflow(ctx context.Context, name string) (int, error) {
	wf, ok := s.registry.Get(name)
	if !ok {
		return 0, fmt.Errorf("workflow %q not found", name)
	}

	runID, err := db.CreateWorkflowRun(s.db, wf.Name, len(wf.Steps))
	if err != nil {
		return 0, err
	}

	if err := s.submitStep(ctx, runID, 0, wf); err != nil {
		return 0, err
	}

	return runID, nil
}

func (s *WorkflowService) ListWorkflows() []workflow.Workflow {
	return s.registry.List()
}

func (s *WorkflowService) GetWorkflowStatus(_ context.Context, runID int) (db.WorkflowRunRow, error) {
	return db.GetWorkflowRun(s.db, runID)
}

func (s *WorkflowService) Advance(ctx context.Context, runID, completedStepIndex int) {
	log := slog.Default()

	run, err := db.GetWorkflowRun(s.db, runID)
	if err != nil {
		log.Error("advanceWorkflow: failed to get workflow run", slog.Int("run_id", runID), slog.String("error", err.Error()))
		return
	}

	nextStep := completedStepIndex + 1
	if nextStep >= run.TotalSteps {
		if err := db.AdvanceWorkflowRun(s.db, runID); err != nil {
			log.Error("advanceWorkflow: failed to advance run", slog.Int("run_id", runID), slog.String("error", err.Error()))
		}
		if err := db.CompleteWorkflowRun(s.db, runID); err != nil {
			log.Error("advanceWorkflow: failed to complete run", slog.Int("run_id", runID), slog.String("error", err.Error()))
		}
		return
	}

	if err := db.AdvanceWorkflowRun(s.db, runID); err != nil {
		log.Error("advanceWorkflow: failed to advance run", slog.Int("run_id", runID), slog.String("error", err.Error()))
	}

	wf, ok := s.registry.Get(run.WorkflowName)
	if !ok {
		if err := db.FailWorkflowRun(s.db, runID); err != nil {
			log.Error("advanceWorkflow: failed to fail run", slog.Int("run_id", runID), slog.String("error", err.Error()))
		}
		return
	}

	if err := s.submitStep(ctx, runID, nextStep, wf); err != nil {
		log.Error("advanceWorkflow: failed to submit next step", slog.Int("run_id", runID), slog.Int("step", nextStep), slog.String("error", err.Error()))
	}
}

func (s *WorkflowService) submitStep(ctx context.Context, runID, stepIndex int, wf workflow.Workflow) error {
	step := wf.Steps[stepIndex]

	payload, err := json.Marshal(struct {
		Command string `json:"command"`
	}{Command: step.Command})
	if err != nil {
		return err
	}

	_, err = db.InsertWorkflowStep(ctx, s.db, runID, stepIndex, string(payload))
	return err
}
