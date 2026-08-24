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

	wf.Normalize()

	runID, err := db.CreateWorkflowRun(s.db, wf.Name, len(wf.Steps))
	if err != nil {
		return 0, err
	}

	for _, idx := range wf.UnblockedSteps(map[string]bool{}, map[int]bool{}) {
		if err := s.submitStep(ctx, runID, idx, wf, ""); err != nil {
			return 0, err
		}
	}

	return runID, nil
}

func (s *WorkflowService) ListWorkflows() []workflow.Workflow {
	return s.registry.List()
}

func (s *WorkflowService) GetWorkflowStatus(_ context.Context, runID int) (db.WorkflowRunRow, error) {
	return db.GetWorkflowRun(s.db, runID)
}

func (s *WorkflowService) Advance(ctx context.Context, runID int) {
	log := slog.Default()

	completedCount, totalSteps, workflowName, err := db.AdvanceWorkflowRun(s.db, runID)
	if err != nil {
		log.Error("advance: failed to increment step counter", slog.Int("run_id", runID), slog.String("error", err.Error()))
		return
	}

	if completedCount >= totalSteps {
		if err := db.CompleteWorkflowRun(s.db, runID); err != nil {
			log.Error("advance: failed to complete run", slog.Int("run_id", runID), slog.String("error", err.Error()))
		}
		return
	}

	wf, ok := s.registry.Get(workflowName)
	if !ok {
		if err := db.FailWorkflowRun(s.db, runID); err != nil {
			log.Error("advance: failed to fail run", slog.Int("run_id", runID), slog.String("error", err.Error()))
		}
		return
	}

	wf.Normalize()

	completedIndices, err := db.GetCompletedStepIndices(s.db, runID)
	if err != nil {
		log.Error("advance: failed to get completed step indices", slog.Int("run_id", runID), slog.String("error", err.Error()))
		return
	}
	completedIDs := make(map[string]bool, len(completedIndices))
	for idx := range completedIndices {
		if idx < len(wf.Steps) {
			completedIDs[wf.Steps[idx].ID] = true
		}
	}

	submitted, err := db.GetSubmittedStepIndices(s.db, runID)
	if err != nil {
		log.Error("advance: failed to get submitted step indices", slog.Int("run_id", runID), slog.String("error", err.Error()))
		return
	}

	for _, idx := range wf.UnblockedSteps(completedIDs, submitted) {
		prevOutput := s.previousOutputFor(runID, idx, wf)
		if err := s.submitStep(ctx, runID, idx, wf, prevOutput); err != nil {
			log.Error("advance: failed to submit step", slog.Int("run_id", runID), slog.Int("step", idx), slog.String("error", err.Error()))
		}
	}
}

// previousOutputFor returns the output of the single dependency of a step,
// for injecting as $PREVIOUS_OUTPUT. Returns "" for root steps or steps with
// multiple dependencies.
func (s *WorkflowService) previousOutputFor(runID, stepIndex int, wf workflow.Workflow) string {
	step := wf.Steps[stepIndex]
	if len(step.DependsOn) != 1 {
		return ""
	}
	depID := step.DependsOn[0]
	for i, ws := range wf.Steps {
		if ws.ID == depID {
			output, err := db.GetJobOutputByWorkflowStep(s.db, runID, i)
			if err != nil {
				slog.Default().Warn("advance: could not fetch dep output",
					slog.Int("run_id", runID),
					slog.String("dep", depID),
					slog.String("error", err.Error()),
				)
				return ""
			}
			return output
		}
	}
	return ""
}

func (s *WorkflowService) submitStep(ctx context.Context, runID, stepIndex int, wf workflow.Workflow, previousOutput string) error {
	step := wf.Steps[stepIndex]

	payload, err := json.Marshal(struct {
		Command        string `json:"command"`
		PreviousOutput string `json:"previous_output,omitempty"`
	}{Command: step.Command, PreviousOutput: previousOutput})
	if err != nil {
		return err
	}

	_, err = db.InsertWorkflowStep(ctx, s.db, runID, stepIndex, step.Retries, string(payload))
	return err
}
