package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/service"
	"github.com/Anshul439/Orqestra/internal/testutil"
	"github.com/Anshul439/Orqestra/internal/workflow"
	"github.com/jackc/pgx/v5/pgxpool"
)

func pendingJobForStep(t *testing.T, pool *pgxpool.Pool, stepIndex int) db.JobRow {
	t.Helper()
	jobs, err := db.ListJobs(pool, "pending")
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	for _, j := range jobs {
		if j.StepIndex != nil && *j.StepIndex == stepIndex {
			return j
		}
	}
	t.Fatalf("no pending job found for step index %d", stepIndex)
	return db.JobRow{}
}

func TestTriggerWorkflow_Step0HasEmptyPreviousOutput(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs", "workflow_runs")

	registry := workflow.NewRegistry()
	registry.Register(workflow.Workflow{
		Name:  "first-step-wf",
		Steps: []workflow.Step{{Command: "echo hello"}},
	})

	svc := service.NewWorkflowService(pool, registry)
	ctx := context.Background()

	if _, err := svc.TriggerWorkflow(ctx, "first-step-wf"); err != nil {
		t.Fatalf("TriggerWorkflow: %v", err)
	}

	jobs, err := db.ListJobs(pool, "pending")
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 pending job, got %d", len(jobs))
	}

	var p struct {
		Command        string `json:"command"`
		PreviousOutput string `json:"previous_output"`
	}
	if err := json.Unmarshal([]byte(jobs[0].Payload), &p); err != nil {
		t.Fatalf("unmarshal step 0 payload: %v", err)
	}
	if p.Command != "echo hello" {
		t.Errorf("command = %q, want %q", p.Command, "echo hello")
	}
	if p.PreviousOutput != "" {
		t.Errorf("step 0 previous_output = %q, want empty", p.PreviousOutput)
	}
}

func TestAdvance_PassesPreviousOutputToNextStep(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs", "workflow_runs")

	registry := workflow.NewRegistry()
	registry.Register(workflow.Workflow{
		Name:  "chain-wf",
		Steps: []workflow.Step{{Command: "echo step0"}, {Command: "echo step1"}},
	})

	svc := service.NewWorkflowService(pool, registry)
	ctx := context.Background()

	runID, err := svc.TriggerWorkflow(ctx, "chain-wf")
	if err != nil {
		t.Fatalf("TriggerWorkflow: %v", err)
	}

	// Production flow: handleResult calls CompleteJob then Advance.
	// Replicate that here so Advance can read the completed-step set from DB.
	const wantOutput = "hello-from-step0"
	step0 := pendingJobForStep(t, pool, 0)
	if err := db.CompleteJob(pool, step0.ID, wantOutput); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}

	svc.Advance(ctx, runID)

	jobs, err := db.ListJobs(pool, "pending")
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}

	var step1Payload string
	for _, j := range jobs {
		if j.StepIndex != nil && *j.StepIndex == 1 {
			step1Payload = j.Payload
			break
		}
	}
	if step1Payload == "" {
		t.Fatal("no pending job found for step 1 after Advance")
	}

	var p struct {
		Command        string `json:"command"`
		PreviousOutput string `json:"previous_output"`
	}
	if err := json.Unmarshal([]byte(step1Payload), &p); err != nil {
		t.Fatalf("unmarshal step 1 payload: %v", err)
	}
	if p.Command != "echo step1" {
		t.Errorf("step 1 command = %q, want %q", p.Command, "echo step1")
	}
	if p.PreviousOutput != wantOutput {
		t.Errorf("step 1 previous_output = %q, want %q", p.PreviousOutput, wantOutput)
	}
}

func TestAdvance_CompletesRunOnLastStep(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs", "workflow_runs")

	registry := workflow.NewRegistry()
	registry.Register(workflow.Workflow{
		Name:  "single-step-wf",
		Steps: []workflow.Step{{Command: "echo only"}},
	})

	svc := service.NewWorkflowService(pool, registry)
	ctx := context.Background()

	runID, err := svc.TriggerWorkflow(ctx, "single-step-wf")
	if err != nil {
		t.Fatalf("TriggerWorkflow: %v", err)
	}

	step0 := pendingJobForStep(t, pool, 0)
	if err := db.CompleteJob(pool, step0.ID, "done"); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}

	svc.Advance(ctx, runID)

	run, err := db.GetWorkflowRun(pool, runID)
	if err != nil {
		t.Fatalf("GetWorkflowRun: %v", err)
	}
	if run.Status != "completed" {
		t.Errorf("run status = %q, want %q", run.Status, "completed")
	}
}

func TestTriggerWorkflow_DAGSubmitsAllRoots(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs", "workflow_runs")

	registry := workflow.NewRegistry()
	registry.Register(workflow.Workflow{
		Name: "dag-wf",
		Steps: []workflow.Step{
			{ID: "a", Command: "echo a"},
			{ID: "b", Command: "echo b"},
			{ID: "c", Command: "echo c", DependsOn: []string{"a", "b"}},
		},
	})

	svc := service.NewWorkflowService(pool, registry)
	ctx := context.Background()

	if _, err := svc.TriggerWorkflow(ctx, "dag-wf"); err != nil {
		t.Fatalf("TriggerWorkflow: %v", err)
	}

	jobs, err := db.ListJobs(pool, "pending")
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 pending jobs (roots a and b), got %d", len(jobs))
	}
}

func TestAdvance_DAGFanIn(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs", "workflow_runs")

	registry := workflow.NewRegistry()
	registry.Register(workflow.Workflow{
		Name: "fanin-wf",
		Steps: []workflow.Step{
			{ID: "a", Command: "echo a"},
			{ID: "b", Command: "echo b"},
			{ID: "merge", Command: "echo merge", DependsOn: []string{"a", "b"}},
		},
	})

	svc := service.NewWorkflowService(pool, registry)
	ctx := context.Background()

	runID, err := svc.TriggerWorkflow(ctx, "fanin-wf")
	if err != nil {
		t.Fatalf("TriggerWorkflow: %v", err)
	}

	jobA := pendingJobForStep(t, pool, 0)
	if err := db.CompleteJob(pool, jobA.ID, "out-a"); err != nil {
		t.Fatalf("CompleteJob a: %v", err)
	}
	svc.Advance(ctx, runID)

	pending, err := db.ListJobs(pool, "pending")
	if err != nil {
		t.Fatalf("ListJobs after a completes: %v", err)
	}
	for _, j := range pending {
		if j.StepIndex != nil && *j.StepIndex == 2 {
			t.Fatal("merge step submitted too early — b has not completed yet")
		}
	}

	jobB := pendingJobForStep(t, pool, 1)
	if err := db.CompleteJob(pool, jobB.ID, "out-b"); err != nil {
		t.Fatalf("CompleteJob b: %v", err)
	}
	svc.Advance(ctx, runID)

	pending, err = db.ListJobs(pool, "pending")
	if err != nil {
		t.Fatalf("ListJobs after b completes: %v", err)
	}
	var mergeFound bool
	for _, j := range pending {
		if j.StepIndex != nil && *j.StepIndex == 2 {
			mergeFound = true
			break
		}
	}
	if !mergeFound {
		t.Fatal("merge step not submitted after both a and b completed")
	}
}
