package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/service"
	"github.com/Anshul439/Orqestra/internal/testutil"
	"github.com/Anshul439/Orqestra/internal/workflow"
)

func TestTriggerWorkflow_Step0HasEmptyPreviousOutput(t *testing.T) {
	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs", "workflow_runs")

	registry := workflow.NewRegistry()
	registry.Register(workflow.Workflow{
		Name: "first-step-wf",
		Steps: []workflow.Step{
			{Command: "echo hello"},
		},
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
		Name: "chain-wf",
		Steps: []workflow.Step{
			{Command: "echo step0"},
			{Command: "echo step1"},
		},
	})

	svc := service.NewWorkflowService(pool, registry)
	ctx := context.Background()

	runID, err := svc.TriggerWorkflow(ctx, "chain-wf")
	if err != nil {
		t.Fatalf("TriggerWorkflow: %v", err)
	}

	const wantOutput = "hello-from-step0"
	svc.Advance(ctx, runID, 0, wantOutput)

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
		Name: "single-step-wf",
		Steps: []workflow.Step{
			{Command: "echo only"},
		},
	})

	svc := service.NewWorkflowService(pool, registry)
	ctx := context.Background()

	runID, err := svc.TriggerWorkflow(ctx, "single-step-wf")
	if err != nil {
		t.Fatalf("TriggerWorkflow: %v", err)
	}

	svc.Advance(ctx, runID, 0, "done")

	run, err := db.GetWorkflowRun(pool, runID)
	if err != nil {
		t.Fatalf("GetWorkflowRun: %v", err)
	}
	if run.Status != "completed" {
		t.Errorf("run status = %q, want %q", run.Status, "completed")
	}
}
