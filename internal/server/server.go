package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/queue"
	"github.com/Anshul439/Orqestra/internal/workflow"
	pb "github.com/Anshul439/Orqestra/proto"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Server implements the generated OrchestratorServiceServer interface.
type Server struct {
	pb.UnimplementedOrchestratorServiceServer
	db       *pgxpool.Pool
	queue    queue.Queue
	registry *workflow.Registry
}

func New(db *pgxpool.Pool, q queue.Queue, registry *workflow.Registry) *Server {
	return &Server{db: db, queue: q, registry: registry}
}

func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	jobID, err := db.InsertJob(ctx, s.db, int(req.MaxRetries), req.Type, req.Payload)
	if err != nil {
		return nil, err
	}
	return &pb.SubmitJobResponse{JobId: int32(jobID)}, nil
}

func (s *Server) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	job, err := db.GetJob(s.db, int(req.JobId))
	if err != nil {
		return nil, err
	}

	return &pb.GetJobResponse{
		JobId:      int32(job.ID),
		Status:     job.Status,
		RetryCount: int32(job.RetryCount),
		MaxRetries: int32(job.MaxRetries),
		Type:       job.Type,
		Payload:    job.Payload,
	}, nil

}

func (s *Server) Work(stream pb.OrchestratorService_WorkServer) error {
	var inFlight *queue.Job

	for {
		msg, err := stream.Recv()
		if err != nil {
			if inFlight != nil {
				db.UpdateJobState(s.db, inFlight.ID, "pending", inFlight.RetryCount)
				s.queue.Retry(context.Background(), *inFlight, 0)
			}
			return err
		}

		switch p := msg.Payload.(type) {
		case *pb.WorkerMessage_Ready:
			job, err := s.queue.Consume(stream.Context())
			if err != nil {
				return err
			}

			if err := db.UpdateJobState(s.db, job.ID, "running", job.RetryCount); err != nil {
				return err
			}

			inFlight = &job

			stream.Send(&pb.ServerMessage{
				Payload: &pb.ServerMessage_Task{
					Task: &pb.TaskAssignment{
						JobId:      int32(job.ID),
						Type:       job.Type,
						Payload:    job.Payload,
						RetryCount: int32(job.RetryCount),
						MaxRetries: int32(job.MaxRetries),
					},
				},
			})

		case *pb.WorkerMessage_Result:
			inFlight = nil
			s.handleResult(stream.Context(), p.Result)
		}
	}
}

func (s *Server) handleResult(ctx context.Context, result *pb.TaskResult) {
	log := slog.Default()
	jobID := int(result.JobId)

	row, err := db.GetJob(s.db, jobID)
	if err != nil {
		log.Error("handleResult: failed to get job", slog.Int("job_id", jobID), slog.String("error", err.Error()))
		return
	}

	if row.Status == "cancelled" {
		return
	}

	if result.Success {
		s.queue.Ack(ctx, queue.Job{ID: jobID})
		if err := db.UpdateJobState(s.db, jobID, "completed", row.RetryCount); err != nil {
			log.Error("handleResult: failed to mark job completed", slog.Int("job_id", jobID), slog.String("error", err.Error()))
		}

		if row.WorkflowRunID != nil {
			s.advanceWorkflow(ctx, *row.WorkflowRunID, *row.StepIndex)
		}
		return
	}

	job := queue.Job{
		ID:         row.ID,
		Type:       row.Type,
		Payload:    row.Payload,
		RetryCount: row.RetryCount,
		MaxRetries: row.MaxRetries,
	}

	if job.RetryCount < job.MaxRetries {
		job.RetryCount++
		delay := retryDelay(job.RetryCount)
		if err := db.UpdateJobState(s.db, job.ID, "retrying", job.RetryCount); err != nil {
			log.Error("handleResult: failed to mark job retrying", slog.Int("job_id", job.ID), slog.String("error", err.Error()))
		}
		s.queue.Retry(ctx, job, delay)
	} else {
		if err := db.UpdateJobState(s.db, job.ID, "failed", job.RetryCount); err != nil {
			log.Error("handleResult: failed to mark job failed", slog.Int("job_id", job.ID), slog.String("error", err.Error()))
		}
		s.queue.Fail(ctx, job)

		if row.WorkflowRunID != nil {
			if err := db.FailWorkflowRun(s.db, *row.WorkflowRunID); err != nil {
				log.Error("handleResult: failed to fail workflow run", slog.Int("run_id", *row.WorkflowRunID), slog.String("error", err.Error()))
			}
		}
	}
}

func (s *Server) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	jobs, err := db.ListJobs(s.db, req.Status)
	if err != nil {
		return nil, err
	}

	var resp []*pb.GetJobResponse
	for _, j := range jobs {
		resp = append(resp, &pb.GetJobResponse{
			JobId:      int32(j.ID),
			Status:     j.Status,
			RetryCount: int32(j.RetryCount),
			MaxRetries: int32(j.MaxRetries),
			Type:       j.Type,
			Payload:    j.Payload,
		})
	}

	return &pb.ListJobsResponse{Jobs: resp}, nil
}

func (s *Server) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	jobID := int(req.JobId)

	row, err := db.GetJob(s.db, jobID)
	if err != nil {
		return nil, err
	}

	switch row.Status {
	case "completed", "failed", "cancelled":
		return nil, fmt.Errorf("job %d cannot be cancelled: status is %s", jobID, row.Status)
	}

	if err := db.UpdateJobState(s.db, jobID, "cancelled", row.RetryCount); err != nil {
		return nil, err
	}

	if err := db.CancelOutboxEntry(ctx, s.db, jobID); err != nil {
		slog.Default().Error("CancelJob: failed to cancel outbox entry", slog.Int("job_id", jobID), slog.String("error", err.Error()))
	}

	s.queue.Cancel(ctx, queue.Job{ID: jobID})

	return &pb.CancelJobResponse{
		JobId:  int32(jobID),
		Status: "cancelled",
	}, nil
}

func (s *Server) TriggerWorkflow(ctx context.Context, req *pb.TriggerWorkflowRequest) (*pb.TriggerWorkflowResponse, error) {
	wf, ok := s.registry.Get(req.Name)
	if !ok {
		return nil, fmt.Errorf("workflow %q not found", req.Name)
	}

	runID, err := db.CreateWorkflowRun(s.db, wf.Name, len(wf.Steps))
	if err != nil {
		return nil, err
	}

	if err := s.submitWorkflowStep(ctx, runID, 0, wf); err != nil {
		return nil, err
	}

	return &pb.TriggerWorkflowResponse{RunId: int32(runID)}, nil
}

func (s *Server) ListWorkflows(ctx context.Context, req *pb.ListWorkflowsRequest) (*pb.ListWorkflowsResponse, error) {
	var infos []*pb.WorkflowInfo
	for _, wf := range s.registry.List() {
		infos = append(infos, &pb.WorkflowInfo{
			Name:      wf.Name,
			StepCount: int32(len(wf.Steps)),
		})
	}
	return &pb.ListWorkflowsResponse{Workflows: infos}, nil
}

func (s *Server) GetWorkflowStatus(ctx context.Context, req *pb.GetWorkflowStatusRequest) (*pb.GetWorkflowStatusResponse, error) {
	run, err := db.GetWorkflowRun(s.db, int(req.RunId))
	if err != nil {
		return nil, err
	}
	return &pb.GetWorkflowStatusResponse{
		RunId:        int32(run.ID),
		WorkflowName: run.WorkflowName,
		Status:       run.Status,
		CurrentStep:  int32(run.CurrentStep),
		TotalSteps:   int32(run.TotalSteps),
	}, nil
}

func (s *Server) submitWorkflowStep(ctx context.Context, runID, stepIndex int, wf workflow.Workflow) error {
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

// advanceWorkflow moves the workflow to the next step or marks it complete/failed.
func (s *Server) advanceWorkflow(ctx context.Context, runID, completedStepIndex int) {
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

	if err := s.submitWorkflowStep(ctx, runID, nextStep, wf); err != nil {
		log.Error("advanceWorkflow: failed to submit next step", slog.Int("run_id", runID), slog.Int("step", nextStep), slog.String("error", err.Error()))
	}
}
