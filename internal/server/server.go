package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/queue"
	"github.com/Anshul439/Orqestra/internal/workflow"
	pb "github.com/Anshul439/Orqestra/proto"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	pb.UnimplementedOrchestratorServiceServer
	db       *pgxpool.Pool
	queue    queue.Queue
	registry *workflow.Registry
	lastSeen sync.Map
	owner    sync.Map
}

func New(db *pgxpool.Pool, q queue.Queue, registry *workflow.Registry) *Server {
	s := &Server{db: db, queue: q, registry: registry}
	go s.runReaper()
	return s
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
	var workerID string

	for {
		msg, err := stream.Recv()
		if err != nil {
			// disconnected mid-job: re-queue for another worker
			if inFlight != nil {
				s.clearOwnership(inFlight.ID)
				db.UpdateJobState(s.db, inFlight.ID, "pending", inFlight.RetryCount)
				s.queue.Retry(context.Background(), *inFlight, 0)
			}
			return err
		}

		workerID = msg.WorkerId

		switch p := msg.Payload.(type) {
		case *pb.WorkerMessage_Ready:
			job, err := s.queue.Consume(stream.Context())
			if err != nil {
				return err
			}

			// Set ownership before sending — if Send fails the disconnect path cleans up.
			s.owner.Store(job.ID, workerID)
			s.lastSeen.Store(job.ID, time.Now())

			if err := db.UpdateJobState(s.db, job.ID, "running", job.RetryCount); err != nil {
				s.clearOwnership(job.ID)
				return err
			}

			inFlight = &job

			if err := stream.Send(&pb.ServerMessage{
				Payload: &pb.ServerMessage_Task{
					Task: &pb.TaskAssignment{
						JobId:      int32(job.ID),
						Type:       job.Type,
						Payload:    job.Payload,
						RetryCount: int32(job.RetryCount),
						MaxRetries: int32(job.MaxRetries),
					},
				},
			}); err != nil {
				s.clearOwnership(job.ID)
				db.UpdateJobState(s.db, job.ID, "pending", job.RetryCount)
				s.queue.Retry(context.Background(), job, 0)
				return err
			}

		case *pb.WorkerMessage_Heartbeat:
			jobID := int(p.Heartbeat.JobId)
			if !s.recordHeartbeat(jobID, workerID) {
				continue
			}
			slog.Default().Debug("heartbeat received",
				slog.Int("job_id", jobID),
				slog.String("worker_id", workerID),
			)

		case *pb.WorkerMessage_Result:
			inFlight = nil
			s.handleResult(stream.Context(), p.Result, workerID)
		}
	}
}

func (s *Server) handleResult(ctx context.Context, result *pb.TaskResult, senderWorkerID string) {
	log := slog.Default()
	jobID := int(result.JobId)

	if !s.workerOwnsJob(jobID, senderWorkerID) {
		log.Warn("handleResult: ignoring result from non-owner worker",
			slog.Int("job_id", jobID),
			slog.String("sender", senderWorkerID),
		)
		return
	}

	row, err := db.GetJob(s.db, jobID)
	if err != nil {
		log.Error("handleResult: failed to get job", slog.Int("job_id", jobID), slog.String("error", err.Error()))
		s.clearOwnership(jobID)
		return
	}

	// Reject if already settled (cancel/reaper/duplicate result race).
	switch row.Status {
	case "cancelled", "completed", "failed":
		s.clearOwnership(jobID)
		return
	}

	if result.Success {
		s.queue.Ack(ctx, queue.Job{ID: jobID})
		if err := db.UpdateJobState(s.db, jobID, "completed", row.RetryCount); err != nil {
			log.Error("handleResult: failed to mark job completed", slog.Int("job_id", jobID), slog.String("error", err.Error()))
		}
		s.clearOwnership(jobID)

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
		s.clearOwnership(job.ID)
		s.queue.Retry(ctx, job, delay)
	} else {
		if err := db.UpdateJobState(s.db, job.ID, "failed", job.RetryCount); err != nil {
			log.Error("handleResult: failed to mark job failed", slog.Int("job_id", job.ID), slog.String("error", err.Error()))
		}
		s.clearOwnership(job.ID)
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

	s.clearOwnership(jobID)

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

func (s *Server) clearOwnership(jobID int) {
	s.lastSeen.Delete(jobID)
	s.owner.Delete(jobID)
}

func (s *Server) workerOwnsJob(jobID int, workerID string) bool {
	ownerVal, ok := s.owner.Load(jobID)
	return ok && ownerVal.(string) == workerID
}

func (s *Server) recordHeartbeat(jobID int, workerID string) bool {
	if !s.workerOwnsJob(jobID, workerID) {
		slog.Default().Warn("heartbeat ignored for non-owner worker",
			slog.Int("job_id", jobID),
			slog.String("worker_id", workerID),
		)
		return false
	}

	s.lastSeen.Store(jobID, time.Now())
	return true
}

func (s *Server) runReaper() {
	const reaperInterval = 10 * time.Second
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.reaperTick(time.Now())
	}
}

// reaperTick runs one sweep; callable directly from tests without the ticker.
func (s *Server) reaperTick(now time.Time) {
	const heartbeatTimeout = 30 * time.Second

	jobs, err := db.ListJobs(s.db, "running")
	if err != nil {
		slog.Default().Error("reaper: failed to list running jobs", slog.String("error", err.Error()))
		return
	}

	for _, job := range jobs {
		lastSeenVal, ok := s.lastSeen.Load(job.ID)
		if !ok {
			// No entry means the job predates this server instance; skip it.
			continue
		}
		if now.Sub(lastSeenVal.(time.Time)) < heartbeatTimeout {
			continue
		}

		ownerVal, ok := s.owner.Load(job.ID)
		if !ok {
			continue
		}

		slog.Default().Warn("reaper: reclaiming stale job",
			slog.Int("job_id", job.ID),
			slog.String("last_owner", ownerVal.(string)),
		)

		s.clearOwnership(job.ID)
		db.UpdateJobState(s.db, job.ID, "pending", job.RetryCount)
		s.queue.Retry(context.Background(), queue.Job{
			ID:         job.ID,
			Type:       job.Type,
			Payload:    job.Payload,
			RetryCount: job.RetryCount,
			MaxRetries: job.MaxRetries,
		}, 0)
	}
}
