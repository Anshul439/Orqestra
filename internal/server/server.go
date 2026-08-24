package server

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Anshul439/Orqestra/internal/db"
	"github.com/Anshul439/Orqestra/internal/queue"
	"github.com/Anshul439/Orqestra/internal/service"
	pb "github.com/Anshul439/Orqestra/proto"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	pb.UnimplementedOrchestratorServiceServer
	db    *pgxpool.Pool
	queue queue.Queue

	workflows *service.WorkflowService

	lastSeen sync.Map
	owner    sync.Map
}

func New(ctx context.Context, pool *pgxpool.Pool, q queue.Queue, workflows *service.WorkflowService) *Server {
	s := &Server{db: pool, queue: q, workflows: workflows}
	go s.runReaper(ctx)
	return s
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
		if err := db.CompleteJob(s.db, jobID, result.Output); err != nil {
			log.Error("handleResult: failed to complete job", slog.Int("job_id", jobID), slog.String("error", err.Error()))
		}
		s.clearOwnership(jobID)

		if row.WorkflowRunID != nil {
			s.workflows.Advance(ctx, *row.WorkflowRunID)
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

func (s *Server) runReaper(ctx context.Context) {
	const reaperInterval = 10 * time.Second
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reaperTick(time.Now())
		}
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
