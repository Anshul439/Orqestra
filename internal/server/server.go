package server

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/anshul439/go-orchestrator/internal/db"
	"github.com/anshul439/go-orchestrator/internal/queue"
	pb "github.com/anshul439/go-orchestrator/proto"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Server implements the generated OrchestratorServiceServer interface.
type Server struct {
	pb.UnimplementedOrchestratorServiceServer // required embed — handles unimplemented RPCs gracefully
	db                                        *pgxpool.Pool
	queue                                     queue.Queue
}

func New(db *pgxpool.Pool, q queue.Queue) *Server {
	return &Server{db: db, queue: q}
}

func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	jobID, err := db.InsertJob(s.db, int(req.MaxRetries), req.Type, req.Payload)
	if err != nil {
		return nil, err
	}

	job := queue.Job{ID: jobID, MaxRetries: int(req.MaxRetries), Type: req.Type, Payload: req.Payload}
	if err := s.queue.Enqueue(ctx, job); err != nil {
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
	jobID := int(result.JobId)

	row, err := db.GetJob(s.db, jobID)
	if err != nil {
		return
	}

	// Job was cancelled while the worker was still executing — ignore the result.
	if row.Status == "cancelled" {
		return
	}

	if result.Success {
		s.queue.Ack(ctx, queue.Job{ID: jobID})
		db.UpdateJobState(s.db, jobID, "completed", row.RetryCount)
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
		delay := time.Duration(math.Pow(2, float64(job.RetryCount))) * time.Second
		db.UpdateJobState(s.db, job.ID, "retrying", job.RetryCount)
		s.queue.Retry(ctx, job, delay)
	} else {
		db.UpdateJobState(s.db, job.ID, "failed", job.RetryCount)
		s.queue.Fail(ctx, job)
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

	s.queue.Cancel(ctx, queue.Job{ID: jobID})

	return &pb.CancelJobResponse{
		JobId:  int32(jobID),
		Status: "cancelled",
	}, nil
}
