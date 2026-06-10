package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/anshul439/go-orchestrator/internal/config"
	"github.com/anshul439/go-orchestrator/internal/logger"
	pb "github.com/anshul439/go-orchestrator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	log := logger.NewLogger()
	cfg := config.LoadConfig()

	conn, err := grpc.NewClient(cfg.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("failed to connect to server", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewOrchestratorServiceClient(conn)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	for i := 1; i <= cfg.WorkerCount; i++ {
		wg.Add(1)
		go runWorker(ctx, client, i, log, &wg)
	}

	wg.Wait()
}

func runWorker(ctx context.Context, client pb.OrchestratorServiceClient, id int, log *slog.Logger, wg *sync.WaitGroup) {
	defer wg.Done()

	stream, err := client.Work(ctx)
	if err != nil {
		log.Error("failed to open work stream", slog.Int("worker_id", id), slog.String("error", err.Error()))
		return
	}

	workerID := fmt.Sprintf("worker-%d", id)

	for {
		if err := stream.Send(&pb.WorkerMessage{
			WorkerId: workerID,
			Payload:  &pb.WorkerMessage_Ready{Ready: &pb.ReadySignal{}},
		}); err != nil {
			return
		}

		msg, err := stream.Recv()
		if err != nil {
			return
		}

		task := msg.Payload.(*pb.ServerMessage_Task).Task

		log.Info("processing job",
			slog.String("worker_id", workerID),
			slog.Int("job_id", int(task.JobId)),
			slog.String("type", task.Type),
		)

		// simulate work
		time.Sleep(7 * time.Second)

		log.Info("simulating job execution",
			slog.Int("job_id", int(task.JobId)),
			slog.String("type", task.Type),
			slog.String("payload", task.Payload),
		)

		success := rand.Intn(2) != 0

		errMsg := ""
		if !success {
			errMsg = "simulated failure"
		}

		if err := stream.Send(&pb.WorkerMessage{
			WorkerId: workerID,
			Payload: &pb.WorkerMessage_Result{
				Result: &pb.TaskResult{
					JobId:   task.JobId,
					Success: success,
					Error:   errMsg,
				},
			},
		}); err != nil {
			return
		}
	}
}
