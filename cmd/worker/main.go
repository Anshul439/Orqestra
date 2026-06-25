package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Anshul439/Orqestra/internal/config"
	"github.com/Anshul439/Orqestra/internal/logger"
	pb "github.com/Anshul439/Orqestra/proto"
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

		taskMsg, ok := msg.Payload.(*pb.ServerMessage_Task)
		if !ok {
			continue
		}
		task := taskMsg.Task

		log.Info("received job",
			slog.String("worker_id", workerID),
			slog.Int("job_id", int(task.JobId)),
			slog.String("type", task.Type),
		)

		success, errMsg := executeJob(ctx, task.Payload, log)

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

func executeJob(ctx context.Context, payload string, log *slog.Logger) (bool, string) {
	var p struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return false, fmt.Sprintf("invalid payload: %v", err)
	}
	if p.Command == "" {
		return false, "missing required field: command"
	}

	log.Info("executing command", slog.String("command", p.Command))

	cmd := exec.CommandContext(ctx, "sh", "-c", p.Command)
	out, err := cmd.CombinedOutput()

	output := strings.TrimSpace(string(out))
	if output != "" {
		log.Info("command output", slog.String("output", output))
	}

	if err != nil {
		return false, fmt.Sprintf("command failed: %v\n%s", err, output)
	}

	return true, ""
}
