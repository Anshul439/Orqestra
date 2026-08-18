package main

import (
	"bytes"
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
	"time"

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

	workerID := fmt.Sprintf("worker-%d-%d-%d", id, os.Getpid(), time.Now().UnixNano())
	var sendMu sync.Mutex

	sendMessage := func(msg *pb.WorkerMessage) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(msg)
	}

	for {
		if err := sendMessage(&pb.WorkerMessage{
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

		// Heartbeat goroutine: pings the server every 5s while the job runs.
		hbCtx, stopHeartbeat := context.WithCancel(ctx)
		hbDone := make(chan struct{})
		go func(jobID int32) {
			defer close(hbDone)
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					log.Info("sending heartbeat",
						slog.String("worker_id", workerID),
						slog.Int("job_id", int(jobID)),
					)
					if err := sendMessage(&pb.WorkerMessage{
						WorkerId: workerID,
						Payload: &pb.WorkerMessage_Heartbeat{
							Heartbeat: &pb.HeartbeatSignal{JobId: jobID},
						},
					}); err != nil {
						log.Warn("failed to send heartbeat",
							slog.String("worker_id", workerID),
							slog.Int("job_id", int(jobID)),
							slog.String("error", err.Error()),
						)
						return
					}
				case <-hbCtx.Done():
					return
				}
			}
		}(task.JobId)

		success, errMsg, output := executeJob(ctx, task.Payload, log)

		// Stop heartbeats before sending the result.
		stopHeartbeat()
		<-hbDone

		if err := sendMessage(&pb.WorkerMessage{
			WorkerId: workerID,
			Payload: &pb.WorkerMessage_Result{
				Result: &pb.TaskResult{
					JobId:   task.JobId,
					Success: success,
					Error:   errMsg,
					Output:  output,
				},
			},
		}); err != nil {
			return
		}
	}
}

// Returns (success, errMsg, stdout). stdout is only meaningful when success=true.
func executeJob(ctx context.Context, payload string, log *slog.Logger) (bool, string, string) {
	const maxPreviousOutputBytes = 64 * 1024 // 64 KB — env vars aren't a data bus

	var p struct {
		Command        string `json:"command"`
		PreviousOutput string `json:"previous_output"`
	}
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return false, fmt.Sprintf("invalid payload: %v", err), ""
	}
	if p.Command == "" {
		return false, "missing required field: command", ""
	}

	log.Info("executing command", slog.String("command", p.Command))

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "sh", "-c", p.Command)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if len(p.PreviousOutput) > maxPreviousOutputBytes {
		log.Warn("previous_output truncated before injection into PREVIOUS_OUTPUT env var",
			slog.Int("original_bytes", len(p.PreviousOutput)),
			slog.Int("limit_bytes", maxPreviousOutputBytes),
		)
		p.PreviousOutput = p.PreviousOutput[:maxPreviousOutputBytes]
	}
	cmd.Env = append(os.Environ(), "PREVIOUS_OUTPUT="+p.PreviousOutput)

	runErr := cmd.Run()

	stdoutStr := strings.TrimSpace(stdout.String())
	stderrStr := strings.TrimSpace(stderr.String())

	if stdoutStr != "" {
		for _, line := range strings.Split(stdoutStr, "\n") {
			log.Info(line)
		}
	}
	if stderrStr != "" {
		for _, line := range strings.Split(stderrStr, "\n") {
			log.Warn(line)
		}
	}

	if runErr != nil {
		return false, fmt.Sprintf("command failed: %v\n%s", runErr, stderrStr), ""
	}

	return true, "", stdoutStr
}
