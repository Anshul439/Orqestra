package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anshul439/go-orchestrator/internal/config"
	"github.com/anshul439/go-orchestrator/internal/db"
	"github.com/anshul439/go-orchestrator/internal/logger"
	"github.com/anshul439/go-orchestrator/internal/outbox"
	"github.com/anshul439/go-orchestrator/internal/queue"
	"github.com/anshul439/go-orchestrator/internal/server"
	"github.com/anshul439/go-orchestrator/internal/workflow"
	pb "github.com/anshul439/go-orchestrator/proto"

	gredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func main() {
	log := logger.NewLogger()

	cfg := config.LoadConfig()

	ctx, cancel := context.WithCancel(context.Background())

	poolConn, err := db.NewPostgresPool(cfg.DBUrl)

	if err != nil {
		log.Error(
			"failed to connect to postgres",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	redisClient := gredis.NewClient(&gredis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	err = redisClient.Ping(ctx).Err()

	if err != nil {
		log.Error(
			"failed to connect to redis",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	log.Info("connected to postgres")
	log.Info("connected to redis")

	defer poolConn.Close()
	defer redisClient.Close()

	q := queue.NewRedisQueue(
		redisClient,
		cfg.RedisQueueName,
		time.Second,
	)

	if err := db.ResetRunningJobs(poolConn); err != nil {
		log.Error(
			"failed to reset running jobs",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	if err := q.Recover(ctx); err != nil {
		log.Error(
			"failed to recover redis queue",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	q.Start(ctx)
	go outbox.Start(ctx, poolConn, q)

	lis, err := net.Listen(
		"tcp",
		cfg.GRPCAddr,
	)

	if err != nil {
		log.Error(
			"failed to listen",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	registry := workflow.NewRegistry()
	if err := workflow.LoadFromDir(registry, "workflows/"); err != nil {
		log.Error("failed to load workflows", slog.String("error", err.Error()))
		os.Exit(1)
	}

	grpcSrv := grpc.NewServer()

	pb.RegisterOrchestratorServiceServer(
		grpcSrv,
		server.New(poolConn, q, registry),
	)

	go func() {
		log.Info(
			"gRPC server listening",
			slog.String("addr", cfg.GRPCAddr),
		)

		if err := grpcSrv.Serve(lis); err != nil {
			log.Error(
				"gRPC server failed",
				slog.String("error", err.Error()),
			)
		}
	}()

	signalChan := make(chan os.Signal, 1)

	signal.Notify(
		signalChan,
		os.Interrupt,
		syscall.SIGTERM,
	)

	<-signalChan

	cancel()

	grpcSrv.GracefulStop()
}
