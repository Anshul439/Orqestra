package e2e_test

import (
	"context"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	gredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Anshul439/Orqestra/internal/outbox"
	"github.com/Anshul439/Orqestra/internal/queue"
	"github.com/Anshul439/Orqestra/internal/server"
	"github.com/Anshul439/Orqestra/internal/testutil"
	"github.com/Anshul439/Orqestra/internal/workflow"
	pb "github.com/Anshul439/Orqestra/proto"
)

func redisAddr() string {
	if addr := os.Getenv("TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// testEnv holds everything an E2E test needs.
type testEnv struct {
	ctx    context.Context
	client pb.OrchestratorServiceClient
	dialer func(context.Context, string) (net.Conn, error)
	lis    *bufconn.Listener
}

// newTestEnv spins up an in-process gRPC server, outbox relay, and Redis queue.
// All resources are cleaned up via t.Cleanup.
func newTestEnv(t *testing.T, registry *workflow.Registry, queueName string) *testEnv {
	t.Helper()

	pool := testutil.NewPool(t)
	testutil.Truncate(t, pool, "job_outbox", "jobs", "workflow_runs")

	rdb := gredis.NewClient(&gredis.Options{Addr: redisAddr()})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("skipping: Redis not reachable: %v", err)
	}
	t.Cleanup(func() {
		for _, k := range []string{":ready", ":processing", ":payloads", ":queued"} {
			rdb.Del(context.Background(), queueName+k)
		}
		rdb.Close()
	})

	q := queue.NewRedisQueue(rdb, queueName, time.Second)

	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	pb.RegisterOrchestratorServiceServer(grpcSrv, server.New(pool, q, registry))
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	q.Start(ctx)
	go outbox.Start(ctx, pool, q)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return &testEnv{
		ctx:    ctx,
		client: pb.NewOrchestratorServiceClient(conn),
		dialer: dialer,
		lis:    lis,
	}
}

func TestJobLifecycle(t *testing.T) {
	env := newTestEnv(t, workflow.NewRegistry(), "e2e_lifecycle")

	go runWorker(env.ctx, env.dialer, func(_ int) bool { return true })

	resp, err := env.client.SubmitJob(env.ctx, &pb.SubmitJobRequest{
		Type: "shell", Payload: `{"command":"echo e2e-ok"}`, MaxRetries: 0,
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	waitForJobStatus(t, env.ctx, env.client, resp.JobId, "completed")
}

func TestWorkflowFailure(t *testing.T) {
	registry := workflow.NewRegistry()
	registry.Register(workflow.Workflow{
		Name: "fail-wf",
		Steps: []workflow.Step{
			{Command: "echo step0"},
			{Command: "echo step1"},
		},
	})

	env := newTestEnv(t, registry, "e2e_wf_fail")

	go runWorker(env.ctx, env.dialer, func(n int) bool { return n == 1 })

	resp, err := env.client.TriggerWorkflow(env.ctx, &pb.TriggerWorkflowRequest{Name: "fail-wf"})
	if err != nil {
		t.Fatalf("TriggerWorkflow: %v", err)
	}

	waitForWorkflowStatus(t, env.ctx, env.client, resp.RunId, "failed")
}

func TestJobRetryLifecycle(t *testing.T) {
	env := newTestEnv(t, workflow.NewRegistry(), "e2e_retry")

	go runWorker(env.ctx, env.dialer, func(_ int) bool { return false })

	resp, err := env.client.SubmitJob(env.ctx, &pb.SubmitJobRequest{
		Type:       "shell",
		Payload:    `{"command":"echo this-will-fail"}`,
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	waitForJobStatus(t, env.ctx, env.client, resp.JobId, "failed")
}

// runWorker starts a lightweight test worker that continuously requests tasks
// and reports success or failure according to successFn(n), where n is the
// 1-indexed call count.
func runWorker(ctx context.Context, dialer func(context.Context, string) (net.Conn, error), successFn func(n int) bool) {
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return
	}
	defer conn.Close()

	stream, err := pb.NewOrchestratorServiceClient(conn).Work(ctx)
	if err != nil {
		return
	}

	var n atomic.Int32
	for {
		if err := stream.Send(&pb.WorkerMessage{
			WorkerId: "e2e-worker",
			Payload:  &pb.WorkerMessage_Ready{Ready: &pb.ReadySignal{}},
		}); err != nil {
			return
		}
		msg, err := stream.Recv()
		if err != nil {
			return
		}
		task, ok := msg.Payload.(*pb.ServerMessage_Task)
		if !ok {
			continue
		}
		num := int(n.Add(1))
		stream.Send(&pb.WorkerMessage{ //nolint:errcheck
			WorkerId: "e2e-worker",
			Payload: &pb.WorkerMessage_Result{
				Result: &pb.TaskResult{
					JobId:   task.Task.JobId,
					Success: successFn(num),
				},
			},
		})
	}
}

func waitForJobStatus(t *testing.T, ctx context.Context, client pb.OrchestratorServiceClient, jobID int32, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		job, err := client.GetJob(ctx, &pb.GetJobRequest{JobId: jobID})
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if job.Status == want {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("job %d did not reach %q within deadline", jobID, want)
}

func waitForWorkflowStatus(t *testing.T, ctx context.Context, client pb.OrchestratorServiceClient, runID int32, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		run, err := client.GetWorkflowStatus(ctx, &pb.GetWorkflowStatusRequest{RunId: runID})
		if err != nil {
			t.Fatalf("GetWorkflowStatus: %v", err)
		}
		if run.Status == want {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("workflow run %d did not reach %q within deadline", runID, want)
}
