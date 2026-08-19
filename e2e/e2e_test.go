package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Anshul439/Orqestra/internal/api"
	"github.com/Anshul439/Orqestra/internal/outbox"
	"github.com/Anshul439/Orqestra/internal/queue"
	"github.com/Anshul439/Orqestra/internal/server"
	"github.com/Anshul439/Orqestra/internal/service"
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

type testEnv struct {
	ctx     context.Context
	baseURL string // HTTP management API
	dialer  func(context.Context, string) (net.Conn, error)
	lis     *bufconn.Listener
}

// newTestEnv spins up an in-process gRPC server (for workers) and an HTTP test
// server (for management), an outbox relay, and a Redis queue.
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	jobSvc := service.NewJobService(pool, q)
	workflowSvc := service.NewWorkflowService(pool, registry)

	// gRPC server — workers connect here via the Work() bidirectional stream.
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	pb.RegisterOrchestratorServiceServer(grpcSrv, server.New(ctx, pool, q, workflowSvc))
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.Stop)

	q.Start(ctx)
	go outbox.Start(ctx, pool, q)

	// HTTP server — management operations (submit, list, cancel, trigger, status).
	h := api.NewHandler(jobSvc, workflowSvc)
	httpSrv := httptest.NewServer(api.NewRouter(h))
	t.Cleanup(httpSrv.Close)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }

	return &testEnv{
		ctx:     ctx,
		baseURL: httpSrv.URL,
		dialer:  dialer,
		lis:     lis,
	}
}

func (e *testEnv) post(t *testing.T, path string, body string) map[string]any {
	t.Helper()
	var r *http.Response
	var err error
	if body != "" {
		r, err = http.Post(e.baseURL+path, "application/json", strings.NewReader(body))
	} else {
		r, err = http.Post(e.baseURL+path, "application/json", nil)
	}
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer r.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
		t.Fatalf("POST %s decode: %v", path, err)
	}
	if r.StatusCode >= 400 {
		t.Fatalf("POST %s: HTTP %d %v", path, r.StatusCode, result)
	}
	return result
}

func (e *testEnv) get(t *testing.T, path string) map[string]any {
	t.Helper()
	r, err := http.Get(e.baseURL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer r.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
		t.Fatalf("GET %s decode: %v", path, err)
	}
	if r.StatusCode >= 400 {
		t.Fatalf("GET %s: HTTP %d %v", path, r.StatusCode, result)
	}
	return result
}

func waitForJobStatus(t *testing.T, env *testEnv, jobID int, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		result := env.get(t, fmt.Sprintf("/api/v1/jobs/%d", jobID))
		if status, _ := result["status"].(string); status == want {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("job %d did not reach %q within deadline", jobID, want)
}

func waitForWorkflowStatus(t *testing.T, env *testEnv, runID int, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		result := env.get(t, fmt.Sprintf("/api/v1/workflows/runs/%d", runID))
		if status, _ := result["status"].(string); status == want {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("workflow run %d did not reach %q within deadline", runID, want)
}

func intResult(m map[string]any, key string) int {
	v, _ := m[key].(float64)
	return int(v)
}

func TestJobLifecycle(t *testing.T) {
	env := newTestEnv(t, workflow.NewRegistry(), "e2e_lifecycle")

	go runWorker(env.ctx, env.dialer, func(_ int) bool { return true })

	result := env.post(t, "/api/v1/jobs", `{"type":"shell","payload":"{\"command\":\"echo e2e-ok\"}","max_retries":0}`)
	jobID := intResult(result, "job_id")

	waitForJobStatus(t, env, jobID, "completed")
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

	result := env.post(t, "/api/v1/workflows/fail-wf/trigger", "")
	runID := intResult(result, "run_id")

	waitForWorkflowStatus(t, env, runID, "failed")
}

func TestJobRetryLifecycle(t *testing.T) {
	env := newTestEnv(t, workflow.NewRegistry(), "e2e_retry")

	go runWorker(env.ctx, env.dialer, func(_ int) bool { return false })

	result := env.post(t, "/api/v1/jobs", `{"type":"shell","payload":"{\"command\":\"echo this-will-fail\"}","max_retries":1}`)
	jobID := intResult(result, "job_id")

	waitForJobStatus(t, env, jobID, "failed")
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

// runWorkerWithOutput is like runWorker but also calls outputFn(n) to get the
// Output field of each result, and sends each raw task payload to payloads
// (non-blocking) so tests can inspect what the worker received.
func runWorkerWithOutput(
	ctx context.Context,
	dialer func(context.Context, string) (net.Conn, error),
	successFn func(n int) bool,
	outputFn func(n int) string,
	payloads chan<- string,
) {
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
			WorkerId: "e2e-chain-worker",
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

		select {
		case payloads <- task.Task.Payload:
		default:
		}

		out := ""
		if outputFn != nil {
			out = outputFn(num)
		}
		stream.Send(&pb.WorkerMessage{ //nolint:errcheck
			WorkerId: "e2e-chain-worker",
			Payload: &pb.WorkerMessage_Result{
				Result: &pb.TaskResult{
					JobId:   task.Task.JobId,
					Success: successFn(num),
					Output:  out,
				},
			},
		})
	}
}

func TestWorkflowOutputChaining(t *testing.T) {
	registry := workflow.NewRegistry()
	registry.Register(workflow.Workflow{
		Name: "chain-wf",
		Steps: []workflow.Step{
			{Command: "echo step0"},
			{Command: "echo step1"},
		},
	})

	env := newTestEnv(t, registry, "e2e_chain")

	// Buffer of 2 so the worker never blocks on channel writes.
	payloads := make(chan string, 2)
	go runWorkerWithOutput(
		env.ctx,
		env.dialer,
		func(_ int) bool { return true },
		func(n int) string {
			if n == 1 {
				return "chained-value"
			}
			return ""
		},
		payloads,
	)

	result := env.post(t, "/api/v1/workflows/chain-wf/trigger", "")
	runID := intResult(result, "run_id")

	waitForWorkflowStatus(t, env, runID, "completed")

	// Both payloads are in the buffer by the time the workflow is completed.
	collect := func() string {
		t.Helper()
		select {
		case p := <-payloads:
			return p
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for task payload")
			return ""
		}
	}
	step0Payload := collect()
	step1Payload := collect()

	var p0, p1 struct {
		PreviousOutput string `json:"previous_output"`
	}
	if err := json.Unmarshal([]byte(step0Payload), &p0); err != nil {
		t.Fatalf("unmarshal step 0 payload: %v", err)
	}
	if err := json.Unmarshal([]byte(step1Payload), &p1); err != nil {
		t.Fatalf("unmarshal step 1 payload: %v", err)
	}

	if p0.PreviousOutput != "" {
		t.Errorf("step 0 previous_output = %q, want empty", p0.PreviousOutput)
	}
	if p1.PreviousOutput != "chained-value" {
		t.Errorf("step 1 previous_output = %q, want %q", p1.PreviousOutput, "chained-value")
	}
}
