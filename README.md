# Orqestra

Orqestra is a distributed job orchestrator written in Go for reliable background job execution. It uses bidirectional gRPC to coordinate distributed workers, Postgres as the source of truth, and Redis for queueing and delayed retries.

## Quick Start

The fastest way to run the full stack is with Docker Compose.

```bash
# Copy env config
cp .env.example .env

# Start Postgres, Redis, server, and worker
task docker:up

# Submit a job
task submit -- "echo hello world"

# Inspect jobs
task list
task status -- <job-id>
```

The gRPC server is exposed on `localhost:50051`, so the CLI works from your host machine while the stack runs in Docker.

## Highlights

- gRPC API for job submission, inspection, listing, cancellation, and workflow triggers
- CLI client for local development and operator workflows
- Distributed workers connected to the server over bidirectional gRPC streams
- Workers execute shell commands using Go's `os/exec` package, capturing stdout and stderr separately
- Completed job stdout is persisted and accessible via the CLI and API
- Sequential YAML workflows for multi-step jobs
- Exponential backoff retries with configurable retry limits
- Transactional outbox pattern for crash-safe job dispatch
- Crash recovery for jobs interrupted by server or worker failures
- Durable job persistence backed by Postgres

## Architecture

```text
cmd/cli  ──gRPC──►  cmd/server  ◄──gRPC stream──  cmd/worker
                        ├── internal/server     (gRPC handlers + workflow engine)
                        ├── internal/workflow   (workflow registry)
                        ├── internal/outbox     (relay: Postgres → Redis)
                        ├── internal/queue      (Redis queue)
                        └── internal/db         (Postgres)
```

## Reliability

Jobs are written to Postgres and an outbox table atomically, then asynchronously relayed to Redis. That keeps Postgres as the source of truth and prevents jobs from being lost if the server crashes between persistence and enqueue.

Failed jobs are retried with exponential backoff, while interrupted jobs are automatically recovered when the server restarts.

## Common Commands

```bash
# Start the core processes locally
task server
task worker

# Submit jobs
task submit -- "echo hello world"
task submit RETRIES=5 -- "go test ./..."

# Inspect and control jobs
task list
task list STATUS=running
task status -- <job-id>
task cancel -- <job-id>

# Workflow commands
task workflow:list
task trigger -- docker_demo
task workflow:status -- <run-id>

# Reset local queue + database state
task cleanup
```

If you need to submit a raw JSON payload directly, use the advanced command:

```bash
task cli:submit PAYLOAD='{"command":"echo hello world"}'
```

## Docker

Docker is the recommended local setup because it starts Postgres, Redis, the server, and the worker together.

```bash
task docker:logs
task docker:ps
task docker:down
```

Assuming the stack is already running via `task docker:up`, these commands help you inspect and manage it.

To inspect the backing services directly:

```bash
docker compose exec postgres psql -U postgres orchestrator
docker compose exec redis redis-cli
```

## Local Development

If you want to run without Docker:

```bash
# Create database
createdb -U postgres orchestrator

# Copy env config
cp .env.example .env

# Apply migrations
task migrate:up
```

Then start the server and worker in separate terminals:

```bash
# Terminal 1
task server

# Terminal 2
task worker
```

`WORKER_COUNT` in `.env` controls how many worker goroutines the worker process spawns.

For hot reload during development:

```bash
task dev:server
task dev:worker
```

## Workflows

Workflows are named sequences of shell commands executed in order. If any step fails, the workflow stops and the run is marked `failed`.

Workflow steps execute sequentially. If a step fails after exhausting its retries, the remaining steps are not scheduled.

Drop `.yaml` files into the `workflows/` directory and restart the server. When using Docker, the directory is volume-mounted, so `docker compose restart server` is enough after changes.

Workflow format:

```yaml
name: ci
steps:
  - command: go test ./...
  - command: go build ./...
```

Example workflows included in `workflows/`:

| File | What it does |
|---|---|
| `ci.yaml` | Runs `go test ./...` then `go build ./...` |
| `docker_demo.yaml` | Queries the local Docker daemon (`ps`, `images`, `system df`) |
| `data_pipeline.yaml` | Hits the GitHub REST API, downloads JSON, and parses a field |

## Testing

The test suite has three tiers. Integration and end-to-end tests require local Postgres and Redis.

```bash
task test
task test:unit
task test:integration
task test:e2e
```

| Layer | Location | What it covers |
|---|---|---|
| Unit | `internal/workflow`, `internal/server` | YAML parsing, workflow registry, exponential backoff |
| DB integration | `internal/db` | Job CRUD, outbox lifecycle, crash recovery, workflow step dispatch |
| Relay | `internal/outbox` | Relay happy path, Redis failure handling, processed entry skipping |
| E2E | `e2e/` | Full job lifecycle, workflow abort on step failure, retry exhaustion |

Unit tests always run offline. Integration and E2E tests are skipped automatically when dependencies are unavailable.

## Migrations

```bash
task migrate:create NAME=<name>
task migrate:up
task migrate:down
task migrate:version
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `DB_URL` | — | Postgres connection string |
| `WORKER_COUNT` | — | Number of concurrent workers |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `REDIS_PASSWORD` | — | Redis password |
| `REDIS_DB` | `0` | Redis DB index |
| `REDIS_QUEUE_NAME` | `jobs` | Redis key prefix for queues |
| `GRPC_ADDR` | `:50051` | gRPC server listen address |

The CLI also reads `GRPC_ADDR`. If it is set to a listen-style value like `:50051`, the CLI treats it as `localhost:50051`.

## Regenerating Proto

If you update `proto/orchestrator.proto`, regenerate the Go bindings with:

```bash
task proto
```

You will need `protoc-gen-go` and `protoc-gen-go-grpc` in your `PATH`:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```
