# go-orchestrator

A distributed job orchestrator built in Go. The server coordinates job assignment across distributed worker processes via bidirectional gRPC streaming, with Redis-backed queuing and Postgres persistence.

## Features

- Real command execution — workers run shell commands via `exec.Command`
- Sequential workflow engine — chain multiple commands into named workflows
- gRPC API for job submission, inspection, listing, and cancellation
- CLI client (`cmd/cli`) for submitting, inspecting, listing, and cancelling jobs
- Distributed workers (`cmd/worker`) — separate processes communicating with the server via bidirectional gRPC streams
- Redis-backed queue with reliable delivery (`BRPOPLPUSH` pattern)
- Postgres-backed job persistence
- Exponential backoff retry with configurable max retries
- Delayed job scheduling via Redis sorted sets
- Crash recovery — in-flight jobs are automatically re-queued when a worker disconnects unexpectedly, no server restart needed
- Job cancellation — cancel pending or running jobs via CLI
- Docker Compose setup — one command to run the full stack
- Structured logging (`log/slog`)
- Graceful shutdown

## Architecture

```
cmd/cli  ──gRPC──►  cmd/server  ◄──gRPC stream──  cmd/worker
                        ├── internal/server     (gRPC handlers + workflow engine)
                        ├── internal/workflow   (workflow registry)
                        ├── internal/queue      (Redis queue)
                        └── internal/db         (Postgres)
```

The server and workers are separate processes. Workers connect to the server over a persistent
bidirectional stream, receive job assignments, execute them, and report results back.

## Requirements

- Go 1.26+
- PostgreSQL
- Redis
- [Task](https://taskfile.dev/)
- [`golang-migrate`](https://github.com/golang-migrate/migrate)
- `protoc` + Go plugins (only needed to regenerate proto)

## Setup

The recommended way to run the full stack is via Docker (see [Docker](#docker) below).

For local development without Docker:

```bash
# Create database
createdb -U postgres orchestrator

# Copy env config
cp .env.example .env

# Apply migrations
task migrate:up
```

## Configuration

| Variable          | Default              | Description                        |
|-------------------|----------------------|------------------------------------|
| `DB_URL`          | —                    | Postgres connection string         |
| `WORKER_COUNT`    | —                    | Number of concurrent workers       |
| `REDIS_ADDR`      | `localhost:6379`     | Redis address                      |
| `REDIS_PASSWORD`  | —                    | Redis password (leave blank if none) |
| `REDIS_DB`        | `0`                  | Redis DB index                     |
| `REDIS_QUEUE_NAME`| `jobs`               | Redis key prefix for queues        |
| `GRPC_ADDR`       | `:50051`             | gRPC server listen address         |

## Docker

The easiest way to run the full stack. No local Postgres or Redis needed.

```bash
# Build images and start everything (postgres, redis, server, worker)
task docker:up

# Follow logs
task docker:logs

# Stop and remove containers
task docker:down
```

The server's gRPC port is exposed on `localhost:50051`, so you can still use the CLI from your host machine:

```bash
go run ./cmd/cli submit --type=shell --payload='{"command":"echo hello"}'
go run ./cmd/cli list
```

Exposed ports:

| Service | Port  |
|---------|-------|
| Server  | 50051 |

Postgres and Redis are only accessible within the Docker network. To inspect them directly:

```bash
docker compose exec postgres psql -U postgres orchestrator
docker compose exec redis redis-cli
```

## Migrations

Create a new migration:

```bash
task migrate:create NAME=<name>
```

Apply all pending migrations:

```bash
task migrate:up
```

Roll back the most recent migration:

```bash
task migrate:down
```

Check the current migration version:

```bash
task migrate:version
```

## Running

The server and workers are separate processes. Workers execute shell commands on the machine they run on. Start them in separate terminals:

```bash
# Terminal 1 — start the gRPC server
task server

# Terminal 2 — start the workers (connects to server via gRPC stream)
task worker
```

`WORKER_COUNT` in `.env` controls how many concurrent worker goroutines the worker process spawns.

For hot reload during development:

```bash
task dev:server   # auto-restarts server on file change
task dev:worker   # auto-restarts worker on file change
```


## CLI Usage

```bash
# Start the server
task server

# Submit a shell command job
go run ./cmd/cli submit --type=shell --payload='{"command":"echo hello world"}'

# Submit with retries
go run ./cmd/cli submit --type=shell --payload='{"command":"go test ./..."}' --retries=3

# Check job status
go run ./cmd/cli status <job-id>

# List all jobs
go run ./cmd/cli list

# List jobs filtered by status
go run ./cmd/cli list --status=pending
go run ./cmd/cli list --status=running
go run ./cmd/cli list --status=completed
go run ./cmd/cli list --status=failed

# Cancel a job
go run ./cmd/cli cancel <job-id>
```

## Workflows

Workflows are named sequences of shell commands executed in order. If any step fails, the workflow stops and is marked `failed`.

Drop `.yaml` files into the `workflows/` directory and restart the server — they are loaded automatically.

```bash
# List loaded workflows
go run ./cmd/cli workflow list

# Trigger a workflow by name
go run ./cmd/cli workflow trigger <name>

# Check run status
go run ./cmd/cli workflow status <run-id>
```

**Workflow file format:**

```yaml
name: ci
steps:
  - command: go test ./...
  - command: go build ./...
```

Example use cases: CI pipelines, database backups, Docker deployments, scheduled maintenance tasks, sequential data processing.

Example workflows are included in `workflows/`. Edit or delete them and add your own.

**Using Docker?** The `workflows/` directory is volume-mounted into the server container. Add a workflow file and run `docker compose restart server` — no rebuild needed.


The CLI reads `GRPC_ADDR` too. If it is set to a listen-style value like `:50051`,
the CLI treats it as `localhost:50051`.

## Regenerating Proto

If you modify `proto/orchestrator.proto`, regenerate Go code with:

```bash
task proto
```

Requires `protoc-gen-go` and `protoc-gen-go-grpc` in your `$PATH`:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```
