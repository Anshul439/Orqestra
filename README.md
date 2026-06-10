# go-orchestrator

A distributed job orchestrator built in Go. The server coordinates job assignment across distributed worker processes via bidirectional gRPC streaming, with Redis-backed queuing and Postgres persistence.

## Features

- gRPC API for job submission and status queries
- Job type and payload — route different kinds of work to appropriate handlers
- CLI client (`cmd/cli`) for submitting, inspecting, listing, and cancelling jobs
- Distributed workers (`cmd/worker`) — separate processes communicating with the server via bidirectional gRPC streaming
- Redis-backed queue with reliable delivery (`BRPOPLPUSH` pattern)
- Postgres-backed job persistence
- Exponential backoff retry with configurable max retries
- Crash recovery — in-flight jobs are automatically re-queued when a worker disconnects unexpectedly, no server restart needed
- Delayed job scheduling via Redis sorted sets
- Job cancellation — cancel pending or running jobs via CLI
- Structured logging (`log/slog`)
- Graceful shutdown

## Architecture

```
cmd/cli  ──gRPC──►  cmd/server  ◄──gRPC stream──  cmd/worker
                        ├── internal/server   (gRPC handlers)
                        ├── internal/queue    (Redis queue)
                        └── internal/db       (Postgres)
```

The server and workers are separate processes. Workers connect to the server over a persistent
bidirectional stream, receive job assignments, execute them, and report results back.

## Requirements

- Go 1.21+
- PostgreSQL
- Redis
- [Task](https://taskfile.dev/)
- [`golang-migrate`](https://github.com/golang-migrate/migrate)
- `protoc` + Go plugins (only needed to regenerate proto)

## Setup

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

The server and workers are separate processes. Start them in separate terminals:

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
# Start the server first
task server

# Submit a job with type and payload
go run ./cmd/cli submit --type=send_email --payload='{"to":"x@y.com"}'

# Submit with custom max retries
go run ./cmd/cli submit --type=resize_image --payload='{"url":"s3://img.jpg"}' --retries=5

# Submit with defaults (type=generic, payload={}, retries=3)
go run ./cmd/cli submit

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

Example output:
```
job submitted, id: 42 (type=send_email)
job 42 (send_email): status=completed retries=1/3
```

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
