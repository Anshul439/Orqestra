# go-orchestrator

A distributed background job processing system built in Go. Jobs are submitted over gRPC, queued in Redis, persisted in Postgres, and executed by a worker pool with retry and recovery.

## Features

- gRPC API for job submission and status queries
- CLI client (`cmd/cli`) for submitting and inspecting jobs
- Worker pool for concurrent job execution
- Redis-backed queue with reliable delivery (`BRPOPLPUSH` pattern)
- Postgres-backed job persistence
- Exponential backoff retry with configurable max retries
- Crash recovery — in-flight jobs are re-queued on restart
- Delayed job scheduling via Redis sorted sets
- Structured logging (`log/slog`)
- Graceful shutdown

## Architecture

```
cmd/cli  ──gRPC──►  cmd/server
                        ├── internal/server   (gRPC handlers)
                        ├── internal/queue    (Redis queue)
                        ├── internal/worker   (worker pool)
                        └── internal/db       (Postgres)
```

## Requirements

- Go 1.21+
- PostgreSQL
- Redis
- `protoc` + Go plugins (only needed to regenerate proto)

## Setup

```bash
# Create database
createdb -U postgres orchestrator

# Copy env config
cp .env.example .env

# Export DB_URL for the migrate CLI
export $(grep '^DB_URL=' .env | xargs)

# Apply migrations
migrate -path migrations -database "$DB_URL" up
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
migrate create -ext sql -dir migrations -seq <name>
```

Apply all pending migrations:

```bash
migrate -path migrations -database "$DB_URL" up
```

Roll back the most recent migration:

```bash
migrate -path migrations -database "$DB_URL" down 1
```

Check the current migration version:

```bash
migrate -path migrations -database "$DB_URL" version
```

## Running

```bash
# Start the orchestrator server
go run ./cmd/server
```

## CLI Usage

```bash
# Start the server first
go run ./cmd/server

# Submit a job (uses default 3 retries)
go run ./cmd/cli submit

# Submit a job with custom max retries
go run ./cmd/cli submit 5

# Check job status
go run ./cmd/cli status <job-id>
```

Example output:
```
job submitted, id: 42
job 42: status=completed retries=1/5
```

The CLI reads `GRPC_ADDR` too. If it is set to a listen-style value like `:50051`,
the CLI treats it as `localhost:50051`.

## Regenerating Proto

If you modify `proto/orchestrator.proto`, regenerate Go code with:

```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/orchestrator.proto
```

Requires `protoc-gen-go` and `protoc-gen-go-grpc` in your `$PATH`:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```
