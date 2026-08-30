# Orqestra

Orqestra is a distributed job orchestrator written in Go for reliable background job execution. It separates job persistence, queueing, and execution so jobs survive server and worker failures without being lost.

Workers connect to the server over a bidirectional gRPC stream. Jobs are persisted to Postgres via a transactional outbox before being relayed to Redis, so the queue is always recoverable from the database.

## Quick Start

```bash
cp .env.example .env

# Start Postgres and Redis
task docker:up

# Apply migrations
task migrate:up

# Generate an API key and add it to .env as ORQESTRA_API_KEY
orq-keygen my-key

# Terminal 1
task server

# Terminal 2
task worker

# Submit a job
orq submit --type=shell --command "echo hello world"
```

The REST API listens on `localhost:8080` (CLI) and gRPC on `localhost:50051` (workers).

## Highlights

- DAG-based workflows with `depends_on` for parallel step execution
- Cron scheduling for automated workflow triggers
- Transactional outbox — jobs are written to Postgres atomically before Redis enqueue
- Crash recovery — interrupted jobs are re-queued on server restart
- Exponential backoff retries with configurable limits
- Per-step output chaining via `$PREVIOUS_OUTPUT`
- API key authentication on all REST endpoints
- Redis is used behind a queue interface, keeping orchestration decoupled from the queue backend
- CLI (`orq`) for job submission, inspection, and workflow management

## Architecture

```text
cmd/cli  ──HTTP──►  cmd/server  ◄──gRPC stream──  cmd/worker
                        ├── internal/api        (REST handlers)
                        ├── internal/server     (gRPC worker stream + workflow engine)
                        ├── internal/workflow   (workflow registry)
                        ├── internal/outbox     (relay: Postgres → Redis)
                        ├── internal/queue      (Redis queue)
                        └── internal/db         (Postgres)
```

## Reliability

Jobs are persisted to Postgres and an outbox table atomically, then asynchronously relayed to Redis. Postgres remains the source of truth, so queued work can be recovered after failures.

Failed jobs use exponential backoff retries, and interrupted jobs are recovered when the server restarts.

## Workflows

Workflows are YAML files defining steps as shell commands. Steps execute in dependency order — steps with no `depends_on` run immediately; downstream steps run once all their dependencies complete. Drop `.yaml` files into the `workflows/` directory and restart the server.

### DAG example

```yaml
name: nightly-backup
schedule: "0 2 * * *"
steps:
  - id: backup-db
    command: ./scripts/backup-db.sh
  - id: backup-redis
    command: ./scripts/backup-redis.sh
  - id: compress
    command: ./scripts/compress.sh
    depends_on: [backup-db, backup-redis]
  - id: upload
    command: ./scripts/upload.sh
    depends_on: [compress]
```

`backup-db` and `backup-redis` run in parallel. `compress` runs once both finish. `upload` runs last.

Cron-triggered runs will not start if a previous run of the same workflow is still active.

### Output chaining

When a step has exactly one dependency, that dependency's stdout is injected as `$PREVIOUS_OUTPUT`. Steps with zero or multiple dependencies receive an empty string. Output is capped at 64 KB — use the filesystem for large payloads.

## CLI

```bash
# Jobs
orq submit --type=shell --command "echo hello world"
orq list
orq status <job-id>
orq cancel <job-id>

# Workflows
orq workflow list
orq workflow trigger <name>
orq workflow runs
orq workflow status <run-id>
orq workflow cancel <run-id>
```

## Testing

```bash
task test          # all tiers
task test:unit     # offline only
task test:e2e      # requires Postgres + Redis
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `DB_URL` | — | Postgres connection string |
| `WORKER_COUNT` | — | Number of concurrent workers |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `GRPC_ADDR` | `:50051` | gRPC listen address |
| `HTTP_ADDR` | `:8080` | REST API listen address |
| `ORQESTRA_API_KEY` | — | API key for CLI authentication |

## Development

### Local setup

```bash
createdb -U postgres orchestrator
cp .env.example .env
task migrate:up
task server   # Terminal 1
task worker   # Terminal 2
```

### Migrations

```bash
task migrate:create NAME=<name>
task migrate:up
task migrate:down
```

### Proto regeneration

Requires `protoc-gen-go` and `protoc-gen-go-grpc` in your `PATH`.

```bash
task proto
```
