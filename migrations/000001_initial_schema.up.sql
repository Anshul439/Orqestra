CREATE TABLE workflow_runs (
    id            BIGSERIAL    PRIMARY KEY,
    workflow_name TEXT         NOT NULL,
    status        TEXT         NOT NULL DEFAULT 'running'
                               CHECK (status IN ('running', 'completed', 'failed')),
    current_step  INT          NOT NULL DEFAULT 0,
    total_steps   INT          NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE jobs (
    id              BIGSERIAL    PRIMARY KEY,
    status          TEXT         NOT NULL DEFAULT 'pending'
                                 CHECK (status IN ('pending', 'running', 'retrying', 'completed', 'failed', 'cancelled')),
    retry_count     INT          NOT NULL DEFAULT 0  CHECK (retry_count >= 0),
    max_retries     INT          NOT NULL DEFAULT 3  CHECK (max_retries >= 0),
    type            TEXT         NOT NULL DEFAULT 'generic',
    payload         TEXT         NOT NULL DEFAULT '{}',
    workflow_run_id BIGINT       REFERENCES workflow_runs(id) ON DELETE CASCADE,
    step_index      INT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE job_outbox (
    id           BIGSERIAL    PRIMARY KEY,
    job_id       BIGINT       NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    processed    BOOLEAN      NOT NULL DEFAULT FALSE,
    processed_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_jobs_status ON jobs(status);

CREATE INDEX idx_jobs_workflow_run_id ON jobs(workflow_run_id);

CREATE INDEX idx_job_outbox_unprocessed ON job_outbox(created_at)
    WHERE processed = FALSE;
