ALTER TABLE workflow_runs DROP CONSTRAINT workflow_runs_status_check;
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_runs_status_check
    CHECK (status IN ('running', 'completed', 'failed'));
