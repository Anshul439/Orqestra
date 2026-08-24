ALTER TABLE jobs
    ADD CONSTRAINT jobs_workflow_step_unique UNIQUE (workflow_run_id, step_index);
