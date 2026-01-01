-- Chronis PostgreSQL Schema
-- This is automatically created by PostgreSQLStorageAdapter,
-- but you can run this manually for custom configurations.

-- Create jobs table
CREATE TABLE IF NOT EXISTS chronis_jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    status VARCHAR(50),
    next_run_time TIMESTAMP,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_chronis_jobs_status
    ON chronis_jobs(status);

CREATE INDEX IF NOT EXISTS idx_chronis_jobs_next_run_time
    ON chronis_jobs(next_run_time);

CREATE INDEX IF NOT EXISTS idx_chronis_jobs_metadata
    ON chronis_jobs USING GIN(metadata);

-- Optional: Partial index for ready jobs (common query)
CREATE INDEX IF NOT EXISTS idx_ready_jobs
    ON chronis_jobs(next_run_time)
    WHERE status = 'scheduled' AND next_run_time IS NOT NULL;

-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON chronis_jobs TO scheduler_user;
