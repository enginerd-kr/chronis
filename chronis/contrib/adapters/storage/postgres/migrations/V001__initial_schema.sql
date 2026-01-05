-- Initial schema for Chronis PostgreSQL storage
-- Creates the main jobs table and required indexes

CREATE TABLE IF NOT EXISTS chronis_jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    status VARCHAR(50),
    next_run_time TIMESTAMP,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index for querying jobs by status
CREATE INDEX IF NOT EXISTS idx_chronis_jobs_status
ON chronis_jobs(status);

-- Index for finding jobs ready to run
CREATE INDEX IF NOT EXISTS idx_chronis_jobs_next_run_time
ON chronis_jobs(next_run_time);

-- GIN index for JSONB metadata queries
CREATE INDEX IF NOT EXISTS idx_chronis_jobs_metadata
ON chronis_jobs USING GIN(metadata);
