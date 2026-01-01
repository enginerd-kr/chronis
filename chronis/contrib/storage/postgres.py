"""PostgreSQL-based storage adapter."""

import json
from typing import Any

from chronis.adapters.base import JobStorageAdapter
from chronis.type_defs import JobStorageData, JobUpdateData
from chronis.utils.time import utc_now


class PostgreSQLStorageAdapter(JobStorageAdapter):
    """
    PostgreSQL-based job storage adapter.

    Design Decisions:
    - Uses JSONB column for flexible job data storage
    - Single table: 'chronis_jobs'
    - Indexes on: job_id (PK), status, next_run_time
    - GIN index on metadata for fast metadata queries
    - psycopg2 for database connectivity

    Schema:
        CREATE TABLE chronis_jobs (
            job_id VARCHAR(255) PRIMARY KEY,
            data JSONB NOT NULL,
            status VARCHAR(50),
            next_run_time TIMESTAMP,
            metadata JSONB,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX idx_status ON chronis_jobs(status);
        CREATE INDEX idx_next_run_time ON chronis_jobs(next_run_time);
        CREATE INDEX idx_metadata ON chronis_jobs USING GIN(metadata);

    Performance Characteristics:
    - O(1) for get/create/update/delete with job_id
    - O(log N) for time-range queries (indexed)
    - O(log N) for status queries (indexed)
    - Fast metadata queries with GIN index

    Optimization Options:
    - Use connection pooling (psycopg2.pool)
    - Add partial indexes for common queries
    - Consider partitioning by status or time
    - Monitor query performance with EXPLAIN

    Example:
        >>> import psycopg2
        >>> conn = psycopg2.connect(
        ...     host='localhost',
        ...     database='scheduler',
        ...     user='postgres',
        ...     password='password'
        ... )
        >>> storage = PostgreSQLStorageAdapter(conn)
    """

    def __init__(self, connection: Any, table_name: str = "chronis_jobs") -> None:
        """
        Initialize PostgreSQL storage adapter.

        Args:
            connection: psycopg2 connection object
            table_name: Table name for job storage (default: "chronis_jobs")

        Example:
            >>> import psycopg2
            >>> conn = psycopg2.connect(
            ...     host='localhost',
            ...     database='scheduler',
            ...     user='postgres'
            ... )
            >>> storage = PostgreSQLStorageAdapter(conn)

            >>> # Or with connection pool
            >>> from psycopg2 import pool
            >>> connection_pool = pool.SimpleConnectionPool(
            ...     minconn=1,
            ...     maxconn=10,
            ...     host='localhost',
            ...     database='scheduler'
            ... )
            >>> conn = connection_pool.getconn()
            >>> storage = PostgreSQLStorageAdapter(conn)
        """
        self.conn = connection
        self.table_name = table_name
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        """Create table and indexes if they don't exist."""
        with self.conn.cursor() as cursor:
            # Create table
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    job_id VARCHAR(255) PRIMARY KEY,
                    data JSONB NOT NULL,
                    status VARCHAR(50),
                    next_run_time TIMESTAMP,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """
            )

            # Create indexes
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_status "
                f"ON {self.table_name}(status)"
            )

            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_next_run_time "
                f"ON {self.table_name}(next_run_time)"
            )

            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_metadata "
                f"ON {self.table_name} USING GIN(metadata)"
            )

            self.conn.commit()

    def create_job(self, job_data: JobStorageData) -> JobStorageData:
        """
        Create a new job in PostgreSQL.

        Implementation:
            INSERT INTO chronis_jobs (job_id, data, status, next_run_time, metadata)
            VALUES (%s, %s, %s, %s, %s)

        Raises:
            ValueError: If job already exists (duplicate key violation)
        """
        job_id = job_data["job_id"]

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(
                    f"""
                    INSERT INTO {self.table_name}
                    (job_id, data, status, next_run_time, metadata)
                    VALUES (%s, %s, %s, %s, %s)
                """,
                    (
                        job_id,
                        json.dumps(job_data),
                        job_data.get("status"),
                        job_data.get("next_run_time"),
                        json.dumps(job_data.get("metadata", {})),
                    ),
                )
                self.conn.commit()
                return job_data

            except Exception as e:
                self.conn.rollback()
                if "duplicate key" in str(e).lower():
                    raise ValueError(f"Job {job_id} already exists") from e
                raise

    def get_job(self, job_id: str) -> JobStorageData | None:
        """
        Get a job from PostgreSQL.

        Implementation:
            SELECT data FROM chronis_jobs WHERE job_id = %s
        """
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT data FROM {self.table_name} WHERE job_id = %s", (job_id,))

            row = cursor.fetchone()
            if row is None:
                return None

            return row[0]  # JSONB data  # type: ignore[return-value]

    def update_job(self, job_id: str, updates: JobUpdateData) -> JobStorageData:
        """
        Update a job in PostgreSQL.

        Implementation:
            1. SELECT current data
            2. Merge updates
            3. UPDATE with new data

        Raises:
            ValueError: If job not found
        """
        # Get current data
        job_data = self.get_job(job_id)
        if job_data is None:
            raise ValueError(f"Job {job_id} not found")

        # Merge updates
        job_data.update(updates)  # type: ignore[typeddict-item]
        job_data["updated_at"] = utc_now().isoformat()  # type: ignore[typeddict-item]

        # Update in database
        with self.conn.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE {self.table_name}
                SET data = %s,
                    status = %s,
                    next_run_time = %s,
                    metadata = %s,
                    updated_at = NOW()
                WHERE job_id = %s
            """,
                (
                    json.dumps(job_data),
                    job_data.get("status"),
                    job_data.get("next_run_time"),
                    json.dumps(job_data.get("metadata", {})),
                    job_id,
                ),
            )
            self.conn.commit()

        return job_data

    def delete_job(self, job_id: str) -> bool:
        """
        Delete a job from PostgreSQL.

        Implementation:
            DELETE FROM chronis_jobs WHERE job_id = %s
        """
        with self.conn.cursor() as cursor:
            cursor.execute(f"DELETE FROM {self.table_name} WHERE job_id = %s", (job_id,))
            deleted = cursor.rowcount > 0
            self.conn.commit()
            return deleted

    def query_jobs(
        self,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[JobStorageData]:
        """
        Query jobs with filters.

        Implementation:
            SELECT data FROM chronis_jobs
            WHERE {conditions}
            ORDER BY next_run_time
            LIMIT %s OFFSET %s

        Supported filters:
        - status: Job status
        - next_run_time_lte: Jobs ready before this time
        - metadata.{key}: Metadata field matching (uses JSONB @> operator)

        Performance:
        - Status queries: O(log N) with index
        - Time queries: O(log N) with index
        - Metadata queries: Fast with GIN index
        """
        conditions = []
        params: list[Any] = []

        if filters:
            # Status filter
            if "status" in filters:
                conditions.append("status = %s")
                params.append(filters["status"])

            # Time filter
            if "next_run_time_lte" in filters:
                conditions.append("next_run_time <= %s")
                params.append(filters["next_run_time_lte"])

            # Metadata filters (JSONB containment)
            for key, value in filters.items():
                if key.startswith("metadata."):
                    metadata_key = key.replace("metadata.", "")
                    # Use JSONB @> operator for containment
                    conditions.append("metadata @> %s::jsonb")
                    params.append(json.dumps({metadata_key: value}))

        # Build query
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT data FROM {self.table_name}
            {where_clause}
            ORDER BY next_run_time NULLS LAST
        """

        # Add limit and offset
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        if offset:
            query += " OFFSET %s"
            params.append(offset)

        # Execute query
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

        return [row[0] for row in rows]  # type: ignore[misc]

    def count_jobs(self, filters: dict[str, Any] | None = None) -> int:
        """
        Count jobs matching filters.

        Implementation:
            SELECT COUNT(*) FROM chronis_jobs WHERE {conditions}

        Performance: O(log N) with indexes
        """
        conditions = []
        params: list[Any] = []

        if filters:
            if "status" in filters:
                conditions.append("status = %s")
                params.append(filters["status"])

            if "next_run_time_lte" in filters:
                conditions.append("next_run_time <= %s")
                params.append(filters["next_run_time_lte"])

            for key, value in filters.items():
                if key.startswith("metadata."):
                    metadata_key = key.replace("metadata.", "")
                    conditions.append("metadata @> %s::jsonb")
                    params.append(json.dumps({metadata_key: value}))

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"SELECT COUNT(*) FROM {self.table_name} {where_clause}"

        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result[0] if result else 0

    def update_job_run_times(
        self,
        job_id: str,
        scheduled_time: str,
        actual_time: str,
        next_run_time: str | None,
    ) -> JobStorageData:
        """
        Update job run times after execution.

        Implementation:
            UPDATE with specific time fields.
        """
        from typing import cast

        return self.update_job(
            job_id,
            cast(
                JobUpdateData,
                {
                    "last_scheduled_time": scheduled_time,
                    "last_run_time": actual_time,
                    "next_run_time": next_run_time,
                    "updated_at": utc_now().isoformat(),
                },
            ),
        )
