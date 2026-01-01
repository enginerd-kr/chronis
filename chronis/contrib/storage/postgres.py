"""PostgreSQL-based storage adapter with SQL injection protection."""

import json
import re
from typing import Any

from psycopg2 import sql

from chronis.adapters.base import JobStorageAdapter
from chronis.type_defs import JobStorageData, JobUpdateData
from chronis.utils.time import utc_now


class PostgreSQLStorageAdapter(JobStorageAdapter):
    """
    PostgreSQL-based job storage adapter with SQL injection protection.

    Security:
    - Uses psycopg2.sql.Identifier for table/index names (prevents SQL injection)
    - Validates table names against PostgreSQL identifier rules
    - Uses parameterized queries for all data values

    Example:
        >>> import os
        >>> import psycopg2
        >>> conn = psycopg2.connect(
        ...     host=os.getenv('DB_HOST', 'localhost'),
        ...     database=os.getenv('DB_NAME', 'scheduler'),
        ...     user=os.getenv('DB_USER', 'postgres'),
        ...     password=os.getenv('DB_PASSWORD')
        ... )
        >>> storage = PostgreSQLStorageAdapter(conn)
    """

    _TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")
    _MAX_TABLE_NAME_LENGTH = 63

    def __init__(self, connection: Any, table_name: str = "chronis_jobs") -> None:
        """
        Initialize PostgreSQL storage adapter.

        Args:
            connection: psycopg2 connection object
            table_name: Table name (alphanumeric + underscores only, max 63 chars)

        Raises:
            ValueError: If table_name contains invalid characters or is too long
        """
        self._validate_table_name(table_name)
        self.conn = connection
        self.table_name = table_name
        self._ensure_table_exists()

    def _validate_table_name(self, table_name: str) -> None:
        """
        Validate table name to prevent SQL injection.

        Args:
            table_name: Table name to validate

        Raises:
            ValueError: If table name is invalid
        """
        if not table_name:
            raise ValueError(
                "Table name cannot be empty\n"
                "Example: PostgreSQLStorageAdapter(conn, 'chronis_jobs')"
            )

        if len(table_name) > self._MAX_TABLE_NAME_LENGTH:
            raise ValueError(
                f"Table name too long: {len(table_name)} characters\n"
                f"PostgreSQL maximum: {self._MAX_TABLE_NAME_LENGTH} characters\n"
                f"Table name: '{table_name}'"
            )

        if not self._TABLE_NAME_PATTERN.match(table_name):
            raise ValueError(
                f"Invalid table name: '{table_name}'\n"
                f"Must start with letter or underscore\n"
                f"Allowed characters: letters, digits, underscores\n"
                f"Valid examples: 'chronis_jobs', 'my_table_123', '_private'"
            )

    def _execute_with_table_identifier(
        self,
        query_template: str,
        params: tuple | list | None = None,
    ) -> Any:
        """Execute SQL query with safe table name substitution."""
        with self.conn.cursor() as cursor:
            query = sql.SQL(query_template).format(sql.Identifier(self.table_name))
            cursor.execute(query, params or ())
            return cursor

    def _ensure_table_exists(self) -> None:
        """Create table and indexes if they don't exist."""
        with self.conn.cursor() as cursor:
            # Create main table with safe identifier
            cursor.execute(
                sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {} (
                        job_id VARCHAR(255) PRIMARY KEY,
                        data JSONB NOT NULL,
                        status VARCHAR(50),
                        next_run_time TIMESTAMP,
                        metadata JSONB,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """).format(sql.Identifier(self.table_name))
            )

            # Create status index
            cursor.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}(status)").format(
                    sql.Identifier(f"idx_{self.table_name}_status"),
                    sql.Identifier(self.table_name),
                )
            )

            # Create next_run_time index
            cursor.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}(next_run_time)").format(
                    sql.Identifier(f"idx_{self.table_name}_next_run_time"),
                    sql.Identifier(self.table_name),
                )
            )

            # Create metadata GIN index
            cursor.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING GIN(metadata)").format(
                    sql.Identifier(f"idx_{self.table_name}_metadata"),
                    sql.Identifier(self.table_name),
                )
            )

            self.conn.commit()

    def create_job(self, job_data: JobStorageData) -> JobStorageData:
        """
        Create a new job in PostgreSQL.

        Raises:
            ValueError: If job already exists (duplicate key violation)
        """
        job_id = job_data["job_id"]

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(
                    sql.SQL("""
                        INSERT INTO {}
                        (job_id, data, status, next_run_time, metadata)
                        VALUES (%s, %s, %s, %s, %s)
                    """).format(sql.Identifier(self.table_name)),
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
        """Get a job from PostgreSQL."""
        cursor = self._execute_with_table_identifier(
            "SELECT data FROM {} WHERE job_id = %s", (job_id,)
        )

        row = cursor.fetchone()
        if row is None:
            return None

        return row[0]  # JSONB data  # type: ignore[return-value]

    def update_job(self, job_id: str, updates: JobUpdateData) -> JobStorageData:
        """
        Update a job in PostgreSQL.

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
                sql.SQL("""
                    UPDATE {}
                    SET data = %s,
                        status = %s,
                        next_run_time = %s,
                        metadata = %s,
                        updated_at = NOW()
                    WHERE job_id = %s
                """).format(sql.Identifier(self.table_name)),
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
        """Delete a job from PostgreSQL."""
        cursor = self._execute_with_table_identifier("DELETE FROM {} WHERE job_id = %s", (job_id,))
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

        Supported filters:
        - status: Job status
        - next_run_time_lte: Jobs ready before this time
        - metadata.{key}: Metadata field matching (uses JSONB @> operator)
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

        # Build WHERE clause safely
        where_clause = (
            sql.SQL("WHERE {}").format(sql.SQL(" AND ").join(sql.SQL(c) for c in conditions))
            if conditions
            else sql.SQL("")
        )

        # Build main query with sql.Identifier for table name
        query = sql.SQL("""
            SELECT data FROM {}
            {}
            ORDER BY next_run_time NULLS LAST
        """).format(
            sql.Identifier(self.table_name),
            where_clause,
        )

        # Add LIMIT and OFFSET as parameterized values
        if limit:
            query += sql.SQL(" LIMIT %s")
            params.append(limit)
        if offset:
            query += sql.SQL(" OFFSET %s")
            params.append(offset)

        # Execute query
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

        return [row[0] for row in rows]  # type: ignore[misc]

    def count_jobs(self, filters: dict[str, Any] | None = None) -> int:
        """Count jobs matching filters."""
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

        where_clause = (
            sql.SQL("WHERE {}").format(sql.SQL(" AND ").join(sql.SQL(c) for c in conditions))
            if conditions
            else sql.SQL("")
        )

        query = sql.SQL("SELECT COUNT(*) FROM {} {}").format(
            sql.Identifier(self.table_name),
            where_clause,
        )

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
        """Update job run times after execution."""
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
