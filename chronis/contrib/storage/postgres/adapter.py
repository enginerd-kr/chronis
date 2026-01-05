"""PostgreSQL-based storage adapter with SQL injection protection."""

import json
import re
from pathlib import Path
from typing import Any

from psycopg2 import sql

from chronis.adapters.base import JobStorageAdapter
from chronis.contrib.storage.postgres.migration import MigrationRunner
from chronis.type_defs import JobStorageData, JobUpdateData
from chronis.utils.time import utc_now


class PostgreSQLStorageAdapter(JobStorageAdapter):
    """
    PostgreSQL-based job storage adapter with migration-based schema management.

    Features automatic migration execution, SQL injection protection, and
    parameterized queries for all operations.
    """

    _TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")
    _MAX_TABLE_NAME_LENGTH = 63

    def __init__(
        self,
        connection: Any,
        table_name: str = "chronis_jobs",
        migrations_dir: Path | str | None = None,
        auto_migrate: bool = True,
    ) -> None:
        """
        Initialize PostgreSQL storage adapter with migration-based schema management.

        Args:
            connection: psycopg2 connection object
            table_name: Table name (alphanumeric + underscores only, max 63 chars)
            migrations_dir: Path to migrations directory (uses built-in if not provided)
            auto_migrate: Automatically run pending migrations (default: True)

        Raises:
            ValueError: If table_name contains invalid characters or is too long
        """
        self._validate_table_name(table_name)
        self.conn = connection
        self.table_name = table_name

        if migrations_dir is None:
            migrations_path = Path(__file__).parent / "migrations"
        else:
            migrations_path = Path(migrations_dir)

        self.migration_runner = MigrationRunner(connection, migrations_path)

        if auto_migrate:
            self.migration_runner.migrate()
        else:
            self.migration_runner._ensure_history_table()  # noqa: SLF001

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
                        INSERT INTO {} (
                            job_id,
                            data,
                            status,
                            next_run_time,
                            metadata
                        )
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
        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("""
                    SELECT data
                    FROM {}
                    WHERE job_id = %s
                """).format(sql.Identifier(self.table_name)),
                (job_id,),
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
                    SET
                        data = %s,
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

    def compare_and_swap_job(
        self,
        job_id: str,
        expected_values: dict[str, Any],
        updates: JobUpdateData,
    ) -> tuple[bool, JobStorageData | None]:
        """
        Atomically update a job only if current values match expected values (PostgreSQL).

        Uses PostgreSQL WHERE clause conditions to ensure atomicity.
        The UPDATE will only succeed if all expected values match.

        Args:
            job_id: Job ID to update
            expected_values: Dictionary of field-value pairs that must match
            updates: Dictionary of fields to update

        Returns:
            Tuple of (success, updated_job_data)

        Raises:
            ValueError: If job_id not found
        """
        # Get current data
        job_data = self.get_job(job_id)
        if job_data is None:
            raise ValueError(f"Job {job_id} not found")

        # Build WHERE conditions dynamically
        where_conditions = ["job_id = %s"]
        where_params: list[Any] = [job_id]

        for field, expected_value in expected_values.items():
            if field == "status":
                where_conditions.append("status = %s")
                where_params.append(expected_value)
            elif field == "next_run_time":
                where_conditions.append("next_run_time = %s")
                where_params.append(expected_value)
            else:
                # For other fields, check JSONB data
                where_conditions.append("data->>%s = %s")
                where_params.append(field)
                where_params.append(str(expected_value))

        # Merge updates
        updated_job_data = job_data.copy()
        updated_job_data.update(updates)  # type: ignore[typeddict-item]
        updated_job_data["updated_at"] = utc_now().isoformat()  # type: ignore[typeddict-item]

        # Atomic UPDATE with WHERE conditions
        with self.conn.cursor() as cursor:
            where_clause = " AND ".join(where_conditions)
            query = sql.SQL("""
                UPDATE {}
                SET
                    data = %s,
                    status = %s,
                    next_run_time = %s,
                    metadata = %s,
                    updated_at = NOW()
                WHERE {}
            """).format(
                sql.Identifier(self.table_name),
                sql.SQL(where_clause),
            )

            cursor.execute(
                query,
                (
                    json.dumps(updated_job_data),
                    updated_job_data.get("status"),
                    updated_job_data.get("next_run_time"),
                    json.dumps(updated_job_data.get("metadata", {})),
                    *where_params,
                ),
            )

            rows_affected = cursor.rowcount
            self.conn.commit()

            if rows_affected == 0:
                # No rows updated - expectations didn't match
                return (False, None)

            return (True, updated_job_data)

    def delete_job(self, job_id: str) -> bool:
        """Delete a job from PostgreSQL."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("""
                    DELETE FROM {}
                    WHERE job_id = %s
                """).format(sql.Identifier(self.table_name)),
                (job_id,),
            )
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
            SELECT data
            FROM {}
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

        query = sql.SQL("""
            SELECT COUNT(*)
            FROM {}
            {}
        """).format(
            sql.Identifier(self.table_name),
            where_clause,
        )

        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result[0] if result else 0
