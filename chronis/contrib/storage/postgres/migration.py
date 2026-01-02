"""Database migration system for Chronis storage adapters.

Flyway-style migration system that:
- Manages SQL migration files in a migrations/ directory
- Tracks applied migrations in a history table
- Supports versioned migrations (V001__description.sql format)
- Executes migrations in order automatically
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg2 import sql


@dataclass
class Migration:
    """Represents a single migration file."""

    version: int
    description: str
    filepath: Path

    @classmethod
    def from_filename(cls, filepath: Path) -> "Migration":
        """
        Parse migration from filename.

        Expected format: V001__initial_schema.sql
        - V: prefix (required)
        - 001: version number (required, zero-padded)
        - __: separator (required)
        - initial_schema: description (required)
        - .sql: extension (required)
        """
        pattern = r"^V(\d+)__(.+)\.sql$"
        match = re.match(pattern, filepath.name)

        if not match:
            raise ValueError(
                f"Invalid migration filename: {filepath.name}\n"
                f"Expected format: V001__description.sql\n"
                f"Examples: V001__initial_schema.sql, V002__add_indexes.sql"
            )

        version = int(match.group(1))
        description = match.group(2).replace("_", " ")

        return cls(version=version, description=description, filepath=filepath)

    def __lt__(self, other: "Migration") -> bool:
        """Sort migrations by version."""
        return self.version < other.version


class MigrationRunner:
    """Executes database migrations with version tracking."""

    HISTORY_TABLE = "chronis_migration_history"

    def __init__(self, connection: Any, migrations_dir: Path | str) -> None:
        """
        Initialize migration runner.

        Args:
            connection: Database connection (psycopg2)
            migrations_dir: Directory containing migration SQL files
        """
        self.conn = connection
        self.migrations_dir = Path(migrations_dir)

        if not self.migrations_dir.exists():
            raise ValueError(f"Migrations directory not found: {self.migrations_dir}")

    def _ensure_history_table(self) -> None:
        """Create migration history table if it doesn't exist."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {} (
                        version INTEGER PRIMARY KEY,
                        description TEXT NOT NULL,
                        filename TEXT NOT NULL,
                        applied_at TIMESTAMP DEFAULT NOW(),
                        checksum TEXT,
                        execution_time_ms INTEGER
                    )
                """).format(sql.Identifier(self.HISTORY_TABLE))
            )
            self.conn.commit()

    def _get_applied_versions(self) -> set[int]:
        """Get set of already applied migration versions."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT version FROM {}").format(sql.Identifier(self.HISTORY_TABLE))
            )
            return {row[0] for row in cursor.fetchall()}

    def _discover_migrations(self) -> list[Migration]:
        """Discover all migration files in the migrations directory."""
        migrations = []

        for filepath in self.migrations_dir.glob("V*.sql"):
            try:
                migration = Migration.from_filename(filepath)
                migrations.append(migration)
            except ValueError as e:
                # Log warning but continue with other migrations
                print(f"Warning: Skipping invalid migration file: {e}")

        return sorted(migrations)

    def _execute_migration(self, migration: Migration) -> None:
        """Execute a single migration and record it in history."""
        import hashlib
        import time

        # Read migration SQL
        sql_content = migration.filepath.read_text(encoding="utf-8")

        # Calculate checksum for verification
        checksum = hashlib.sha256(sql_content.encode()).hexdigest()

        # Execute migration
        start_time = time.time()

        with self.conn.cursor() as cursor:
            try:
                # Execute the migration SQL
                cursor.execute(sql_content)

                # Record in history
                execution_time_ms = int((time.time() - start_time) * 1000)

                cursor.execute(
                    sql.SQL("""
                        INSERT INTO {} (version, description, filename, checksum, execution_time_ms)
                        VALUES (%s, %s, %s, %s, %s)
                    """).format(sql.Identifier(self.HISTORY_TABLE)),
                    (
                        migration.version,
                        migration.description,
                        migration.filepath.name,
                        checksum,
                        execution_time_ms,
                    ),
                )

                self.conn.commit()

                print(
                    f"✓ Applied migration V{migration.version:03d}: "
                    f"{migration.description} ({execution_time_ms}ms)"
                )

            except Exception as e:
                self.conn.rollback()
                raise RuntimeError(
                    f"Failed to apply migration V{migration.version:03d}: "
                    f"{migration.description}\n"
                    f"Error: {e}"
                ) from e

    def migrate(self, target_version: int | None = None) -> int:
        """
        Run pending migrations up to target version.

        Args:
            target_version: Stop at this version (None = run all)

        Returns:
            Number of migrations applied

        Raises:
            RuntimeError: If migration fails
        """
        self._ensure_history_table()

        # Get applied and pending migrations
        applied_versions = self._get_applied_versions()
        all_migrations = self._discover_migrations()

        # Filter pending migrations
        pending = [
            m
            for m in all_migrations
            if m.version not in applied_versions
            and (target_version is None or m.version <= target_version)
        ]

        if not pending:
            print("✓ No pending migrations")
            return 0

        # Execute pending migrations in order
        print(f"Found {len(pending)} pending migration(s)")

        for migration in pending:
            self._execute_migration(migration)

        print(f"\n✓ Successfully applied {len(pending)} migration(s)")
        return len(pending)

    def status(self) -> dict[str, Any]:
        """
        Get migration status.

        Returns:
            Dict with applied/pending migration info
        """
        self._ensure_history_table()

        applied_versions = self._get_applied_versions()
        all_migrations = self._discover_migrations()

        applied = [m for m in all_migrations if m.version in applied_versions]
        pending = [m for m in all_migrations if m.version not in applied_versions]

        return {
            "total_migrations": len(all_migrations),
            "applied_count": len(applied),
            "pending_count": len(pending),
            "applied": [
                {
                    "version": m.version,
                    "description": m.description,
                    "filename": m.filepath.name,
                }
                for m in applied
            ],
            "pending": [
                {
                    "version": m.version,
                    "description": m.description,
                    "filename": m.filepath.name,
                }
                for m in pending
            ],
        }
