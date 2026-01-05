"""Tests for the database migration system."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from chronis.contrib.adapters.storage.postgres.migration import Migration, MigrationRunner


class TestMigration:
    """Test Migration dataclass."""

    def test_from_filename_valid(self):
        """Test parsing valid migration filename."""
        filepath = Path("V001__initial_schema.sql")
        migration = Migration.from_filename(filepath)

        assert migration.version == 1
        assert migration.description == "initial schema"
        assert migration.filepath == filepath

    def test_from_filename_with_underscores(self):
        """Test parsing filename with underscores in description."""
        filepath = Path("V042__add_user_authentication_table.sql")
        migration = Migration.from_filename(filepath)

        assert migration.version == 42
        assert migration.description == "add user authentication table"

    def test_from_filename_invalid_format(self):
        """Test that invalid filenames raise ValueError."""
        invalid_filenames = [
            "001__initial.sql",  # Missing V prefix
            "V001_initial.sql",  # Single underscore
            "V001__initial.txt",  # Wrong extension
            "Vabc__initial.sql",  # Non-numeric version
            "V001__.sql",  # Empty description
        ]

        for filename in invalid_filenames:
            with pytest.raises(ValueError, match="Invalid migration filename"):
                Migration.from_filename(Path(filename))

    def test_sorting(self):
        """Test migrations sort by version."""
        m1 = Migration(1, "first", Path("V001__first.sql"))
        m2 = Migration(2, "second", Path("V002__second.sql"))
        m10 = Migration(10, "tenth", Path("V010__tenth.sql"))

        migrations = [m10, m1, m2]
        sorted_migrations = sorted(migrations)

        assert [m.version for m in sorted_migrations] == [1, 2, 10]


class TestMigrationRunner:
    """Test MigrationRunner."""

    @pytest.fixture
    def mock_conn(self):
        """Create a mock database connection."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        return conn

    @pytest.fixture
    def migrations_dir(self):
        """Create a temporary migrations directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_path = Path(tmpdir) / "migrations"
            migrations_path.mkdir()

            # Create sample migration files
            (migrations_path / "V001__initial_schema.sql").write_text(
                "CREATE TABLE test1 (id INT);"
            )
            (migrations_path / "V002__add_indexes.sql").write_text(
                "CREATE INDEX idx_test ON test1(id);"
            )

            yield migrations_path

    def test_init_creates_history_table(self, mock_conn, migrations_dir):
        """Test that initialization creates history table."""
        runner = MigrationRunner(mock_conn, migrations_dir)
        runner._ensure_history_table()

        # Verify CREATE TABLE was called
        cursor = mock_conn.cursor.return_value.__enter__.return_value
        assert cursor.execute.called

        # Check that it creates the history table
        call_args = cursor.execute.call_args[0][0]
        assert "chronis_migration_history" in str(call_args)

    def test_init_nonexistent_directory(self, mock_conn):
        """Test that initialization fails with nonexistent directory."""
        with pytest.raises(ValueError, match="Migrations directory not found"):
            MigrationRunner(mock_conn, Path("/nonexistent/path"))

    def test_discover_migrations(self, mock_conn, migrations_dir):
        """Test discovering migration files."""
        runner = MigrationRunner(mock_conn, migrations_dir)
        migrations = runner._discover_migrations()

        assert len(migrations) == 2
        assert migrations[0].version == 1
        assert migrations[1].version == 2

    def test_discover_migrations_ignores_invalid(self, mock_conn, migrations_dir):
        """Test that invalid migration files are skipped with warning."""
        # Create an invalid migration file that matches V*.sql pattern
        (migrations_dir / "V999_invalid.sql").write_text("INVALID")

        runner = MigrationRunner(mock_conn, migrations_dir)

        # Should discover only valid migrations
        with patch("builtins.print") as mock_print:
            migrations = runner._discover_migrations()

            # Should have printed warning for invalid format
            assert mock_print.called
            assert "Warning" in str(mock_print.call_args)

        # Should still find the 2 valid migrations
        assert len(migrations) == 2

    def test_get_applied_versions(self, mock_conn, migrations_dir):
        """Test getting applied migration versions."""
        cursor = mock_conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [(1,), (2,)]

        runner = MigrationRunner(mock_conn, migrations_dir)
        runner._ensure_history_table()
        applied = runner._get_applied_versions()

        assert applied == {1, 2}

    def test_execute_migration_success(self, mock_conn, migrations_dir):
        """Test successful migration execution."""
        migration = Migration(
            version=1,
            description="test migration",
            filepath=migrations_dir / "V001__initial_schema.sql",
        )

        runner = MigrationRunner(mock_conn, migrations_dir)
        runner._ensure_history_table()

        with patch("builtins.print"):
            runner._execute_migration(migration)

        # Verify SQL was executed
        cursor = mock_conn.cursor.return_value.__enter__.return_value
        assert cursor.execute.call_count >= 2  # Migration SQL + history insert
        assert mock_conn.commit.called

    def test_execute_migration_rollback_on_failure(self, mock_conn, migrations_dir):
        """Test that migration is rolled back on failure."""
        cursor = mock_conn.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = Exception("SQL error")

        migration = Migration(
            version=1,
            description="test migration",
            filepath=migrations_dir / "V001__initial_schema.sql",
        )

        runner = MigrationRunner(mock_conn, migrations_dir)

        with pytest.raises(RuntimeError, match="Failed to apply migration"):
            runner._execute_migration(migration)

        # Verify rollback was called
        assert mock_conn.rollback.called

    def test_migrate_applies_pending_migrations(self, mock_conn, migrations_dir):
        """Test that migrate applies all pending migrations."""
        # Simulate version 1 already applied
        cursor = mock_conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [(1,)]

        runner = MigrationRunner(mock_conn, migrations_dir)

        with patch("builtins.print"):
            count = runner.migrate()

        # Should apply only version 2
        assert count == 1

    def test_migrate_with_target_version(self, mock_conn, migrations_dir):
        """Test migrating to specific version."""
        cursor = mock_conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []  # No migrations applied

        # Create additional migration
        (migrations_dir / "V003__add_feature.sql").write_text("CREATE TABLE test3;")

        runner = MigrationRunner(mock_conn, migrations_dir)

        with patch("builtins.print"):
            count = runner.migrate(target_version=2)

        # Should apply only versions 1 and 2, not 3
        assert count == 2

    def test_migrate_no_pending(self, mock_conn, migrations_dir):
        """Test migrate when all migrations are applied."""
        # Simulate all migrations applied
        cursor = mock_conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [(1,), (2,)]

        runner = MigrationRunner(mock_conn, migrations_dir)

        with patch("builtins.print") as mock_print:
            count = runner.migrate()

        assert count == 0
        # Should print no pending message
        assert any("No pending" in str(call) for call in mock_print.call_args_list)

    def test_status(self, mock_conn, migrations_dir):
        """Test getting migration status."""
        # Simulate version 1 applied
        cursor = mock_conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [(1,)]

        runner = MigrationRunner(mock_conn, migrations_dir)
        status = runner.status()

        assert status["total_migrations"] == 2
        assert status["applied_count"] == 1
        assert status["pending_count"] == 1
        assert len(status["applied"]) == 1
        assert len(status["pending"]) == 1
        assert status["applied"][0]["version"] == 1
        assert status["pending"][0]["version"] == 2
