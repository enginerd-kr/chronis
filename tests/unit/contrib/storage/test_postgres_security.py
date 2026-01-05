"""SQL injection prevention tests for PostgreSQL storage adapter."""

from unittest.mock import MagicMock, patch

import pytest

from chronis.contrib.adapters.storage.postgres import PostgreSQLStorageAdapter


class TestTableNameValidation:
    """Test table name validation to prevent SQL injection."""

    @pytest.mark.parametrize(
        "valid_name",
        [
            "chronis_jobs",
            "my_table",
            "_private_table",
            "Table123",
            "t",  # Single char
            "a" * 63,  # Max length
            "_underscore_start",
            "MixedCase_123",
        ],
    )
    def test_valid_table_names(self, valid_name):
        """Valid table names should pass validation."""
        # Mock connection and migration runner to avoid DB operations
        mock_conn = MagicMock()

        with patch("chronis.contrib.adapters.storage.postgres.adapter.MigrationRunner"):
            # Should not raise ValueError for valid names
            PostgreSQLStorageAdapter(mock_conn, table_name=valid_name, auto_migrate=False)

    @pytest.mark.parametrize(
        "malicious_name",
        [
            "jobs; DROP TABLE users;--",
            "jobs' OR '1'='1",
            'jobs"; DROP TABLE users; --',
            "jobs` OR 1=1;--",
            "jobs/**/OR/**/1=1",
            "jobs; DELETE FROM users WHERE 1=1--",
            "jobs'; UPDATE users SET admin=true--",
            "jobs UNION SELECT * FROM passwords--",
        ],
    )
    def test_sql_injection_attempts_blocked(self, malicious_name):
        """SQL injection attempts should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid table name"):
            PostgreSQLStorageAdapter(None, table_name=malicious_name)  # type: ignore

    def test_table_name_too_long(self):
        """Table names over 63 characters should be rejected."""
        too_long = "a" * 64

        with pytest.raises(ValueError, match="too long"):
            PostgreSQLStorageAdapter(None, table_name=too_long)  # type: ignore

    @pytest.mark.parametrize(
        "invalid_name",
        [
            "123table",
            "9_invalid",
            "0table",
            "1",
        ],
    )
    def test_table_name_invalid_start_character(self, invalid_name):
        """Table names starting with numbers should be rejected."""
        with pytest.raises(ValueError, match="Invalid table name"):
            PostgreSQLStorageAdapter(None, table_name=invalid_name)  # type: ignore

    def test_table_name_empty(self):
        """Empty table name should be rejected."""
        with pytest.raises(ValueError, match="cannot be empty"):
            PostgreSQLStorageAdapter(None, table_name="")  # type: ignore

    @pytest.mark.parametrize(
        "invalid_name",
        [
            "table-name",
            "table.name",
            "table name",  # space
            "table@name",
            "table#name",
            "table$name",
            "table%name",
            "table&name",
            "table*name",
            "table(name)",
            "table[name]",
        ],
    )
    def test_table_name_special_characters(self, invalid_name):
        """Table names with special characters should be rejected."""
        with pytest.raises(ValueError, match="Invalid table name"):
            PostgreSQLStorageAdapter(None, table_name=invalid_name)  # type: ignore


class TestSecurityPrinciples:
    """Test that security design principles are applied."""

    def test_fail_fast_validation(self):
        """Validation should fail immediately during __init__ (Fail Fast)."""
        # Should fail at initialization, not at first query
        with pytest.raises(ValueError):
            PostgreSQLStorageAdapter(None, table_name="invalid; DROP")  # type: ignore

    def test_clear_error_messages(self):
        """Error messages should be clear and helpful (Principle of Least Surprise)."""
        with pytest.raises(ValueError) as exc_info:
            PostgreSQLStorageAdapter(None, table_name="123invalid")  # type: ignore

        error_msg = str(exc_info.value)
        # Check that error message is detailed
        assert "Invalid table name" in error_msg
        assert "123invalid" in error_msg
        assert "Must start with letter or underscore" in error_msg
        assert "examples" in error_msg.lower() or "valid" in error_msg.lower()

    def test_validation_constants_defined(self):
        """Validation constants should be class-level (SOLID: OCP)."""
        assert hasattr(PostgreSQLStorageAdapter, "_TABLE_NAME_PATTERN")
        assert hasattr(PostgreSQLStorageAdapter, "_MAX_TABLE_NAME_LENGTH")
        assert PostgreSQLStorageAdapter._MAX_TABLE_NAME_LENGTH == 63
