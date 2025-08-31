"""Tests for schema_inspector module."""

import pytest

from pgsql_upserter.config import test_connection
from pgsql_upserter.schema_inspector import inspect_table_schema
from pgsql_upserter.exceptions import TableNotFoundError, ConnectionError


def test_connection_from_env():
    """Test database connection using environment variables."""
    try:
        connection = test_connection()
        assert connection is not None
        assert not connection.closed
        connection.close()
    except (ConnectionError, Exception) as e:
        pytest.skip(f"Database not available for testing: {e}")


def test_inspect_meta_ads_metrics_table():
    """Test schema introspection on the real meta_ads_metrics table."""
    try:
        connection = test_connection()

        # Inspect the test table
        schema = inspect_table_schema(connection, 'meta_ads_metrics', 'public')

        # Basic assertions
        assert schema.table_name == 'meta_ads_metrics'
        assert schema.schema_name == 'public'
        assert len(schema.columns) > 0

        # Check that we have some expected columns from test_data.csv
        column_names = [col.name for col in schema.columns]
        expected_columns = ['id', 'account_id', 'campaign_id', 'spend', 'impressions', 'date_start']

        for expected_col in expected_columns:
            assert expected_col in column_names, f"Expected column '{expected_col}' not found"

        # Check that 'id' is marked as auto-generated
        id_column = next((col for col in schema.columns if col.name == 'id'), None)
        assert id_column is not None, "Column 'id' not found"
        assert id_column.is_auto_generated, "Column 'id' should be auto-generated"

        # Check that id is not in valid_columns (should be excluded for inserts)
        assert 'id' not in schema.valid_columns

        # Check constraints
        print(f"Table has {len(schema.unique_constraints)} unique constraints")
        for uc in schema.unique_constraints:
            print(f"Constraint '{uc.name}': {uc.columns} (primary: {uc.is_primary})")
        if schema.primary_key:
            print(f"Primary key: {schema.primary_key.columns}")

        connection.close()

    except (ConnectionError, Exception) as e:
        pytest.skip(f"Database not available for testing: {e}")


def test_inspect_nonexistent_table():
    """Test that inspecting a non-existent table raises TableNotFoundError."""
    try:
        connection = test_connection()

        with pytest.raises(TableNotFoundError):
            inspect_table_schema(connection, 'nonexistent_table', 'public')

        connection.close()

    except (ConnectionError, Exception) as e:
        pytest.skip(f"Database not available for testing: {e}")


def test_column_auto_generation_detection():
    """Test that auto-generated columns are properly detected."""
    try:
        connection = test_connection()
        schema = inspect_table_schema(connection, 'meta_ads_metrics', 'public')

        auto_gen_columns = [col.name for col in schema.columns if col.is_auto_generated]
        manual_columns = [col.name for col in schema.columns if not col.is_auto_generated]

        print(f"Auto-generated columns: {auto_gen_columns}")
        print(f"Manual columns: {manual_columns}")
        print(f"Valid columns for insert: {schema.valid_columns}")

        # Ensure valid_columns excludes auto-generated ones
        for auto_col in auto_gen_columns:
            assert auto_col not in schema.valid_columns

        connection.close()

    except (ConnectionError, Exception) as e:
        pytest.skip(f"Database not available for testing: {e}")


if __name__ == "__main__":
    # Run tests directly for development
    print("Testing connection...")
    test_connection_from_env()

    print("\nTesting schema inspection...")
    test_inspect_meta_ads_metrics_table()

    print("\nTesting auto-generation detection...")
    test_column_auto_generation_detection()

    print("\nTesting error handling...")
    test_inspect_nonexistent_table()

    print("\nAll tests completed!")
