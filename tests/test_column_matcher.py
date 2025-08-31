"""Tests for column_matcher module."""

import pytest

from pgsql_upserter.column_matcher import match_columns
from pgsql_upserter.schema_inspector import TableSchema, ColumnInfo, UniqueConstraint


def create_test_schema():
    """Create a test table schema similar to meta_ads_metrics."""
    columns = [
        ColumnInfo("id", "serial", False, "nextval('seq')", True, 1),
        ColumnInfo("account_id", "text", True, None, False, 2),
        ColumnInfo("campaign_id", "text", True, None, False, 3),
        ColumnInfo("spend", "float8", True, None, False, 4),
        ColumnInfo("impressions", "int4", True, None, False, 5),
        ColumnInfo("last_update", "timestamp", True, "CURRENT_TIMESTAMP", True, 6),
    ]

    return TableSchema(
        table_name="test_table",
        schema_name="public",
        columns=columns,
        unique_constraints=[],
        primary_key=None
    )


def test_basic_column_matching():
    """Test basic column matching functionality."""
    schema = create_test_schema()
    data_list = [
        {"account_id": "123", "campaign_id": "456", "spend": 10.5},
        {"account_id": "789", "impressions": 1000}
    ]

    result = match_columns(data_list, schema)

    # All data columns should match valid table columns
    assert set(result['matched_columns']) == {"account_id", "campaign_id", "spend", "impressions"}
    assert result['ignored_columns'] == []
    assert result['missing_columns'] == []


def test_column_matching_with_ignore_list():
    """Test column matching with ignore list."""
    schema = create_test_schema()
    data_list = [
        {"account_id": "123", "campaign_id": "456", "spend": 10.5, "extra_col": "ignore_me"}
    ]

    result = match_columns(data_list, schema, ignore_columns=["EXTRA_COL"])  # Test case insensitive

    assert set(result['matched_columns']) == {"account_id", "campaign_id", "spend"}
    assert result['ignored_columns'] == ["extra_col"]
    assert result['missing_columns'] == []


def test_column_matching_with_missing_columns():
    """Test column matching when data has columns not in table."""
    schema = create_test_schema()
    data_list = [
        {"account_id": "123", "nonexistent_col": "value", "another_missing": 123}
    ]

    result = match_columns(data_list, schema)

    assert result['matched_columns'] == ["account_id"]
    assert result['ignored_columns'] == []
    assert set(result['missing_columns']) == {"nonexistent_col", "another_missing"}


def test_empty_data_list():
    """Test edge case with empty data list."""
    schema = create_test_schema()
    result = match_columns([], schema)

    assert result['matched_columns'] == []
    assert result['ignored_columns'] == []
    assert result['missing_columns'] == []


def test_auto_generated_columns_excluded():
    """Test that auto-generated columns are excluded from matching."""
    schema = create_test_schema()
    data_list = [
        {"id": 1, "account_id": "123", "last_update": "2025-01-01"}  # id and last_update are auto-generated
    ]

    result = match_columns(data_list, schema)

    assert result['matched_columns'] == ["account_id"]
    assert result['ignored_columns'] == []
    assert set(result['missing_columns']) == {"id", "last_update"}  # Auto-generated columns treated as missing


if __name__ == "__main__":
    # Run tests directly for development
    print("Testing basic column matching...")
    test_basic_column_matching()

    print("Testing with ignore list...")
    test_column_matching_with_ignore_list()

    print("Testing with missing columns...")
    test_column_matching_with_missing_columns()

    print("Testing empty data list...")
    test_empty_data_list()

    print("Testing auto-generated exclusion...")
    test_auto_generated_columns_excluded()

    print("All tests completed!")
