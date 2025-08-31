"""Column matching utilities for PostgreSQL upsert operations."""

import logging
from typing import Any

from .schema_inspector import TableSchema

logger = logging.getLogger(__name__)


def match_columns(
    data_list: list[dict[str, Any]],
    table_schema: TableSchema,
    ignore_columns: list[str] | None = None
) -> dict[str, list[str]]:
    """Match data columns against table schema with union approach.

    Args:
        data_list: List of dictionaries containing data to be inserted
        table_schema: Table schema information from schema inspector
        ignore_columns: Optional list of column names to ignore (case insensitive)

    Returns:
        Dict with keys: matched_columns, ignored_columns, missing_columns
        Each containing respective lists of column names
    """
    # Handle edge cases
    if not data_list:
        return {
            'matched_columns': [],
            'ignored_columns': [],
            'missing_columns': []
        }

    # Collect all unique keys from entire data_list
    all_data_columns = set()
    for row in data_list:
        all_data_columns.update(row.keys())

    # Convert to list for consistent ordering
    all_data_columns = list(all_data_columns)

    # Get valid table columns (excludes auto-generated)
    valid_table_columns = set(table_schema.valid_columns)

    # Normalize ignore_columns to lowercase (PostgreSQL standard)
    ignore_set = set()
    if ignore_columns:
        ignore_set = {col.lower() for col in ignore_columns}

    # Add auto-generated columns to ignore_set
    auto_gen_columns = [col.name for col in table_schema.columns if col.is_auto_generated]
    ignore_set.update(col for col in auto_gen_columns)

    # Categorize columns
    matched_columns = []
    ignored_columns = []
    missing_columns = []

    for col in all_data_columns:
        col_lower = col.lower()

        # Check if column should be ignored
        if col_lower in ignore_set:
            ignored_columns.append(col)
            continue

        # Check if column exists in table schema
        if col in valid_table_columns:
            matched_columns.append(col)
        else:
            missing_columns.append(col)
            logger.debug(
                f"Column '{col}' does not exist in table '{table_schema.schema_name}.{table_schema.table_name}'")

    logger.debug(f"Column matching results: {len(matched_columns)} matched, "
                 f"{len(ignored_columns)} ignored, {len(missing_columns)} missing")

    return {
        'matched_columns': matched_columns,
        'ignored_columns': ignored_columns,
        'missing_columns': missing_columns
    }
