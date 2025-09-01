"""Main upsert engine - public API for PostgreSQL upsert operations."""

import csv
import logging
import psycopg2

from dataclasses import dataclass
from pathlib import Path

from .schema_inspector import inspect_table_schema
from .column_matcher import match_columns
from .temp_staging import create_temp_table, bulk_insert_to_temp
from .conflict_resolver import (
    find_conflict_strategy,
    deduplicate_temp_table,
    execute_upsert,
    DeduplicationResult,
    ConflictStrategy
)
from .config import create_connection_from_env, test_connection

# Configure module logger
logger = logging.getLogger(__name__)


@dataclass
class UpsertResult:
    """Complete results from upsert workflow execution."""
    rows_inserted: int
    rows_updated: int
    total_affected: int
    deduplication_result: DeduplicationResult
    matched_columns: list[str]
    conflict_strategy_type: str
    conflict_strategy_description: str


@staticmethod
def read_csv_to_dict_list(csv_path: str | Path) -> list[dict[str, str]]:
    """Read CSV file and return list of dictionaries.

    Args:
        csv_path: Path to the CSV file

    Returns:
        List of dictionaries with CSV data

    Note:
        CSV data is returned as strings. Type conversion should be handled
        by the database driver during insertion.
    """
    csv_file = Path(csv_path)
    data_list = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data_list = list(reader)
    return data_list


@staticmethod
def execute_upsert_workflow(
    connection: psycopg2.extensions.connection,
    data: list[dict[str, str]] | str | Path,
    target_table: str,
    conflict_columns: list[str] | None = None,
    update_columns: list[str] | None = None,
    batch_size: int = 1000,
    keep_temp_table: bool = False,
    schema: str = 'public'
) -> UpsertResult:
    """Execute complete upsert workflow with automatic conflict detection.

    This function orchestrates the complete upsert process including:
    1. Data input handling (CSV files or direct data lists)
    2. Temporary table creation with automatic column detection
    3. Conflict strategy detection (primary key, unique constraints, or insert-only)
    4. Data deduplication using appropriate strategy
    5. Upsert execution with proper conflict resolution

    Args:
        connection: Active PostgreSQL database connection
        target_table: Name of the target table for upsert operation
        data: Input data as list of dictionaries, CSV file path, or Path object
        conflict_columns: Optional override for conflict detection columns. If provided,
                        these columns will be used for conflict resolution instead of
                        automatic detection (primary keys, unique constraints)
        update_columns: Optional override for columns to update on conflict. If not
                    provided, all matched columns will be updated
        batch_size: Number of rows to process in each batch during temp table population
        temp_table_prefix: Prefix for temporary table name (default: "_temp_")
        keep_temp_table: Whether to preserve temporary table after operation

    Returns:
        UpsertResult: Object containing operation results and statistics

    Raises:
        ValueError: If data is empty or target_table is invalid
        psycopg2.Error: For database connection or operation errors

    Example:
        >>> # Using direct data
        >>> data = [{'id': 1, 'name': 'John'}, {'id': 2, 'name': 'Jane'}]
        >>> result = execute_upsert_workflow(conn, 'users', data)
        >>> print(f"Inserted: {result.inserted_count}, Updated: {result.updated_count}")

        >>> # Using CSV file
        >>> result = execute_upsert_workflow(conn, 'users', 'data.csv')

    Note:
        For serverless ETL: This function is designed to work seamlessly with
        API responses by accepting direct data lists, eliminating the need
        for intermediate CSV file creation in lambda/cloud functions.
    """
    logger.info(f"Starting upsert workflow for table '{target_table}'")

    # Step 1: Handle input data
    if isinstance(data, (str, Path)):
        logger.info(f"Reading CSV file: {data}")
        data_list = read_csv_to_dict_list(data)
    else:
        data_list = data

    if not data_list:
        raise ValueError("No data provided for upsert operation")

    logger.info(f"Processing {len(data_list)} rows")

    # Step 2: Inspect target table schema
    target_schema = inspect_table_schema(connection, target_table, schema)
    logger.info("Target table schema inspected")

    # Step 3: Match and map columns
    column_mapping = match_columns(data_list, target_schema)
    matched_columns = column_mapping['matched_columns']
    logger.info(f"Matched columns: {matched_columns}")

    # Step 4: Create and populate temp table
    # Create temp table with auto-generated name
    temp_table_name = create_temp_table(connection, target_table, schema)
    logger.info(f"Created temp table: {temp_table_name}")

    # Populate temp table
    rows_inserted = bulk_insert_to_temp(
        connection=connection,
        temp_table_name=temp_table_name,
        data_list=data_list,
        matched_columns=matched_columns,
        target_schema=target_schema,
        batch_size=batch_size
    )
    logger.info(f"Populated temp table with {rows_inserted} rows")

    try:
        # Step 5: Find conflict strategy
        if conflict_columns:
            # Use user-provided conflict columns
            conflict_strategy = ConflictStrategy(
                type="USER_DEFINED",
                columns=conflict_columns,
                description=f"User-defined conflict resolution on: {conflict_columns}"
            )
            logger.info(f"Using user-defined conflict strategy: {conflict_strategy.description}")
        else:
            # Automatic detection
            conflict_strategy = find_conflict_strategy(
                target_schema,
                matched_columns
            )
            logger.info(f"Using automatic conflict strategy: {conflict_strategy.type}")

        # Step 6: Deduplicate temp table
        dedup_result = deduplicate_temp_table(
            connection,
            temp_table_name,
            conflict_strategy.columns
        )
        logger.info(f"Deduplication: {dedup_result.original_count} -> {dedup_result.deduplicated_count}")

        # Step 7: Execute upsert
        columns_to_update = update_columns or matched_columns or list(data_list[0].keys())
        inserted_count, updated_count = execute_upsert(
            connection,
            temp_table_name,
            target_table,
            conflict_strategy,
            columns_to_update,
            schema,
        )
        logger.info(f"Upsert complete: {inserted_count} inserted, {updated_count} updated\n")

        # Step 8: Create final result
        result = UpsertResult(
            rows_inserted=inserted_count,
            rows_updated=updated_count,
            total_affected=inserted_count + updated_count,
            deduplication_result=dedup_result,
            matched_columns=matched_columns or list(data_list[0].keys()),
            conflict_strategy_type=conflict_strategy.type,
            conflict_strategy_description=conflict_strategy.description
        )

        return result

    finally:
        # Clean up temp table unless requested to keep
        if not keep_temp_table:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                    connection.commit()
                logger.debug(f"Cleaned up temp table: {temp_table_name}")
            except Exception as e:
                logger.warning(f"Failed to clean up temp table {temp_table_name}: {e}")


class UpsertEngine:
    """Main interface for PostgreSQL upsert operations.

    This class provides a clean API for all upsert functionality including
    connection management, data loading, and workflow execution.
    """

    @staticmethod
    def create_connection() -> psycopg2.extensions.connection:
        """Create PostgreSQL connection from environment variables.

        Expected environment variables:
        - PGHOST (default: localhost)
        - PGPORT (default: 5432)
        - PGDATABASE (required)
        - PGUSER (required)
        - PGPASSWORD (required)

        Returns:
            psycopg2.connection: Active database connection

        Raises:
            ConnectionError: If connection fails
        """
        return create_connection_from_env()

    @staticmethod
    def test_connection() -> psycopg2.extensions.connection:
        """Test database connection and validate permissions for upsert operations.

        This method creates a connection and validates that the user has
        sufficient permissions to perform upsert operations (CREATE TEMP TABLE).

        Returns:
            psycopg2.connection: Validated connection ready for use

        Raises:
            ConnectionError: If connection fails
            PermissionError: If user lacks required permissions
        """
        return test_connection()

    @staticmethod
    def upsert_data(
        connection: psycopg2.extensions.connection,
        data: list[dict[str, str]] | str | Path,
        target_table: str,
        conflict_columns: list[str] | None = None,
        update_columns: list[str] | None = None,
        batch_size: int = 1000,
        keep_temp_table: bool = False,
        schema: str = 'public'
    ) -> UpsertResult:
        """Execute complete upsert workflow with automatic conflict detection.

        This function orchestrates the complete upsert process including:
        1. Data input handling (CSV files or direct data lists)
        2. Temporary table creation with automatic column detection
        3. Conflict strategy detection (primary key, unique constraints, or insert-only)
        4. Data deduplication using appropriate strategy
        5. Upsert execution with proper conflict resolution

        Args:
            connection: Active PostgreSQL database connection
            target_table: Name of the target table for upsert operation
            data: Input data as list of dictionaries, CSV file path, or Path object
            conflict_columns: Optional override for conflict detection columns. If provided,
                            these columns will be used for conflict resolution instead of
                            automatic detection (primary keys, unique constraints)
            update_columns: Optional override for columns to update on conflict. If not
                        provided, all matched columns will be updated
            batch_size: Number of rows to process in each batch during temp table population
            temp_table_prefix: Prefix for temporary table name (default: "_temp_")
            keep_temp_table: Whether to preserve temporary table after operation

        Returns:
            UpsertResult: Object containing operation results and statistics

        Raises:
            ValueError: If data is empty or target_table is invalid
            psycopg2.Error: For database connection or operation errors

        Example:
            >>> # Using direct data
            >>> data = [{'id': 1, 'name': 'John'}, {'id': 2, 'name': 'Jane'}]
            >>> result = execute_upsert_workflow(conn, 'users', data)
            >>> print(f"Inserted: {result.inserted_count}, Updated: {result.updated_count}")

            >>> # Using CSV file
            >>> result = execute_upsert_workflow(conn, 'users', 'data.csv')

        Note:
            For serverless ETL: This function is designed to work seamlessly with
            API responses by accepting direct data lists, eliminating the need
            for intermediate CSV file creation in lambda/cloud functions.
        """

        return execute_upsert_workflow(
            connection=connection,
            data=data,
            target_table=target_table,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
            batch_size=batch_size,
            keep_temp_table=keep_temp_table,
            schema=schema
        )
