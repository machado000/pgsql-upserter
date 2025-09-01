"""Temporary table management for PostgreSQL upsert operations."""

import json
import logging
import uuid
import psycopg2

from psycopg2.extras import RealDictCursor, execute_values
from typing import Any

from .exceptions import PgsqlUpserterError

logger = logging.getLogger(__name__)


def _normalize_null_values(value: Any) -> Any | None:
    """Convert common null representations to None for PostgreSQL NULL."""
    if value is None:
        return None

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ['', 'none', 'null', 'nan', 'na', '-']:
            return None

    return value


def _convert_value_for_postgres(value: Any, column_data_type: str) -> Any | None:
    """Convert Python values to PostgreSQL-compatible formats based on column data type.

    Args:
        value: The value to convert
        column_data_type: PostgreSQL data type of the target column

    Returns:
        Converted value suitable for PostgreSQL insertion
    """
    # First apply null normalization
    normalized_value = _normalize_null_values(value)
    if normalized_value is None:
        return None

    # Convert based on PostgreSQL data type
    column_type = column_data_type.lower()

    try:
        # Handle JSONB and JSON columns
        if column_type in ('jsonb', 'json'):
            if isinstance(normalized_value, (dict, list)):
                return json.dumps(normalized_value)
            elif isinstance(normalized_value, str):
                # Try to parse as JSON, if it fails, treat as string literal
                try:
                    json.loads(normalized_value)
                    return normalized_value  # Already valid JSON string
                except json.JSONDecodeError:
                    return json.dumps(normalized_value)  # Wrap non-JSON string in quotes
            else:
                return json.dumps(normalized_value)

        # Handle PostgreSQL arrays (text[], integer[], etc.)
        elif column_type.endswith('[]') or column_type.startswith('_'):
            if isinstance(normalized_value, list):
                # Convert Python list to PostgreSQL array format
                # For text arrays, we need to escape quotes and handle null values
                array_elements = []
                for item in normalized_value:
                    if item is None:
                        array_elements.append('NULL')
                    elif isinstance(item, str):
                        # Escape quotes and wrap in quotes
                        escaped = item.replace('"', '\\"')
                        array_elements.append(f'"{escaped}"')
                    else:
                        array_elements.append(str(item))
                return '{' + ','.join(array_elements) + '}'
            elif isinstance(normalized_value, str):
                # If it's already a string, assume it's in PostgreSQL array format
                return normalized_value
            else:
                # Convert single value to single-element array
                if isinstance(normalized_value, str):
                    escaped = normalized_value.replace('"', '\\"')
                    return '{' + f'"{escaped}"' + '}'
                else:
                    return '{' + str(normalized_value) + '}'

        # For all other types, return the normalized value as-is
        # PostgreSQL driver will handle the conversion
        else:
            return normalized_value

    except Exception as e:
        logger.warning(f"Failed to convert value {repr(value)} for column type {column_data_type}: {e}")
        # Fallback: convert to string
        return str(normalized_value) if normalized_value is not None else None


def create_temp_table(connection, target_table: str, schema: str = 'public') -> str:
    """Create temporary table with same structure as target table.

    Args:
        connection: Active PostgreSQL connection
        target_table: Name of the target table to copy structure from
        schema: Schema name (default: 'public')

    Returns:
        str: The temporary table name that was created

    Raises:
        PgsqlUpserterError: If temp table creation fails
    """
    # Generate unique temp table name
    temp_table_name = f"temp_staging_{uuid.uuid4().hex[:8]}"

    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Create temp table with same structure, no constraints or defaults
            create_sql = f"""
                CREATE TEMPORARY TABLE {temp_table_name}
                (LIKE {schema}.{target_table} EXCLUDING ALL)
            """

            cursor.execute(create_sql)

            # Drop auto-generated columns to avoid constraint issues
            # Get auto-generated column info
            cursor.execute("""
                SELECT column_name, is_generated, column_default, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, target_table))

            columns_to_drop = []
            for row in cursor.fetchall():
                # Check if column is auto-generated
                is_auto_generated = (
                    row['data_type'] in ('serial', 'bigserial') or
                    (row['column_default'] and 'nextval(' in row['column_default'].lower()) or
                    row['is_generated'] == 'ALWAYS' or
                    (row['column_default'] and any(ts in row['column_default'].lower()
                                                   for ts in ['current_timestamp', 'now()', 'clock_timestamp()']))
                )

                if is_auto_generated:
                    columns_to_drop.append(row['column_name'])

            # Drop auto-generated columns from temp table
            for col_name in columns_to_drop:
                cursor.execute(f"ALTER TABLE {temp_table_name} DROP COLUMN {col_name}")

            connection.commit()

            logger.debug(
                f"Created temporary table '{temp_table_name}' based on '{schema}.{target_table}', dropped {len(columns_to_drop)} auto-generated columns")  # noqa
            return temp_table_name

    except psycopg2.Error as e:
        connection.rollback()
        raise PgsqlUpserterError(f"Failed to create temporary table: {e}")


def bulk_insert_to_temp(
    connection,
    temp_table_name: str,
    data_list: list[dict[str, Any]],
    matched_columns: list[str],
    target_schema=None,
    batch_size: int = 1000,
    show_progress: bool = True
) -> int:
    """Bulk insert filtered data into temporary table.

    Args:
        connection: Active PostgreSQL connection
        temp_table_name: Name of the temporary table
        data_list: List of dictionaries containing data to insert
        matched_columns: List of column names to include in insert
        target_schema: TableSchema object for data type conversion (optional)
        batch_size: Number of rows to process in each batch
        show_progress: Whether to show progress for large datasets

    Returns:
        int: Number of rows inserted

    Raises:
        PgsqlUpserterError: If bulk insert fails
    """
    if not data_list or not matched_columns:
        return 0

    total_rows = len(data_list)
    if show_progress and total_rows > batch_size:
        logger.info(f"Processing {total_rows} rows...")

    # Build column data type mapping for proper conversion
    column_type_map = {}
    if target_schema:
        for col_info in target_schema.columns:
            if col_info.name in matched_columns:
                column_type_map[col_info.name] = col_info.data_type

    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Filter and normalize data with progress tracking
            filtered_data = []
            for i, row in enumerate(data_list):
                filtered_row = []
                for col in matched_columns:
                    value = row.get(col)  # Missing keys become None

                    # Convert value based on column data type if available
                    if col in column_type_map:
                        converted_value = _convert_value_for_postgres(value, column_type_map[col])
                    else:
                        converted_value = _normalize_null_values(value)

                    filtered_row.append(converted_value)
                filtered_data.append(filtered_row)

                # Show progress every batch_size rows
                if show_progress and (i + 1) % batch_size == 0:
                    progress = (i + 1) / total_rows * 100
                    logger.info(f"Processed {i + 1}/{total_rows} rows ({progress:.1f}%)")

            # Build INSERT statement for execute_values
            columns_sql = ', '.join(matched_columns)
            insert_sql = f"INSERT INTO {temp_table_name} ({columns_sql}) VALUES %s"

            if show_progress:
                logger.info("Inserting data into temporary table...")

            # Use execute_values for better performance in serverless environments
            execute_values(
                cursor,
                insert_sql,
                filtered_data,
                template=None,
                page_size=batch_size  # Good balance for serverless memory limits
            )

            rows_inserted = len(filtered_data)  # Use actual data length instead of cursor.rowcount
            connection.commit()

            if show_progress:
                logger.info(f"Successfully inserted {rows_inserted} batched rows")

            logger.info(f"Inserted {rows_inserted} total rows into temporary table '{temp_table_name}'")
            return rows_inserted

    except psycopg2.Error as e:
        connection.rollback()
        # Try to cleanup temp table
        _cleanup_temp_table(connection, temp_table_name)
        raise PgsqlUpserterError(f"Failed to bulk insert into temporary table: {e}")


# Alias for better API naming
populate_temp_table = bulk_insert_to_temp


def convert_temp_to_permanent(
    connection,
    temp_table_name: str,
    permanent_table_name: str,
    schema: str = 'public'
) -> None:
    """Convert temporary table to permanent table for debugging purposes.

    Args:
        connection: Active PostgreSQL connection
        temp_table_name: Name of the temporary table
        permanent_table_name: Name for the permanent table (will be dropped if exists)
        schema: Schema name (default: 'public')

    Raises:
        PgsqlUpserterError: If conversion fails
    """
    try:
        with connection.cursor() as cursor:
            # Drop existing permanent table if it exists
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{permanent_table_name}")

            # Create permanent table from temp table structure and data
            cursor.execute(f"""
                CREATE TABLE {schema}.{permanent_table_name} AS
                SELECT * FROM {temp_table_name}
            """)

            connection.commit()

            # Get row count for confirmation
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{permanent_table_name}")
            row_count = cursor.fetchone()[0]

            logger.info(
                f"Converted temp table '{temp_table_name}' to permanent table '{schema}.{permanent_table_name}' with {row_count} rows")  # noqa

    except psycopg2.Error as e:
        connection.rollback()
        raise PgsqlUpserterError(f"Failed to convert temp table to permanent: {e}")


def _cleanup_temp_table(connection, temp_table_name: str) -> None:
    """Silently attempt to drop temporary table."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            connection.commit()
            logger.debug(f"Cleaned up temporary table '{temp_table_name}'")
    except Exception:
        # Fail silently as requested - temp tables auto-cleanup on session end anyway
        pass
