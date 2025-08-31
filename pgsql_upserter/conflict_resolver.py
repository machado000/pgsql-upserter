"""Conflict resolution logic for PostgreSQL upsert operations."""

import logging

from dataclasses import dataclass

from .schema_inspector import TableSchema
from .exceptions import PgsqlUpserterError

# Configure module logger
logger = logging.getLogger(__name__)


@dataclass
class ConflictStrategy:
    """Represents a conflict resolution strategy."""
    type: str  # "PRIMARY_KEY", "UNIQUE_COMBINED", "INSERT_ONLY"
    columns: list[str]  # Column names for conflict resolution
    description: str  # Human-readable description


@dataclass
class DeduplicationResult:
    """Results from temp table deduplication process."""
    original_count: int
    deduplicated_count: int
    dropped_count: int
    dropped_reasons: dict[str, int]  # reason -> count mapping


def find_conflict_strategy(
    table_schema: TableSchema,
    matched_columns: list[str]
) -> ConflictStrategy:
    """
    Determine the best conflict resolution strategy based on table schema and available columns.

    Priority:
    1. Non-auto-generated primary key(s) - highest priority
    2. All unique constraints combined - medium priority
    3. Simple insert only - fallback

    Args:
        table_schema: Table schema information
        matched_columns: List of columns available in the data (from column_matcher)

    Returns:
        ConflictStrategy object with strategy details

    Raises:
        PgsqlUpserterError: If no valid strategy can be determined
    """
    logger.debug(f"Analyzing conflict strategy for table '{table_schema.table_name}' "
                 f"with {len(matched_columns)} available columns")

    # Convert matched_columns to set for faster lookup
    available_columns = set(matched_columns)

    # Strategy 1: Check for non-auto-generated primary key
    if table_schema.primary_key:
        pk_columns = table_schema.primary_key.columns
        # Check if all PK columns are available in matched_columns
        pk_available = all(col in available_columns for col in pk_columns)

        if pk_available:
            strategy = ConflictStrategy(
                type="PRIMARY_KEY",
                columns=pk_columns,
                description=f"Primary key conflict resolution on: {pk_columns}"
            )
            logger.debug(f"Using PRIMARY_KEY strategy: {strategy.description}")
            return strategy
        else:
            missing_pk_cols = [col for col in pk_columns if col not in available_columns]
            logger.warning(f"Primary key columns not available in data: {missing_pk_cols}")

    # Strategy 2: Combine all unique constraints (union approach)
    all_unique_columns = []
    for constraint in table_schema.unique_constraints:
        # Only include constraint columns that are available in matched_columns
        available_constraint_cols = [col for col in constraint.columns if col in available_columns]
        all_unique_columns.extend(available_constraint_cols)

    # Remove duplicates
    unique_columns = list(set(all_unique_columns))

    if unique_columns:
        strategy = ConflictStrategy(
            type="UNIQUE_COMBINED",
            columns=unique_columns,
            description=f"Combined unique constraints conflict resolution on: {unique_columns}"
        )
        logger.debug(f"Using UNIQUE_COMBINED strategy: {strategy.description}")
        return strategy

    # Strategy 3: Fallback to INSERT only
    strategy = ConflictStrategy(
        type="INSERT_ONLY",
        columns=[],
        description="No conflict resolution - INSERT only (no unique constraints found)"
    )
    logger.warning(f"Fallback to INSERT_ONLY strategy: {strategy.description}")
    return strategy


def deduplicate_temp_table(
    connection,
    temp_table_name: str,
    conflict_columns: list[str]
) -> DeduplicationResult:
    """
    Deduplicate temp table based on conflict columns, keeping last occurrence.
    Creates a new cleaned temp table and logs dropped rows.

    Args:
        connection: Database connection
        temp_table_name: Name of the source temp table
        conflict_columns: Columns to use for deduplication
        schema_name: Schema name (default: 'public')

    Returns:
        DeduplicationResult with statistics

    Raises:
        PgsqlUpserterError: If deduplication fails
    """
    logger.debug(f"Starting deduplication of '{temp_table_name}' on columns: {conflict_columns}")

    try:
        with connection.cursor() as cursor:
            # Get original count
            cursor.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
            original_count = cursor.fetchone()[0]
            logger.debug(f"Original temp table count: {original_count} rows")

            if not conflict_columns:
                # No deduplication needed for INSERT_ONLY strategy
                return DeduplicationResult(
                    original_count=original_count,
                    deduplicated_count=original_count,
                    dropped_count=0,
                    dropped_reasons={}
                )

            # Create cleaned temp table name
            cleaned_table_name = f"{temp_table_name}_cleaned"

            # Step 1: Build NULL checking conditions for conflict columns
            # We need to handle text columns differently from other types
            cursor.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{temp_table_name.split('.')[-1]}'
                  AND column_name = ANY(%s)
            """, (conflict_columns,))

            column_types = {row[0]: row[1] for row in cursor.fetchall()}

            null_conditions = []
            for col in conflict_columns:
                if col in column_types:
                    data_type = column_types[col]
                    if data_type in ('text', 'varchar', 'character varying', 'char'):
                        # For text columns, check both NULL and empty string
                        null_conditions.append(f"({col} IS NULL OR {col} = '')")
                    else:
                        # For other types (date, numeric, etc.), only check NULL
                        null_conditions.append(f"{col} IS NULL")
                else:
                    # Fallback: assume text type
                    null_conditions.append(f"({col} IS NULL OR {col} = '')")

            null_where_clause = " OR ".join(null_conditions)

            # Count rows with NULLs before removing
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM {temp_table_name}
                WHERE {null_where_clause}
            """)
            null_count = cursor.fetchone()[0]

            # Step 2: Create cleaned table with deduplication (keeping last occurrence)
            conflict_columns_str = ", ".join(conflict_columns)

            cursor.execute(f"""
                CREATE TEMP TABLE {cleaned_table_name} AS
                SELECT * FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY {conflict_columns_str}
                               ORDER BY ctid DESC
                           ) as rn
                    FROM {temp_table_name}
                    WHERE NOT ({null_where_clause})
                ) ranked
                WHERE rn = 1
            """)

            # Get final count
            cursor.execute(f"SELECT COUNT(*) FROM {cleaned_table_name}")
            deduplicated_count = cursor.fetchone()[0]

            # Calculate dropped counts
            duplicate_count = (original_count - null_count) - deduplicated_count
            total_dropped = original_count - deduplicated_count

            dropped_reasons = {}
            if null_count > 0:
                dropped_reasons["null_or_empty_conflict_columns"] = null_count
            if duplicate_count > 0:
                dropped_reasons["duplicate_conflict_keys"] = duplicate_count

            # Drop original temp table and rename cleaned table
            cursor.execute(f"DROP TABLE {temp_table_name}")
            cursor.execute(f"ALTER TABLE {cleaned_table_name} DROP COLUMN rn")
            cursor.execute(f"ALTER TABLE {cleaned_table_name} RENAME TO {temp_table_name.split('.')[-1]}")

            result = DeduplicationResult(
                original_count=original_count,
                deduplicated_count=deduplicated_count,
                dropped_count=total_dropped,
                dropped_reasons=dropped_reasons
            )

            logger.debug(f"Deduplication completed: {original_count} → {deduplicated_count} rows "
                         f"({total_dropped} dropped)")
            for reason, count in dropped_reasons.items():
                logger.debug(f"  - {reason}: {count} rows")

            return result

    except Exception as e:
        logger.error(f"Deduplication failed: {e}")
        raise PgsqlUpserterError(f"Failed to deduplicate temp table: {e}") from e


def execute_upsert(
    connection,
    temp_table_name: str,
    target_table: str,
    conflict_strategy: ConflictStrategy,
    matched_columns: list[str],
    schema_name: str = 'public'
) -> tuple[int, int]:
    """
    Execute the final upsert operation using INSERT...ON CONFLICT.

    Args:
        connection: Database connection
        temp_table_name: Name of the cleaned temp table
        target_table: Target table name
        conflict_strategy: Conflict resolution strategy
        matched_columns: List of columns available in the temp table
        schema_name: Schema name (default: 'public')

    Returns:
        Tuple of (rows_inserted, rows_updated)

    Raises:
        PgsqlUpserterError: If upsert operation fails
    """
    logger.debug(f"Executing upsert from '{temp_table_name}' to '{schema_name}.{target_table}' "
                 f"using {conflict_strategy.type} strategy")

    try:
        with connection.cursor() as cursor:
            # Use matched_columns instead of querying target table columns
            columns_str = ", ".join(matched_columns)

            if conflict_strategy.type == "INSERT_ONLY":
                # Simple INSERT without conflict resolution
                cursor.execute(f"""
                    INSERT INTO {schema_name}.{target_table} ({columns_str})
                    SELECT {columns_str}
                    FROM {temp_table_name}
                """)

                rows_affected = cursor.rowcount
                logger.debug(f"INSERT_ONLY completed: {rows_affected} rows inserted")
                return rows_affected, 0

            else:
                # INSERT...ON CONFLICT with UPDATE
                conflict_columns_str = ", ".join(conflict_strategy.columns)

                # Build UPDATE SET clause (matched columns except conflict columns)
                update_columns = [col for col in matched_columns if col not in conflict_strategy.columns]
                update_set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])

                # Track before counts for calculating inserts vs updates
                cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{target_table}")
                before_count = cursor.fetchone()[0]

                cursor.execute(f"""
                    INSERT INTO {schema_name}.{target_table} ({columns_str})
                    SELECT {columns_str}
                    FROM {temp_table_name}
                    ON CONFLICT ({conflict_columns_str})
                    DO UPDATE SET {update_set_clause}
                """)

                # Calculate insert vs update counts
                cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{target_table}")
                after_count = cursor.fetchone()[0]

                cursor.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
                temp_count = cursor.fetchone()[0]

                rows_inserted = after_count - before_count
                rows_updated = temp_count - rows_inserted

        logger.debug(f"Upsert completed: {rows_inserted} inserted, {rows_updated} updated")
        return rows_inserted, rows_updated

    except Exception as e:
        logger.error(f"Upsert operation failed: {e}")
        raise PgsqlUpserterError(f"Failed to execute upsert: {e}") from e
