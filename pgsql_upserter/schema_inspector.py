"""PostgreSQL table schema introspection utilities."""

import logging
import psycopg2

from dataclasses import dataclass
from psycopg2.extras import RealDictCursor

from .exceptions import TableNotFoundError, SchemaIntrospectionError

logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    """Information about a table column."""
    name: str
    data_type: str
    is_nullable: bool
    default_value: str | None
    is_auto_generated: bool
    ordinal_position: int


@dataclass
class UniqueConstraint:
    """Information about a unique constraint."""
    name: str
    columns: list[str]
    is_primary: bool


@dataclass
class TableSchema:
    """Complete table schema information."""
    table_name: str
    schema_name: str
    columns: list[ColumnInfo]
    unique_constraints: list[UniqueConstraint]
    primary_key: UniqueConstraint | None

    @property
    def valid_columns(self) -> list[str]:
        """Non-auto-generated column names for data insertion."""
        return [col.name for col in self.columns if not col.is_auto_generated]


def _is_auto_generated_column(column_default: str, data_type: str) -> bool:
    """Determine if a column is auto-generated.

    Checks for:
    - SERIAL/BIGSERIAL types
    - nextval() sequences
    - GENERATED columns
    - DEFAULT CURRENT_TIMESTAMP
    """
    if not column_default:
        return False

    column_default = column_default.lower()
    data_type = data_type.lower()

    # SERIAL types
    if data_type in ('serial', 'bigserial'):
        return True

    # Sequence defaults
    if 'nextval(' in column_default:
        return True

    # Generated columns
    if column_default.startswith('generated'):
        return True

    # Timestamp defaults
    if any(ts in column_default for ts in ['current_timestamp', 'now()', 'clock_timestamp()']):
        return True

    return False


def inspect_table_schema(
    connection,
    table_name: str,
    schema: str = 'public'
) -> TableSchema:
    """Inspect PostgreSQL table schema and return structured information.

    Args:
        connection: Active PostgreSQL connection
        table_name: Name of the table to inspect
        schema: Schema name (default: 'public')

    Returns:
        TableSchema: Complete table schema information

    Raises:
        TableNotFoundError: If table doesn't exist
        SchemaIntrospectionError: If schema cannot be introspected
    """
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # First, verify table exists
            cursor.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """, (schema, table_name))

            if not cursor.fetchone():
                raise TableNotFoundError(f"Table '{schema}.{table_name}' not found")

            # Get column information
            columns = _get_columns_info(cursor, table_name, schema)

            # Get unique constraints
            unique_constraints = _get_unique_constraints(cursor, table_name, schema)

            # Find primary key
            primary_key = next((uc for uc in unique_constraints if uc.is_primary), None)

            logger.debug(f"Successfully introspected table '{schema}.{table_name}' "
                         f"with {len(columns)} columns and {len(unique_constraints)} constraints")

            return TableSchema(
                table_name=table_name,
                schema_name=schema,
                columns=columns,
                unique_constraints=unique_constraints,
                primary_key=primary_key
            )

    except psycopg2.Error as e:
        raise SchemaIntrospectionError(f"Failed to introspect table '{schema}.{table_name}': {e}")


def _get_columns_info(cursor, table_name: str, schema: str) -> list[ColumnInfo]:
    """Get detailed column information from information_schema."""
    cursor.execute("""
        SELECT
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            c.ordinal_position,
            c.is_generated,
            c.generation_expression
        FROM information_schema.columns c
        WHERE c.table_schema = %s AND c.table_name = %s
        ORDER BY c.ordinal_position
    """, (schema, table_name))

    columns = []
    for row in cursor.fetchall():
        # Check if column is auto-generated
        is_auto_generated = (
            # SERIAL types
            row['data_type'] in ('serial', 'bigserial') or
            # Sequence defaults
            (row['column_default'] and 'nextval(' in row['column_default'].lower()) or
            # Generated columns (PostgreSQL 12+)
            row['is_generated'] == 'ALWAYS' or
            # Timestamp defaults
            (row['column_default'] and any(ts in row['column_default'].lower()
                                           for ts in ['current_timestamp', 'now()', 'clock_timestamp()']))
        )

        columns.append(ColumnInfo(
            name=row['column_name'],
            data_type=row['data_type'],
            is_nullable=row['is_nullable'] == 'YES',
            default_value=row['column_default'],
            is_auto_generated=is_auto_generated,
            ordinal_position=row['ordinal_position']
        ))

    return columns


def _get_unique_constraints(cursor, table_name: str, schema: str) -> list[UniqueConstraint]:
    """Get unique constraints from information_schema."""
    cursor.execute("""
        SELECT
            tc.constraint_name,
            tc.constraint_type,
            array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = %s
            AND tc.table_name = %s
            AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
        GROUP BY tc.constraint_name, tc.constraint_type
        ORDER BY tc.constraint_type DESC, tc.constraint_name
    """, (schema, table_name))

    constraints = []
    for row in cursor.fetchall():
        # Parse PostgreSQL array format: {col1,col2,col3} -> ['col1', 'col2', 'col3']
        columns_raw = row['columns']
        if isinstance(columns_raw, list):
            # Already a list (some PostgreSQL drivers)
            columns = columns_raw
        elif isinstance(columns_raw, str):
            # Parse PostgreSQL array string format
            columns = columns_raw.strip('{}').split(',')
        else:
            # Fallback
            columns = [str(columns_raw)]

        constraints.append(UniqueConstraint(
            name=row['constraint_name'],
            columns=columns,
            is_primary=row['constraint_type'] == 'PRIMARY KEY'
        ))

    return constraints
