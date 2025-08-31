"""PostgreSQL dynamic upsert utility with automatic schema introspection."""

import logging

from .config import create_connection_from_env, test_connection, validate_permissions
from .schema_inspector import inspect_table_schema, TableSchema, ColumnInfo, UniqueConstraint
from .column_matcher import match_columns
from .temp_staging import create_temp_table, bulk_insert_to_temp, populate_temp_table, convert_temp_to_permanent
from .conflict_resolver import (
    find_conflict_strategy,
    deduplicate_temp_table,
    execute_upsert,
    ConflictStrategy,
    DeduplicationResult
)
from .upsert_engine import (
    UpsertEngine,
    UpsertResult,
    read_csv_to_dict_list,
)
from .exceptions import (
    PgsqlUpserterError,
    ConnectionError,
    PermissionError,
    TableNotFoundError,
    SchemaIntrospectionError
)

# Configure package-level logging
_package_logger = logging.getLogger(__name__)
_package_logger.setLevel(logging.INFO)

# Create console handler if no handlers exist
if not _package_logger.handlers and not _package_logger.parent.handlers:  # type: ignore
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    _package_logger.addHandler(console_handler)

# Disable propagation to prevent duplicate messages when external logging is configured
_package_logger.propagate = False


__all__ = [
    # Main API - Upsert Engine
    'UpsertEngine',
    'UpsertResult',
    'read_csv_to_dict_list',

    # Connection utilities
    'create_connection_from_env',
    'test_connection',
    'validate_permissions',

    # Lower-level components
    'inspect_table_schema',
    'match_columns',
    'create_temp_table',
    'populate_temp_table',
    'bulk_insert_to_temp',
    'convert_temp_to_permanent',

    # Conflict resolution components
    'find_conflict_strategy',
    'deduplicate_temp_table',
    'execute_upsert',

    # Data classes
    'TableSchema',
    'ColumnInfo',
    'UniqueConstraint',
    'ConflictStrategy',
    'DeduplicationResult',

    # Exceptions
    'PgsqlUpserterError',
    'ConnectionError',
    'PermissionError',
    'TableNotFoundError',
    'SchemaIntrospectionError',
]
