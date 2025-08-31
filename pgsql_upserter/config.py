"""Database connection and configuration utilities."""

import logging
import os
import psycopg2

from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

from .exceptions import ConnectionError, PermissionError

load_dotenv()

logger = logging.getLogger(__name__)


def create_connection_from_env():
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
    try:
        connection_params = {
            'host': os.getenv('PGHOST', 'localhost'),
            'port': int(os.getenv('PGPORT', '5432')),
            'database': os.getenv('PGDATABASE'),
            'user': os.getenv('PGUSER'),
            'password': os.getenv('PGPASSWORD'),
        }

        # Validate required parameters
        if not all([connection_params['database'], connection_params['user'], connection_params['password']]):
            raise ConnectionError(
                "Missing required environment variables: PGDATABASE, PGUSER, PGPASSWORD")

        logger.debug(
            f"Connecting to PostgreSQL: {connection_params['user']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}")  # noqa

        connection = psycopg2.connect(**connection_params)
        connection.autocommit = False  # Explicit transaction control

        return connection

    except psycopg2.Error as e:
        raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")
    except Exception as e:
        raise ConnectionError(f"Unexpected error during connection: {e}")


def validate_permissions(connection) -> None:
    """Validate that user has required permissions for upsert operations.

    Checks:
    - CREATE privilege on database (for temp tables)
    - General connection health

    Args:
        connection: Active PostgreSQL connection

    Raises:
        PermissionError: If user lacks required permissions
    """
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Test CREATE TEMP TABLE permission
            cursor.execute("""
                CREATE TEMP TABLE pgsql_upserter_permission_test (
                    test_col INTEGER
                );
                DROP TABLE pgsql_upserter_permission_test;
            """)
            connection.commit()

            logger.debug("Permission validation successful")

    except psycopg2.Error as e:
        connection.rollback()
        raise PermissionError(f"Insufficient permissions for upsert operations: {e}")


def test_connection():
    """Test database connection and validate permissions.

    Returns:
        psycopg2.connection: Validated connection ready for use

    Raises:
        ConnectionError: If connection fails
        PermissionError: If user lacks required permissions
    """
    connection = create_connection_from_env()
    validate_permissions(connection)

    logger.debug("Database connection and permissions validated successfully")
    return connection
