"""Custom exceptions for pgsql_upserter package."""


class PgsqlUpserterError(Exception):
    """Base exception for all pgsql_upserter errors."""
    pass


class ConnectionError(PgsqlUpserterError):
    """Raised when database connection fails."""
    pass


class PermissionError(PgsqlUpserterError):
    """Raised when user lacks required database permissions."""
    pass


class TableNotFoundError(PgsqlUpserterError):
    """Raised when target table doesn't exist."""
    pass


class SchemaIntrospectionError(PgsqlUpserterError):
    """Raised when table schema cannot be introspected."""
    pass
