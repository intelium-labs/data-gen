"""Custom exception hierarchy for data-gen."""


class DataGenError(Exception):
    """Base exception for all data-gen errors."""


class EntityNotFoundError(DataGenError):
    """Raised when a referenced entity does not exist."""


class ReferentialIntegrityError(EntityNotFoundError):
    """Raised when a foreign key reference is violated."""


class InvalidEntityStateError(DataGenError):
    """Raised when an entity is in an invalid state for the operation."""


class ConfigurationError(DataGenError):
    """Raised when configuration is invalid or missing."""


class SinkError(DataGenError):
    """Raised when a sink operation fails."""
