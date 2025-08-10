"""Custom exceptions for the HPC ETL Pipeline."""


class HpcEtlException(Exception):
    """Base exception for HPC ETL Pipeline."""
    pass


class ConfigurationError(HpcEtlException):
    """Raised when there's an error in configuration."""
    pass


class ExtractionError(HpcEtlException):
    """Raised when there's an error during data extraction."""
    pass


class TransformationError(HpcEtlException):
    """Raised when there's an error during data transformation."""
    pass


class LoadError(HpcEtlException):
    """Raised when there's an error during data loading."""
    pass


class ValidationError(HpcEtlException):
    """Raised when data validation fails."""
    pass


class SchemaMismatchError(ValidationError):
    """Raised when data schema doesn't match expected format."""
    pass


class DiskSpaceError(HpcEtlException):
    """Raised when insufficient disk space is available."""
    pass


class MemoryError(HpcEtlException):
    """Raised when memory limits are exceeded."""
    pass