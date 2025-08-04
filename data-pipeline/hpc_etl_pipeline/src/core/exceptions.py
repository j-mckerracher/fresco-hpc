
class PipelineError(Exception):
    """Base exception for all pipeline errors."""
    pass

class ConfigurationError(PipelineError):
    """Raised for errors in configuration."""
    pass

class ExtractionError(PipelineError):
    """Raised for errors during data extraction."""
    pass

class TransformationError(PipelineError):
    """Raised for errors during data transformation."""
    pass

class LoadError(PipelineError):
    """Raised for errors during data loading."""
    pass

class SchemaMismatchError(TransformationError):
    """Raised when data does not match the expected schema."""
    pass
