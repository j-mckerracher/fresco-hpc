"""Data validation utilities."""

from pathlib import Path
from typing import List, Optional, Dict, Any
import polars as pl
import logging

logger = logging.getLogger(__name__)


def validate_dataframe(df: pl.DataFrame, 
                      required_columns: Optional[List[str]] = None,
                      min_rows: int = 1,
                      max_rows: Optional[int] = None) -> bool:
    """
    Validate DataFrame structure and content.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        min_rows: Minimum number of rows required
        max_rows: Maximum number of rows allowed
        
    Returns:
        True if DataFrame is valid
    """
    try:
        # Check if DataFrame is empty
        if df.is_empty():
            logger.error("DataFrame is empty")
            return False
        
        # Check row count
        row_count = len(df)
        if row_count < min_rows:
            logger.error(f"DataFrame has {row_count} rows, minimum required: {min_rows}")
            return False
        
        if max_rows and row_count > max_rows:
            logger.error(f"DataFrame has {row_count} rows, maximum allowed: {max_rows}")
            return False
        
        # Check required columns
        if required_columns:
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return False
        
        logger.debug(f"DataFrame validation passed: {row_count} rows, {len(df.columns)} columns")
        return True
        
    except Exception as e:
        logger.error(f"Error validating DataFrame: {e}")
        return False


def validate_parquet_file(file_path: Path, 
                         expected_min_rows: int = 1,
                         required_columns: Optional[List[str]] = None) -> bool:
    """
    Validate that a Parquet file is properly written and readable.
    
    Args:
        file_path: Path to the Parquet file to validate
        expected_min_rows: Minimum number of rows expected
        required_columns: List of required column names
        
    Returns:
        True if file is valid, False otherwise
    """
    try:
        if not file_path.exists():
            logger.error(f"File does not exist: {file_path}")
            return False
        
        file_size = file_path.stat().st_size
        if file_size == 0:
            logger.error(f"File is empty: {file_path}")
            return False
        
        # Try to read the file
        try:
            # For large files, just check that we can read some rows
            if expected_min_rows > 10000:
                test_df = pl.read_parquet(file_path, n_rows=min(1000, expected_min_rows))
                if test_df.is_empty():
                    logger.error(f"File appears to be empty: {file_path}")
                    return False
                
                # For very large files, don't read the entire file for row count
                if expected_min_rows > 100000:
                    logger.debug(f"Large file validation passed (sample check): {file_path}")
                    return True
                else:
                    # Get the actual row count for medium-sized files
                    try:
                        actual_rows = len(pl.read_parquet(file_path))
                        if actual_rows < expected_min_rows:
                            logger.error(f"File has {actual_rows} rows, expected at least {expected_min_rows}: {file_path}")
                            return False
                    except Exception as e:
                        logger.warning(f"Could not get exact row count for {file_path}: {e}")
                        return len(test_df) >= min(100, expected_min_rows)
            else:
                # For smaller files, read and validate fully
                test_df = pl.read_parquet(file_path)
                return validate_dataframe(test_df, required_columns, expected_min_rows)
            
            logger.debug(f"File validation passed: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Error validating file {file_path}: {e}")
        return False


def validate_config_schema(config: Dict[str, Any]) -> List[str]:
    """
    Validate configuration schema and return list of errors.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    # Check required top-level sections
    required_sections = ['dataset', 'source', 'output']
    for section in required_sections:
        if section not in config:
            errors.append(f"Missing required section: {section}")
    
    # Validate dataset section
    if 'dataset' in config:
        dataset = config['dataset']
        required_fields = ['name', 'type', 'version']
        for field in required_fields:
            if field not in dataset:
                errors.append(f"Missing required field in dataset section: {field}")
    
    # Validate source section
    if 'source' in config:
        source = config['source']
        if 'type' not in source:
            errors.append("Missing 'type' in source configuration")
        
        source_type = source.get('type')
        if source_type == 'remote_http' and 'base_url' not in source:
            errors.append("Missing 'base_url' for remote_http source type")
        elif source_type == 'local_fs' and 'base_path' not in source:
            errors.append("Missing 'base_path' for local_fs source type")
        elif source_type == 'globus' and 'endpoint_id' not in source:
            errors.append("Missing 'endpoint_id' for globus source type")
    
    # Validate output section
    if 'output' in config:
        output = config['output']
        valid_formats = ['parquet', 'csv']
        if 'format' in output and output['format'] not in valid_formats:
            errors.append(f"Invalid output format. Must be one of: {valid_formats}")
    
    # Validate transformations if present
    if 'transformations' in config:
        for i, transform in enumerate(config['transformations']):
            if 'type' not in transform:
                errors.append(f"Missing 'type' in transformation {i}")
    
    return errors


def validate_file_patterns(patterns: List[str]) -> bool:
    """
    Validate file patterns for common issues.
    
    Args:
        patterns: List of file patterns to validate
        
    Returns:
        True if patterns are valid
    """
    if not patterns:
        logger.warning("No file patterns specified")
        return False
    
    for pattern in patterns:
        if not isinstance(pattern, str):
            logger.error(f"Invalid pattern type: {type(pattern)}")
            return False
        
        if not pattern.strip():
            logger.error("Empty pattern found")
            return False
    
    return True


def validate_numeric_range(value: float, 
                          min_value: Optional[float] = None,
                          max_value: Optional[float] = None,
                          field_name: str = "value") -> bool:
    """
    Validate numeric value is within specified range.
    
    Args:
        value: Value to validate
        min_value: Minimum allowed value
        max_value: Maximum allowed value
        field_name: Name of the field for error messages
        
    Returns:
        True if value is valid
    """
    try:
        if min_value is not None and value < min_value:
            logger.error(f"{field_name} {value} is below minimum {min_value}")
            return False
        
        if max_value is not None and value > max_value:
            logger.error(f"{field_name} {value} is above maximum {max_value}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating {field_name}: {e}")
        return False