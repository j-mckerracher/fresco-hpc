"""Generic reusable data transformers."""

from typing import Dict, Any, List, Optional
import polars as pl
import logging

from .base_transformer import BaseTransformer
from ..core.exceptions import TransformationError

logger = logging.getLogger(__name__)


class SuffixTransformer(BaseTransformer):
    """Adds suffixes to specified columns."""
    
    def transform(self, data: pl.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """Add suffixes to specified columns."""
        if not self.validate_input(data):
            return data
        
        # Find suffix transformation config
        suffix_config = None
        for transform in self.transformation_config:
            if transform.get('type') == 'suffix_transform':
                suffix_config = transform
                break
        
        if not suffix_config:
            return data
        
        suffix = suffix_config.get('suffix', '')
        columns = suffix_config.get('columns', [])
        
        if not suffix or not columns:
            return data
        
        try:
            # Apply suffix to specified columns that exist in the dataframe
            transformations = []
            for col in columns:
                if col in data.columns:
                    transformations.append(
                        pl.col(col).map_elements(
                            lambda x: f"{x}{suffix}" if x is not None else x,
                            return_dtype=pl.Utf8
                        ).alias(col)
                    )
            
            if transformations:
                result = data.with_columns(transformations)
                logger.debug(f"Applied suffix '{suffix}' to columns: {columns}")
                return result
            
            return data
            
        except Exception as e:
            raise TransformationError(f"Failed to apply suffix transformation: {e}")


class ColumnReorderTransformer(BaseTransformer):
    """Reorders columns according to specified schema."""
    
    def transform(self, data: pl.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """Reorder columns according to expected schema."""
        if not self.validate_input(data):
            return data
        
        expected_columns = self.get_expected_columns()
        if not expected_columns:
            return data
        
        try:
            # Only select columns that exist in both the data and expected schema
            available_columns = [col for col in expected_columns if col in data.columns]
            
            if available_columns:
                result = data.select(available_columns)
                logger.debug(f"Reordered columns to: {available_columns}")
                return result
            
            return data
            
        except Exception as e:
            raise TransformationError(f"Failed to reorder columns: {e}")


class StringNormalizer(BaseTransformer):
    """Normalizes strings using pattern replacements."""
    
    def transform(self, data: pl.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """Apply string normalization patterns."""
        if not self.validate_input(data):
            return data
        
        # Find job_id_normalization config
        normalization_config = None
        for transform in self.transformation_config:
            if transform.get('type') == 'job_id_normalization':
                normalization_config = transform
                break
        
        if not normalization_config:
            return data
        
        patterns = normalization_config.get('patterns', [])
        if not patterns:
            return data
        
        try:
            transformations = []
            
            # Apply patterns to all string columns
            for col in data.columns:
                if data[col].dtype == pl.Utf8:
                    col_expr = pl.col(col)
                    
                    # Apply each pattern replacement
                    for pattern in patterns:
                        find_str = pattern.get('find', '')
                        replace_str = pattern.get('replace', '')
                        if find_str:
                            col_expr = col_expr.str.replace_all(find_str, replace_str)
                    
                    transformations.append(col_expr.alias(col))
            
            if transformations:
                # Keep non-string columns as they are
                non_string_cols = [pl.col(col) for col in data.columns 
                                 if data[col].dtype != pl.Utf8]
                all_transformations = transformations + non_string_cols
                
                result = data.with_columns(all_transformations)
                logger.debug(f"Applied string normalization patterns to {len(transformations)} columns")
                return result
            
            return data
            
        except Exception as e:
            raise TransformationError(f"Failed to apply string normalization: {e}")


class TimestampNormalizer(BaseTransformer):
    """Normalizes timestamp formats."""
    
    def transform(self, data: pl.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """Normalize timestamp formats."""
        if not self.validate_input(data):
            return data
        
        try:
            transformations = []
            
            # Look for timestamp-like columns
            timestamp_columns = [col for col in data.columns 
                               if 'time' in col.lower() or 'timestamp' in col.lower()]
            
            for col in timestamp_columns:
                if col in data.columns:
                    # Try to parse as datetime if it's not already
                    if data[col].dtype != pl.Datetime:
                        try:
                            # Attempt to parse various timestamp formats
                            transformations.append(
                                pl.col(col).str.strptime(pl.Datetime, format=None, strict=False).alias(col)
                            )
                        except:
                            # If parsing fails, keep original
                            transformations.append(pl.col(col))
                    else:
                        transformations.append(pl.col(col))
            
            if transformations:
                # Keep other columns as they are
                other_cols = [pl.col(col) for col in data.columns 
                            if col not in timestamp_columns]
                all_transformations = transformations + other_cols
                
                result = data.with_columns(all_transformations)
                logger.debug(f"Normalized timestamps in columns: {timestamp_columns}")
                return result
            
            return data
            
        except Exception as e:
            raise TransformationError(f"Failed to normalize timestamps: {e}")


class UnitColumnAdder(BaseTransformer):
    """Adds missing unit column."""
    
    def transform(self, data: pl.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """Add unit column if missing."""
        if not self.validate_input(data):
            return data
        
        try:
            if "unit" not in data.columns:
                # Add unit column with empty string as default
                result = data.with_columns(pl.lit("").alias("unit"))
                logger.debug("Added missing 'unit' column")
                return result
            
            return data
            
        except Exception as e:
            raise TransformationError(f"Failed to add unit column: {e}")


class SchemaStandardizer(BaseTransformer):
    """Standardizes data to expected output schema."""
    
    def transform(self, data: pl.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """Standardize data to expected schema."""
        if not self.validate_input(data):
            return data
        
        expected_columns = self.get_expected_columns()
        if not expected_columns:
            return data
        
        try:
            # Start with available columns that match expected schema
            available_columns = []
            transformations = []
            
            for expected_col in expected_columns:
                if expected_col in data.columns:
                    available_columns.append(expected_col)
                    transformations.append(pl.col(expected_col))
                else:
                    # Add placeholder for missing columns
                    transformations.append(pl.lit(None).alias(expected_col))
                    logger.debug(f"Added placeholder for missing column: {expected_col}")
            
            if transformations:
                result = data.select(transformations)
                logger.debug(f"Standardized to schema with {len(expected_columns)} columns")
                return result
            
            return data
            
        except Exception as e:
            raise TransformationError(f"Failed to standardize schema: {e}")


class CompositeTransformer(BaseTransformer):
    """Applies multiple transformations in sequence."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.transformers = self._create_transformers()
    
    def _create_transformers(self) -> List[BaseTransformer]:
        """Create individual transformers based on configuration."""
        transformers = []
        
        for transform_config in self.transformation_config:
            transform_type = transform_config.get('type')
            
            if transform_type == 'suffix_transform':
                transformers.append(SuffixTransformer(self.config))
            elif transform_type == 'job_id_normalization':
                transformers.append(StringNormalizer(self.config))
            elif transform_type == 'standardize_columns':
                transformers.append(SchemaStandardizer(self.config))
            elif transform_type == 'add_unit_column':
                transformers.append(UnitColumnAdder(self.config))
            elif transform_type == 'normalize_timestamps':
                transformers.append(TimestampNormalizer(self.config))
        
        # Always add column reorderer at the end
        transformers.append(ColumnReorderTransformer(self.config))
        
        return transformers
    
    def transform(self, data: pl.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """Apply all transformations in sequence."""
        if not self.validate_input(data):
            return data
        
        result = data
        
        for transformer in self.transformers:
            try:
                result = transformer.transform(result, metadata)
            except Exception as e:
                logger.error(f"Transformation failed with {transformer.__class__.__name__}: {e}")
                # Continue with other transformations
                continue
        
        if not self.validate_output(result):
            logger.warning("Output validation failed after transformations")
        
        return result