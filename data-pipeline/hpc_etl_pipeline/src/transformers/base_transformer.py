"""Base transformer class for data transformation."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd
import polars as pl
import logging

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """Abstract base class for data transformers."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transformer with configuration.
        
        Args:
            config: Transformer configuration
        """
        self.config = config
        self.transformation_config = config.get('transformations', [])
        
    @abstractmethod
    def transform(self, data: pl.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """
        Transform the input data.
        
        Args:
            data: Input DataFrame to transform
            metadata: Optional metadata about the data
            
        Returns:
            Transformed DataFrame
        """
        pass
    
    def validate_input(self, data: pl.DataFrame) -> bool:
        """
        Validate input data before transformation.
        
        Args:
            data: Input DataFrame to validate
            
        Returns:
            True if data is valid for transformation
        """
        if data.is_empty():
            logger.warning("Input DataFrame is empty")
            return False
        return True
    
    def validate_output(self, data: pl.DataFrame) -> bool:
        """
        Validate output data after transformation.
        
        Args:
            data: Output DataFrame to validate
            
        Returns:
            True if output data is valid
        """
        if data.is_empty():
            logger.warning("Output DataFrame is empty")
            return False
        return True
    
    def get_expected_columns(self) -> list:
        """Get expected output columns from configuration."""
        for transform in self.transformation_config:
            if transform.get('type') == 'standardize_columns':
                return transform.get('output_schema', [])
        return []