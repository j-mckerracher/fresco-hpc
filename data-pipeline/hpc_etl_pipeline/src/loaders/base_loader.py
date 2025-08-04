"""Base loader class for data loading."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Optional
import polars as pl
import logging

logger = logging.getLogger(__name__)


class BaseLoader(ABC):
    """Abstract base class for data loaders."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize loader with configuration.
        
        Args:
            config: Loader configuration
        """
        self.config = config
        self.output_config = config.get('output', {})
        
    @abstractmethod
    def load(self, data: pl.DataFrame, output_path: Path, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Load data to the specified output path.
        
        Args:
            data: DataFrame to load
            output_path: Path where data should be saved
            metadata: Optional metadata about the data
            
        Returns:
            True if load was successful
        """
        pass
    
    @abstractmethod
    def validate_output(self, output_path: Path) -> bool:
        """
        Validate that the output file was created successfully.
        
        Args:
            output_path: Path to the output file
            
        Returns:
            True if output is valid
        """
        pass
    
    def get_output_format(self) -> str:
        """Get output format from configuration."""
        return self.output_config.get('format', 'parquet')
    
    def get_compression(self) -> str:
        """Get compression setting from configuration."""
        return self.output_config.get('compression', 'snappy')
    
    def should_chunk(self) -> bool:
        """Check if chunking is enabled."""
        chunking = self.output_config.get('chunking', {})
        return chunking.get('enabled', False)
    
    def get_max_chunk_size(self) -> float:
        """Get maximum chunk size in GB."""
        chunking = self.output_config.get('chunking', {})
        return chunking.get('max_size_gb', 2.0)
    
    def get_min_rows_per_chunk(self) -> int:
        """Get minimum rows per chunk."""
        chunking = self.output_config.get('chunking', {})
        return chunking.get('min_rows_per_chunk', 500000)
    
    def generate_output_path(self, metadata: Dict[str, Any]) -> Path:
        """
        Generate output path based on configuration template.
        
        Args:
            metadata: Metadata for path generation
            
        Returns:
            Generated output path
        """
        template = self.output_config.get('path_template', '{dataset_name}_{timestamp}.{format}')
        format_ext = self.get_output_format()
        
        # Add default values if not in metadata
        if 'timestamp' not in metadata:
            from datetime import datetime
            metadata['timestamp'] = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Replace template variables
        filename = template.format(**metadata, format=format_ext)
        return Path(filename)