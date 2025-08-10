"""Base extractor class for data extraction."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract base class for data extractors."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize extractor with configuration.
        
        Args:
            config: Extractor configuration
        """
        self.config = config
        self.source_config = config.get('source', {})
        
    @abstractmethod
    def extract(self, source: Optional[str] = None) -> Iterator[Path]:
        """
        Extract data from source and yield file paths.
        
        Args:
            source: Optional source override
            
        Yields:
            Path objects to extracted files
        """
        pass
    
    @abstractmethod
    def validate_source(self) -> bool:
        """
        Validate that the source is accessible.
        
        Returns:
            True if source is valid and accessible
        """
        pass
    
    def get_file_patterns(self) -> list:
        """Get file patterns from configuration."""
        return self.source_config.get('file_patterns', [])
    
    def get_folder_pattern(self) -> Optional[str]:
        """Get folder pattern from configuration."""
        return self.source_config.get('folder_pattern')
    
    def cleanup(self):
        """Clean up any resources used by the extractor."""
        pass