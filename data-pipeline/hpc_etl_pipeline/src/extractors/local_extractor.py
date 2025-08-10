"""Local filesystem extractor."""

import re
from pathlib import Path
from typing import Iterator, Dict, Any, Optional
import logging

from .base_extractor import BaseExtractor
from ..core.exceptions import ExtractionError

logger = logging.getLogger(__name__)


class LocalExtractor(BaseExtractor):
    """Extractor for local filesystem sources."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_path = Path(self.source_config.get('base_path', '.'))
        
        if not self.base_path.exists():
            raise ExtractionError(f"Base path does not exist: {self.base_path}")
    
    def extract(self, source: Optional[str] = None) -> Iterator[Path]:
        """
        Extract files from local filesystem.
        
        Args:
            source: Optional specific path to extract
            
        Yields:
            Path objects to found files
        """
        try:
            if source:
                source_path = Path(source)
                if source_path.is_file():
                    yield source_path
                elif source_path.is_dir():
                    yield from self._extract_directory(source_path)
            else:
                yield from self._extract_directory(self.base_path)
                
        except Exception as e:
            raise ExtractionError(f"Failed to extract from local source: {e}")
    
    def _extract_directory(self, directory: Path) -> Iterator[Path]:
        """Extract files from a directory."""
        file_patterns = self.get_file_patterns()
        folder_pattern = self.get_folder_pattern()
        
        if folder_pattern:
            # Look for folders matching the pattern first
            for item in directory.iterdir():
                if item.is_dir() and re.match(folder_pattern, item.name):
                    yield from self._extract_files_from_folder(item, file_patterns)
        else:
            # Extract files directly from the directory
            yield from self._extract_files_from_folder(directory, file_patterns)
    
    def _extract_files_from_folder(self, folder: Path, file_patterns: list) -> Iterator[Path]:
        """Extract files matching patterns from a folder."""
        if not file_patterns:
            # If no patterns specified, yield all files
            for file_path in folder.iterdir():
                if file_path.is_file():
                    yield file_path
        else:
            # Match against patterns
            for pattern in file_patterns:
                for file_path in folder.glob(pattern):
                    if file_path.is_file():
                        yield file_path
    
    def validate_source(self) -> bool:
        """Validate that the local source is accessible."""
        return self.base_path.exists() and self.base_path.is_dir()