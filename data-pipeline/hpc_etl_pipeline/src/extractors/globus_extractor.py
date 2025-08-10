"""Globus-based extractor for single file processing."""

from pathlib import Path
from typing import Iterator, Dict, Any, Optional
import logging

from .base_extractor import BaseExtractor
from ..core.exceptions import ExtractionError

logger = logging.getLogger(__name__)


class GlobusExtractor(BaseExtractor):
    """Extractor for files received via Globus transfers."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.endpoint_id = self.source_config.get('endpoint_id')
        self.base_path = self.source_config.get('path', '/')
    
    def extract(self, source: Optional[str] = None) -> Iterator[Path]:
        """
        Extract file provided by Globus transfer.
        
        Args:
            source: Path to the file received from Globus
            
        Yields:
            Path to the received file
        """
        if not source:
            raise ExtractionError("Globus extractor requires a source file path")
        
        file_path = Path(source)
        
        if not file_path.exists():
            raise ExtractionError(f"Globus file does not exist: {file_path}")
        
        if not file_path.is_file():
            raise ExtractionError(f"Globus source is not a file: {file_path}")
        
        # Validate file matches expected patterns if configured
        file_patterns = self.get_file_patterns()
        if file_patterns:
            matched = any(file_path.match(pattern) for pattern in file_patterns)
            if not matched:
                logger.warning(f"File {file_path} does not match expected patterns: {file_patterns}")
        
        logger.info(f"Processing Globus file: {file_path}")
        yield file_path
    
    def validate_source(self) -> bool:
        """Validate Globus configuration."""
        # For Globus, we can't validate until we receive a file
        # Just check that basic configuration is present
        return bool(self.endpoint_id) or bool(self.base_path)