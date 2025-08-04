
from typing import Dict, Iterator
from pathlib import Path

from hpc_etl_pipeline.src.core.exceptions import ExtractionError
from hpc_etl_pipeline.src.extractors.base_extractor import BaseExtractor
from hpc_etl_pipeline.src.utils.logger import get_logger

logger = get_logger(__name__)

class GlobusExtractor(BaseExtractor):
    """Extracts data from a Globus endpoint."""

    def extract(self, source_config: Dict) -> Iterator[Path]:
        """
        Receives a file path from a Globus watcher and yields it.

        Args:
            source_config: A dictionary containing the source configuration.

        Returns:
            An iterator yielding the path to the extracted file.
        """
        file_path = source_config.get("file_path")
        if not file_path:
            raise ExtractionError("Missing 'file_path' in source configuration for GlobusExtractor.")

        path = Path(file_path)
        if not path.exists():
            raise ExtractionError(f"File received from Globus does not exist: {file_path}")

        logger.info(f"Received file from Globus: {file_path}")
        yield path
