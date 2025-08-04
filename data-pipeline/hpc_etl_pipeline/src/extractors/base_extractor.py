
from abc import ABC, abstractmethod
from typing import Dict, Iterator
from pathlib import Path

class BaseExtractor(ABC):
    """Abstract base class for all data extractors."""

    @abstractmethod
    def extract(self, source_config: Dict) -> Iterator[Path]:
        """
        Extracts data from a source as defined by the configuration.

        Args:
            source_config: A dictionary containing the source configuration.

        Returns:
            An iterator of PosixPaths to the extracted files.
        """
        pass
