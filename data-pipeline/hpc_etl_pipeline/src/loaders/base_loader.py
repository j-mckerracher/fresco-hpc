
from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd

class BaseLoader(ABC):
    """Abstract base class for all data loaders."""

    @abstractmethod
    def load(self, data: pd.DataFrame, config: Dict):
        """
        Loads the given data according to the configuration.

        Args:
            data: The DataFrame to load.
            config: A dictionary containing the loader configuration.
        """
        pass
