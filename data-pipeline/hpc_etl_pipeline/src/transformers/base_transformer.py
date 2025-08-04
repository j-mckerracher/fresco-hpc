
from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd

class BaseTransformer(ABC):
    """Abstract base class for all data transformers."""

    @abstractmethod
    def transform(self, data: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """
        Transforms the given data according to the configuration.

        Args:
            data: The input DataFrame to transform.
            config: A dictionary containing the transformation configuration.

        Returns:
            The transformed DataFrame.
        """
        pass
