
import gc
import pandas as pd

from hpc_etl_pipeline.src.utils.logger import get_logger

logger = get_logger(__name__)

class MemoryOptimizer:
    """Optimizes memory usage during processing."""

    def __init__(self, memory_limit_gb: int = 80):
        self.memory_limit_gb = memory_limit_gb

    def optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimizes the memory usage of a DataFrame.

        Args:
            df: The DataFrame to optimize.

        Returns:
            The optimized DataFrame.
        """
        # Downcast numeric columns
        for col in df.select_dtypes(include=["int64", "float64"]):
            if df[col].dtype == "int64":
                df[col] = pd.to_numeric(df[col], downcast="integer")
            else:
                df[col] = pd.to_numeric(df[col], downcast="float")

        # Convert object columns to category where appropriate
        for col in df.select_dtypes(include=["object"]):
            if len(df[col].unique()) / len(df[col]) < 0.5:
                df[col] = df[col].astype("category")

        gc.collect()
        return df
