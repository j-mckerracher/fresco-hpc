
import pandas as pd
from typing import Dict, List

from hpc_etl_pipeline.src.transformers.base_transformer import BaseTransformer
from hpc_etl_pipeline.src.utils.logger import get_logger

logger = get_logger(__name__)

class SuffixTransformer(BaseTransformer):
    """Applies a suffix to specified columns."""

    def transform(self, data: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """
        Applies a suffix to the specified columns.

        Args:
            data: The input DataFrame.
            config: The transformation configuration, containing 'suffix' and 'columns'.

        Returns:
            The transformed DataFrame.
        """
        suffix = config.get("suffix")
        columns = config.get("columns")

        if not suffix or not columns:
            logger.warning("SuffixTransformer is missing 'suffix' or 'columns' in config.")
            return data

        logger.info(f"Applying suffix '{suffix}' to columns: {columns}")
        for col in columns:
            if col in data.columns:
                if col == "jid":
                    data[col] = data[col].astype(str).str.replace("jobID", "JOB", case=False).str.cat(suffix, na_rep="")
                elif col == "host_list":
                    # This is a placeholder for the more complex regex logic from the original script.
                    # A more robust implementation would be needed for production.
                    data[col] = data[col].astype(str).str.replace(r"([A-Z]+\d+)", r"\1_C", regex=True)
                else:
                    data[col] = data[col].astype(str).str.cat(suffix, na_rep="")
        return data

class ColumnReorderTransformer(BaseTransformer):
    """Reorders columns in the DataFrame."""

    def transform(self, data: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """
        Reorders the columns of the DataFrame based on the provided order.

        Args:
            data: The input DataFrame.
            config: The transformation configuration, containing 'output_schema'.

        Returns:
            The DataFrame with reordered columns.
        """
        output_schema = config.get("output_schema")
        if not output_schema:
            logger.warning("ColumnReorderTransformer is missing 'output_schema' in config.")
            return data

        logger.info(f"Reordering columns to: {output_schema}")
        existing_columns = [col for col in output_schema if col in data.columns]
        other_columns = [col for col in data.columns if col not in output_schema]
        return data[existing_columns + other_columns]

class TimestampNormalizer(BaseTransformer):
    """Normalizes timestamp columns to a consistent format."""

    def transform(self, data: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """
        Normalizes timestamp columns to a consistent format.

        Args:
            data: The input DataFrame.
            config: The transformation configuration, containing 'timestamp_columns'.

        Returns:
            The transformed DataFrame.
        """
        timestamp_columns = config.get("timestamp_columns", ["Timestamp"])
        output_format = config.get("output_format", "%Y-%m-%d %H:%M:%S")

        logger.info(f"Normalizing timestamp columns: {timestamp_columns}")
        for col in timestamp_columns:
            if col in data.columns:
                data[col] = pd.to_datetime(data[col], errors='coerce').dt.strftime(output_format)
        return data
