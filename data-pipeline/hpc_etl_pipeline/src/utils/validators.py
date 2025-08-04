
import pandas as pd
from typing import Dict, List

from hpc_etl_pipeline.src.utils.logger import get_logger

logger = get_logger(__name__)

def validate_dataframe(df: pd.DataFrame, validation_config: Dict) -> bool:
    """
    Validates a DataFrame based on the provided configuration.

    Args:
        df: The DataFrame to validate.
        validation_config: The validation configuration.

    Returns:
        True if the DataFrame is valid, False otherwise.
    """
    required_columns = validation_config.get("required_columns", [])
    if required_columns:
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Validation failed: Missing columns {missing_columns}")
            return False

    min_rows = validation_config.get("min_rows", 1)
    if len(df) < min_rows:
        logger.error(f"Validation failed: DataFrame has {len(df)} rows, expected at least {min_rows}")
        return False

    logger.info("DataFrame validation passed.")
    return True
