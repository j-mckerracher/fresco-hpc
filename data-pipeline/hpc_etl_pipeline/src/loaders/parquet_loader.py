
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict

from hpc_etl_pipeline.src.loaders.base_loader import BaseLoader
from hpc_etl_pipeline.src.utils.logger import get_logger

logger = get_logger(__name__)

class ParquetLoader(BaseLoader):
    """Loads data into Parquet files."""

    def load(self, data: pd.DataFrame, config: Dict):
        """
        Writes the DataFrame to a Parquet file.

        Args:
            data: The DataFrame to write.
            config: The loader configuration.
        """
        output_path = config.get("path_template")
        if not output_path:
            logger.error("ParquetLoader is missing 'path_template' in config.")
            return

        compression = config.get("compression", "snappy")
        chunking_config = config.get("chunking", {})
        chunking_enabled = chunking_config.get("enabled", False)

        if chunking_enabled:
            self._write_chunked(data, output_path, compression, chunking_config)
        else:
            self._write_single(data, output_path, compression)

    def _write_single(self, data: pd.DataFrame, path: str, compression: str):
        """
        Writes the DataFrame to a single Parquet file.
        """
        try:
            logger.info(f"Writing {len(data)} rows to {path}...")
            table = pa.Table.from_pandas(data)
            pq.write_table(table, path, compression=compression)
            logger.info(f"Successfully wrote to {path}")
        except Exception as e:
            logger.error(f"Error writing to {path}: {e}")

    def _write_chunked(self, data: pd.DataFrame, path_template: str, compression: str, chunking_config: Dict):
        """
        Writes the DataFrame to multiple chunked Parquet files.
        """
        max_size_gb = chunking_config.get("max_size_gb", 2.0)
        min_rows_per_chunk = chunking_config.get("min_rows_per_chunk", 500000)

        # Estimate rows per chunk
        estimated_row_size = data.memory_usage(deep=True).sum() / len(data)
        rows_per_chunk = int((max_size_gb * 1024**3) / estimated_row_size)
        rows_per_chunk = max(rows_per_chunk, min_rows_per_chunk)

        num_chunks = (len(data) - 1) // rows_per_chunk + 1
        logger.info(f"Writing {len(data)} rows into {num_chunks} chunks...")

        for i in range(num_chunks):
            chunk = data.iloc[i * rows_per_chunk : (i + 1) * rows_per_chunk]
            chunk_path = path_template.format(chunk_num=i)
            self._write_single(chunk, chunk_path, compression)
