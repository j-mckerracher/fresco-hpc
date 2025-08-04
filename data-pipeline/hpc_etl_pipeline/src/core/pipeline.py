
from pathlib import Path
from typing import Dict

from hpc_etl_pipeline.src.core.config_loader import ConfigLoader
from hpc_etl_pipeline.src.core.exceptions import PipelineError
from hpc_etl_pipeline.src.extractors.base_extractor import BaseExtractor
from hpc_etl_pipeline.src.loaders.base_loader import BaseLoader
from hpc_etl_pipeline.src.transformers.base_transformer import BaseTransformer
from hpc_etl_pipeline.src.utils.logger import get_logger, setup_logging

logger = get_logger(__name__)

class Pipeline:
    """Orchestrates the ETL process."""

    def __init__(self, config_path: str):
        self.config = ConfigLoader().load_dataset_config(config_path)
        setup_logging(log_level=self.config.get("processing", {}).get("log_level", "INFO"))
        self.extractor = self._get_extractor()
        self.transformer = self._get_transformer()
        self.loader = self._get_loader()

    def _get_extractor(self) -> BaseExtractor:
        """Returns an instance of the appropriate extractor based on the config."""
        extractor_type = self.config.get("source", {}).get("type")
        if extractor_type == "remote_http":
            from hpc_etl_pipeline.src.extractors.http_extractor import HttpExtractor
            return HttpExtractor()
        elif extractor_type == "globus":
            from hpc_etl_pipeline.src.extractors.globus_extractor import GlobusExtractor
            return GlobusExtractor()
        else:
            raise PipelineError(f"Unsupported extractor type: {extractor_type}")

    def _get_transformer(self) -> BaseTransformer:
        """Returns an instance of the appropriate transformer based on the config."""
        # This will be expanded to handle multiple transformers
        transformer_type = self.config.get("transformations", [{}])[0].get("type")
        if transformer_type == "standardize_columns":
            from hpc_etl_pipeline.src.transformers.generic_transformers import ColumnReorderTransformer
            return ColumnReorderTransformer()
        else:
            # Returning a default transformer for now
            from hpc_etl_pipeline.src.transformers.generic_transformers import ColumnReorderTransformer
            return ColumnReorderTransformer()

    def _get_loader(self) -> BaseLoader:
        """Returns an instance of the appropriate loader based on the config."""
        loader_type = self.config.get("output", {}).get("format")
        if loader_type == "parquet":
            from hpc_etl_pipeline.src.loaders.parquet_loader import ParquetLoader
            return ParquetLoader()
        else:
            raise PipelineError(f"Unsupported loader type: {loader_type}")

    def run(self):
        """Runs the ETL pipeline."""
        logger.info(f"Starting ETL pipeline for dataset: {self.config.get('dataset', {}).get('name')}")
        try:
            extracted_files = self.extractor.extract(self.config.get("source"))
            for file_path in extracted_files:
                # For now, we'll just log the extracted files.
                # The transformation and loading logic will be added next.
                logger.info(f"Extracted file: {file_path}")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise PipelineError("Pipeline execution failed.") from e
