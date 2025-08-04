"""Main pipeline orchestrator for HPC ETL processing."""

import gc
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import polars as pl

from .config_loader import ConfigLoader
from .exceptions import HpcEtlException, ConfigurationError
from ..extractors.http_extractor import HttpExtractor
from ..extractors.local_extractor import LocalExtractor
from ..extractors.globus_extractor import GlobusExtractor
from ..transformers.hpc_transformers import BlockIOTransformer, CPUTransformer, MemoryTransformer, NFSTransformer
from ..transformers.generic_transformers import CompositeTransformer
from ..loaders.parquet_loader import ParquetLoader

logger = logging.getLogger(__name__)


class Pipeline:
    """Main pipeline orchestrator for HPC data processing."""
    
    EXTRACTOR_REGISTRY = {
        'remote_http': HttpExtractor,
        'local_fs': LocalExtractor,
        'globus': GlobusExtractor,
    }
    
    HPC_TRANSFORMER_REGISTRY = {
        'block.csv': BlockIOTransformer,
        'cpu.csv': CPUTransformer,
        'mem.csv': MemoryTransformer,
        'llite.csv': NFSTransformer,
    }
    
    def __init__(self, config_path: str):
        """
        Initialize pipeline with configuration.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config_loader = ConfigLoader()
        self.config = self.config_loader.load_dataset_config(config_path)
        
        # Initialize components
        self.extractor = self._create_extractor()
        self.generic_transformer = CompositeTransformer(self.config)
        self.loader = ParquetLoader(self.config)
        
        # Pipeline state
        self.processed_files = 0
        self.failed_files = 0
        
        logger.info(f"Initialized pipeline for dataset: {self.config['dataset']['name']}")
    
    def _create_extractor(self):
        """Create appropriate extractor based on configuration."""
        source_type = self.config['source']['type']
        
        if source_type not in self.EXTRACTOR_REGISTRY:
            raise ConfigurationError(f"Unknown source type: {source_type}")
        
        extractor_class = self.EXTRACTOR_REGISTRY[source_type]
        return extractor_class(self.config)
    
    def _get_hpc_transformer(self, file_path: Path):
        """Get appropriate HPC transformer based on file name."""
        file_name = file_path.name.lower()
        
        for pattern, transformer_class in self.HPC_TRANSFORMER_REGISTRY.items():
            if pattern in file_name:
                return transformer_class(self.config)
        
        logger.warning(f"No specific HPC transformer found for {file_name}, using generic transformer only")
        return None
    
    def process_file(self, file_path: str) -> bool:
        """
        Process a single file.
        
        Args:
            file_path: Path to the file to process
            
        Returns:
            True if processing was successful
        """
        file_path_obj = Path(file_path)
        
        try:
            logger.info(f"Processing file: {file_path_obj}")
            
            # Read the file
            data = self._read_file(file_path_obj)
            if data.is_empty():
                logger.warning(f"No data to process in {file_path_obj}")
                return False
            
            # Apply HPC-specific transformation if applicable
            hpc_transformer = self._get_hpc_transformer(file_path_obj)
            if hpc_transformer:
                logger.info(f"Applying HPC-specific transformation: {hpc_transformer.__class__.__name__}")
                data = hpc_transformer.transform(data)
                
                if data.is_empty():
                    logger.warning(f"No data after HPC transformation for {file_path_obj}")
                    return False
            
            # Apply generic transformations
            logger.info("Applying generic transformations")
            data = self.generic_transformer.transform(data)
            
            if data.is_empty():
                logger.warning(f"No data after generic transformations for {file_path_obj}")
                return False
            
            # Generate output path
            metadata = self._create_metadata(file_path_obj, data)
            output_path = self.loader.generate_output_path(metadata)
            
            # Save the processed data
            logger.info(f"Saving processed data to: {output_path}")
            success = self.loader.load(data, output_path, metadata)
            
            if success:
                self.processed_files += 1
                logger.info(f"Successfully processed {file_path_obj} -> {output_path}")
            else:
                self.failed_files += 1
                logger.error(f"Failed to save processed data for {file_path_obj}")
            
            # Clean up memory
            del data
            gc.collect()
            
            return success
            
        except Exception as e:
            self.failed_files += 1
            logger.error(f"Error processing file {file_path_obj}: {e}")
            return False
    
    def process_folder(self, folder_path: str) -> Dict[str, int]:
        """
        Process all files in a folder.
        
        Args:
            folder_path: Path to the folder to process
            
        Returns:
            Dictionary with processing statistics
        """
        folder_path_obj = Path(folder_path)
        
        if not folder_path_obj.exists() or not folder_path_obj.is_dir():
            raise HpcEtlException(f"Folder does not exist or is not a directory: {folder_path}")
        
        logger.info(f"Processing folder: {folder_path_obj}")
        
        # Get file patterns from configuration
        file_patterns = self.config['source'].get('file_patterns', ['*'])
        processed_count = 0
        
        for pattern in file_patterns:
            for file_path in folder_path_obj.glob(pattern):
                if file_path.is_file():
                    if self.process_file(str(file_path)):
                        processed_count += 1
        
        return {
            'processed': processed_count,
            'failed': self.failed_files,
            'total': self.processed_files + self.failed_files
        }
    
    def run(self, source: Optional[str] = None) -> Dict[str, int]:
        """
        Run the complete pipeline.
        
        Args:
            source: Optional specific source to process
            
        Returns:
            Dictionary with processing statistics
        """
        try:
            logger.info("Starting pipeline execution")
            
            # Validate extractor
            if not self.extractor.validate_source():
                raise HpcEtlException("Source validation failed")
            
            # Extract files
            extracted_files = list(self.extractor.extract(source))
            logger.info(f"Extracted {len(extracted_files)} files")
            
            if not extracted_files:
                logger.warning("No files extracted from source")
                return {'processed': 0, 'failed': 0, 'total': 0}
            
            # Process each extracted file
            for file_path in extracted_files:
                self.process_file(str(file_path))
            
            # Clean up extractor resources
            self.extractor.cleanup()
            
            stats = {
                'processed': self.processed_files,
                'failed': self.failed_files,
                'total': self.processed_files + self.failed_files
            }
            
            logger.info(f"Pipeline completed. Stats: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise HpcEtlException(f"Pipeline execution failed: {e}")
    
    def _read_file(self, file_path: Path) -> pl.DataFrame:
        """Read file into DataFrame."""
        if file_path.suffix.lower() == '.csv':
            try:
                # Try different encodings
                for encoding in ['utf-8', 'latin-1', 'cp1252']:
                    try:
                        return pl.read_csv(file_path, encoding=encoding, ignore_errors=True)
                    except Exception:
                        continue
                
                # If all encodings fail, try with error handling
                logger.warning(f"Using fallback encoding for {file_path}")
                return pl.read_csv(file_path, ignore_errors=True)
                
            except Exception as e:
                logger.error(f"Failed to read CSV file {file_path}: {e}")
                return pl.DataFrame()
        
        elif file_path.suffix.lower() == '.parquet':
            try:
                return pl.read_parquet(file_path)
            except Exception as e:
                logger.error(f"Failed to read Parquet file {file_path}: {e}")
                return pl.DataFrame()
        
        else:
            logger.error(f"Unsupported file format: {file_path}")
            return pl.DataFrame()
    
    def _create_metadata(self, file_path: Path, data: pl.DataFrame) -> Dict[str, Any]:
        """Create metadata for output path generation."""
        dataset_name = self.config['dataset']['name']
        version = self.config['dataset']['version']
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Extract folder name from file path (useful for monthly/daily processing)
        folder_name = file_path.parent.name
        if folder_name == '.':
            folder_name = 'data'
        
        return {
            'dataset_name': dataset_name,
            'version': version,
            'timestamp': timestamp,
            'folder_name': folder_name,
            'file_name': file_path.stem,
            'row_count': len(data)
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get pipeline processing statistics."""
        return {
            'processed_files': self.processed_files,
            'failed_files': self.failed_files,
            'total_files': self.processed_files + self.failed_files,
            'success_rate': (self.processed_files / max(1, self.processed_files + self.failed_files)) * 100
        }