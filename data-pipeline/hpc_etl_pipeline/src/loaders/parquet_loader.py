"""Parquet file loader with chunking support."""

import gc
from pathlib import Path
from typing import Dict, Any, Optional, List
import polars as pl
import logging

from .base_loader import BaseLoader
from ..core.exceptions import LoadError

logger = logging.getLogger(__name__)


class ParquetLoader(BaseLoader):
    """Loads data to Parquet format with optional chunking."""
    
    def load(self, data: pl.DataFrame, output_path: Path, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Load data to Parquet file with optional chunking.
        
        Args:
            data: DataFrame to save
            output_path: Path where data should be saved
            metadata: Optional metadata about the data
            
        Returns:
            True if load was successful
        """
        try:
            if data.is_empty():
                logger.warning("Cannot save empty DataFrame")
                return False
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if self.should_chunk():
                return self._save_with_chunking(data, output_path, metadata)
            else:
                return self._save_single_file(data, output_path)
                
        except Exception as e:
            raise LoadError(f"Failed to save to {output_path}: {e}")
    
    def _save_single_file(self, data: pl.DataFrame, output_path: Path) -> bool:
        """Save data as a single Parquet file."""
        try:
            compression = self.get_compression()
            data.write_parquet(output_path, compression=compression)
            
            # Validate the saved file
            if self.validate_output(output_path):
                logger.info(f"Successfully saved {len(data)} rows to {output_path}")
                return True
            else:
                logger.error(f"Validation failed for {output_path}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to save single file {output_path}: {e}")
            return False
    
    def _save_with_chunking(self, data: pl.DataFrame, output_path: Path, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Save data with chunking based on size limits."""
        try:
            max_size_bytes = self.get_max_chunk_size() * (1024**3)  # Convert GB to bytes
            min_rows_per_chunk = self.get_min_rows_per_chunk()
            
            # Estimate memory usage per row (rough approximation)
            sample_size = min(1000, len(data))
            sample_memory = data.head(sample_size).estimated_size()
            avg_bytes_per_row = sample_memory / sample_size if sample_size > 0 else 1000
            
            # Calculate chunk size
            max_rows_by_size = int(max_size_bytes / avg_bytes_per_row)
            chunk_size = max(min_rows_per_chunk, min(max_rows_by_size, len(data)))
            
            if chunk_size >= len(data):
                # Data fits in single chunk
                return self._save_single_file(data, output_path)
            
            # Save in chunks
            chunks_saved = self._save_chunks(data, output_path, chunk_size, metadata)
            return chunks_saved > 0
            
        except Exception as e:
            logger.error(f"Failed to save with chunking {output_path}: {e}")
            return False
    
    def _save_chunks(self, data: pl.DataFrame, base_path: Path, chunk_size: int, metadata: Optional[Dict[str, Any]] = None) -> int:
        """Save data in chunks and return number of chunks saved."""
        total_rows = len(data)
        num_chunks = (total_rows + chunk_size - 1) // chunk_size  # Ceiling division
        chunks_saved = 0
        
        # Generate chunk file paths
        base_name = base_path.stem
        extension = base_path.suffix
        parent_dir = base_path.parent
        
        logger.info(f"Splitting {total_rows} rows into {num_chunks} chunks of ~{chunk_size} rows each")
        
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            
            # Create chunk filename
            chunk_filename = f"{base_name}_chunk_{i + 1:03d}_of_{num_chunks:03d}{extension}"
            chunk_path = parent_dir / chunk_filename
            
            try:
                # Extract chunk data
                chunk_data = data.slice(start_idx, end_idx - start_idx)
                
                # Save chunk
                compression = self.get_compression()
                chunk_data.write_parquet(chunk_path, compression=compression)
                
                # Validate chunk
                if self.validate_output(chunk_path):
                    logger.info(f"Saved chunk {i + 1}/{num_chunks}: {len(chunk_data)} rows to {chunk_filename}")
                    chunks_saved += 1
                else:
                    logger.error(f"Validation failed for chunk {chunk_filename}")
                
                # Clean up chunk data from memory
                del chunk_data
                gc.collect()
                
            except Exception as e:
                logger.error(f"Failed to save chunk {i + 1}: {e}")
                continue
        
        logger.info(f"Successfully saved {chunks_saved}/{num_chunks} chunks")
        return chunks_saved
    
    def validate_output(self, output_path: Path) -> bool:
        """Validate that the Parquet file was created successfully."""
        try:
            if not output_path.exists():
                logger.error(f"Output file does not exist: {output_path}")
                return False
            
            file_size = output_path.stat().st_size
            if file_size == 0:
                logger.error(f"Output file is empty: {output_path}")
                return False
            
            # Try to read a sample to verify file integrity
            try:
                sample = pl.read_parquet(output_path, n_rows=10)
                if sample.is_empty():
                    logger.error(f"Output file appears to be empty: {output_path}")
                    return False
                
                # Validate against expected schema if configured
                expected_columns = self._get_expected_columns()
                if expected_columns:
                    missing_columns = set(expected_columns) - set(sample.columns)
                    if missing_columns:
                        logger.error(f"Output file missing expected columns: {missing_columns}")
                        return False
                
                logger.debug(f"Validated output file: {output_path} ({file_size} bytes)")
                return True
                
            except Exception as e:
                logger.error(f"Failed to read output file for validation: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error validating output file {output_path}: {e}")
            return False
    
    def _get_expected_columns(self) -> List[str]:
        """Get expected output columns from configuration."""
        # Standard FRESCO schema
        return ["Job Id", "Host", "Event", "Value", "Units", "Timestamp"]
    
    def get_file_size_gb(self, file_path: Path) -> float:
        """Get file size in GB."""
        try:
            if file_path.exists():
                size_bytes = file_path.stat().st_size
                return size_bytes / (1024**3)
            return 0.0
        except Exception:
            return 0.0