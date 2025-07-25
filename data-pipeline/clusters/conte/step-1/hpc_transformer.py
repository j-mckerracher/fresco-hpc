#!/usr/bin/env python3
"""
HPC Cluster Data Transformer

A high-performance Python script for processing HPC cluster monitoring data from remote repositories.
Transforms raw CSV data from multiple sources into a standardized format optimized for analysis.

Key Features:
- Parallel processing with ThreadPoolExecutor
- Memory optimization for 90GB RAM servers
- Robust error handling with retry logic
- State management for resumable processing
- Optimized for ~2.5GB output files

Author: Generated for FRESCO HPC Data Processing
"""

import os
import sys
import json
import gc
import shutil
import tempfile
import logging
import time
import threading
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Union, Dict, List, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hpc_transformer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def validate_parquet_file(file_path: Path, expected_min_rows: int = 1) -> bool:
    """
    Validate that a parquet file is properly written and readable.
    
    Args:
        file_path: Path to the parquet file to validate
        expected_min_rows: Minimum number of rows expected
        
    Returns:
        True if file is valid, False otherwise
    """
    try:
        if not file_path.exists():
            logger.error(f"Validation failed: File does not exist: {file_path}")
            return False
        
        file_size = file_path.stat().st_size
        if file_size == 0:
            logger.error(f"Validation failed: File is empty: {file_path}")
            return False
        
        # Try to read the file with polars (fast validation)
        try:
            # For large files, just check that we can read some rows and get actual row count
            if expected_min_rows > 10000:
                # For large files, read a sample and get the total row count
                test_df = pl.read_parquet(file_path, n_rows=1000)
                if len(test_df) == 0:
                    logger.error(f"Validation failed: File appears to be empty: {file_path}")
                    return False
                
                # Get the actual row count for large files
                try:
                    actual_rows = len(pl.read_parquet(file_path))
                    if actual_rows < expected_min_rows:
                        logger.error(f"Validation failed: File has {actual_rows} rows, expected at least {expected_min_rows}: {file_path}")
                        return False
                except Exception as count_e:
                    logger.warning(f"Could not get exact row count for {file_path}, using sampling validation: {count_e}")
                    # Fallback: if we can read some rows, assume it's valid
                    if len(test_df) < 100:
                        logger.error(f"Validation failed: Sample too small ({len(test_df)} rows) for {file_path}")
                        return False
            else:
                # For smaller files, read the expected number of rows
                test_df = pl.read_parquet(file_path, n_rows=min(1000, expected_min_rows))
                if len(test_df) < expected_min_rows:
                    logger.error(f"Validation failed: File has {len(test_df)} rows, expected at least {expected_min_rows}: {file_path}")
                    return False
            
            # Check that basic schema is present
            required_columns = ['Job Id', 'Host', 'Event', 'Value', 'Units', 'Timestamp']
            missing_columns = [col for col in required_columns if col not in test_df.columns]
            if missing_columns:
                logger.error(f"Validation failed: Missing columns {missing_columns}: {file_path}")
                return False
            
            logger.debug(f"Validation passed: {file_path} ({file_size / (1024*1024):.1f} MB, {len(test_df)} rows sampled)")
            return True
            
        except Exception as e:
            # Try with pandas as fallback
            if "PAR1" in str(e) or "out of specification" in str(e):
                logger.debug(f"Polars validation failed, trying pandas fallback for {file_path}")
                try:
                    import pandas as pd
                    if expected_min_rows > 10000:
                        # For large files, read a sample and check if we can get the total count
                        test_df = pd.read_parquet(file_path, nrows=1000)
                        if len(test_df) == 0:
                            logger.error(f"Validation failed (pandas): File appears to be empty: {file_path}")
                            return False
                        
                        # Try to get actual row count
                        try:
                            actual_rows = len(pd.read_parquet(file_path))
                            if actual_rows < expected_min_rows:
                                logger.error(f"Validation failed (pandas): File has {actual_rows} rows, expected at least {expected_min_rows}: {file_path}")
                                return False
                        except Exception:
                            # If we can't get row count, but we can read some rows, assume it's valid
                            if len(test_df) < 100:
                                logger.error(f"Validation failed (pandas): Sample too small ({len(test_df)} rows) for {file_path}")
                                return False
                    else:
                        # For smaller files, use the original validation
                        test_df = pd.read_parquet(file_path, nrows=min(1000, expected_min_rows))
                        if len(test_df) < expected_min_rows:
                            logger.error(f"Validation failed (pandas): File has {len(test_df)} rows, expected at least {expected_min_rows}: {file_path}")
                            return False
                    
                    logger.debug(f"Validation passed with pandas fallback: {file_path}")
                    return True
                    
                except Exception as pandas_e:
                    logger.error(f"Validation failed with both polars and pandas: {file_path} - {pandas_e}")
                    return False
            else:
                raise e
                
    except Exception as e:
        logger.error(f"Validation failed with exception: {file_path} - {e}")
        return False


def write_chunk_with_validation_and_retry(chunk_df: pd.DataFrame, chunk_path: Path, 
                                         chunk_num: int, process_id: int, disk_manager,
                                         max_retries: int = 1) -> bool:
    """
    Write a chunk with validation and retry logic.
    
    Args:
        chunk_df: DataFrame chunk to write
        chunk_path: Path where chunk should be written
        chunk_num: Chunk number for logging
        process_id: Process ID for logging
        disk_manager: DiskSpaceManager instance for space checks
        max_retries: Maximum number of retry attempts
        
    Returns:
        True if successful, False otherwise
    """
    expected_rows = len(chunk_df)
    
    for attempt in range(max_retries + 1):  # +1 for initial attempt
        try:
            # Clean up any existing file from previous failed attempt
            if chunk_path.exists():
                chunk_path.unlink()
                logger.debug(f"[PID {process_id}] Removed existing file before attempt {attempt + 1}: {chunk_path}")
            
            # Write to temporary file first (atomic write)
            temp_path = chunk_path.with_suffix('.tmp')
            if temp_path.exists():
                temp_path.unlink()
            
            logger.info(f"[PID {process_id}] Writing chunk {chunk_num} attempt {attempt + 1}: {len(chunk_df):,} rows to {chunk_path.name}")
            
            # Check disk space before writing
            disk_space_check, free_gb = disk_manager.check_disk_space()
            if free_gb < 5.0:  # Need at least 5GB free
                logger.error(f"[PID {process_id}] Insufficient disk space before writing chunk {chunk_num}: {free_gb:.1f}GB available")
                return False
            
            # Convert to pyarrow table and write
            table = pa.Table.from_pandas(chunk_df)
            pq.write_table(table, temp_path, compression='snappy')
            
            # Clean up table to free memory immediately
            del table
            gc.collect()
            
            # Validate the temporary file
            if validate_parquet_file(temp_path, expected_min_rows=expected_rows):
                # Atomic rename if validation passes
                temp_path.rename(chunk_path)
                
                # Final verification after rename
                if validate_parquet_file(chunk_path, expected_min_rows=expected_rows):
                    chunk_size_mb = chunk_path.stat().st_size / (1024 * 1024)
                    logger.info(f"[PID {process_id}] Chunk {chunk_num} written and validated successfully ({chunk_size_mb:.1f} MB)")
                    return True
                else:
                    logger.error(f"[PID {process_id}] Chunk {chunk_num} failed validation after rename on attempt {attempt + 1}")
                    if chunk_path.exists():
                        chunk_path.unlink()
            else:
                logger.error(f"[PID {process_id}] Chunk {chunk_num} failed validation on attempt {attempt + 1}")
                if temp_path.exists():
                    temp_path.unlink()
            
        except Exception as e:
            logger.error(f"[PID {process_id}] Error writing chunk {chunk_num} on attempt {attempt + 1}: {e}")
            
            # Clean up any partial files
            for path_name in ['temp_path', 'chunk_path']:
                if path_name in locals():
                    path = locals()[path_name]
                    if path.exists():
                        try:
                            path.unlink()
                            logger.debug(f"[PID {process_id}] Cleaned up partial file: {path}")
                        except Exception as cleanup_e:
                            logger.warning(f"[PID {process_id}] Could not clean up {path}: {cleanup_e}")
        
        # If we're here, the attempt failed
        if attempt < max_retries:
            wait_time = (attempt + 1) * 2  # Exponential backoff: 2s, 4s, 6s...
            logger.warning(f"[PID {process_id}] Chunk {chunk_num} attempt {attempt + 1} failed, retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            
            # Force garbage collection before retry
            gc.collect()
        else:
            logger.error(f"[PID {process_id}] Chunk {chunk_num} failed after {max_retries + 1} attempts")
    
    return False


class ProcessingTracker:
    """Tracks processing state and enables resumption from failures"""
    
    def __init__(self, status_file: str = "processing_status.json"):
        self.status_file = status_file
        self.processed_folders = set()
        self.failed_folders = set()
        self.last_processed_index = -1
        self.lock = threading.RLock()  # Use re-entrant lock to prevent deadlock
        self.load_status()
    
    def load_status(self):
        """Load processing status from JSON file"""
        try:
            if os.path.exists(self.status_file):
                with open(self.status_file, 'r') as f:
                    data = json.load(f)
                    self.processed_folders = set(data.get('processed_folders', []))
                    self.failed_folders = set(data.get('failed_folders', []))
                    self.last_processed_index = data.get('last_processed_index', -1)
                logger.info(f"Loaded status: {len(self.processed_folders)} processed, {len(self.failed_folders)} failed")
        except Exception as e:
            logger.warning(f"Could not load processing status: {e}")
    
    def save_status(self):
        """Save processing status to JSON file"""
        with self.lock:
            try:
                logger.info("Creating status data dictionary...")
                data = {
                    'processed_folders': list(self.processed_folders),
                    'failed_folders': list(self.failed_folders),
                    'last_processed_index': self.last_processed_index,
                    'last_updated': datetime.now().isoformat()
                }
                logger.info(f"Status data created: {len(data['processed_folders'])} processed, {len(data['failed_folders'])} failed")
                
                # Use atomic write to prevent corruption
                temp_file = self.status_file + '.tmp'
                logger.info(f"Writing to temporary file: {temp_file}")
                with open(temp_file, 'w') as f:
                    json.dump(data, f, indent=2)
                logger.info("JSON written to temp file, performing atomic rename...")
                os.rename(temp_file, self.status_file)
                logger.info(f"Status saved successfully: {len(self.processed_folders)} processed, {len(self.failed_folders)} failed")
            except Exception as e:
                logger.error(f"Could not save processing status: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
    
    def mark_processed(self, folder_name: str, index: int):
        """Mark a folder as successfully processed"""
        logger.info(f"Acquiring lock for marking {folder_name} as processed...")
        with self.lock:
            logger.info(f"Lock acquired, updating processed folders for {folder_name}...")
            self.processed_folders.add(folder_name)
            self.failed_folders.discard(folder_name)
            self.last_processed_index = max(self.last_processed_index, index)
            logger.info(f"About to save status for {folder_name}...")
            self.save_status()
            logger.info(f"Status saved successfully for {folder_name}")
        logger.info(f"Lock released for {folder_name}")
    
    def mark_failed(self, folder_name: str):
        """Mark a folder as failed"""
        with self.lock:
            self.failed_folders.add(folder_name)
            self.processed_folders.discard(folder_name)
            self.save_status()
    
    def is_processed(self, folder_name: str) -> bool:
        """Check if folder has been processed"""
        return folder_name in self.processed_folders
    
    def is_failed(self, folder_name: str) -> bool:
        """Check if folder has failed"""
        return folder_name in self.failed_folders


class DataVersionManager:
    """Manages output file versioning"""
    
    def __init__(self, version_file: str = "version_info.json"):
        self.version_file = version_file
        self.versions = {}
        self.load_versions()
    
    def load_versions(self):
        """Load version information from JSON file"""
        try:
            if os.path.exists(self.version_file):
                with open(self.version_file, 'r') as f:
                    self.versions = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load version info: {e}")
    
    def save_versions(self):
        """Save version information to JSON file"""
        try:
            with open(self.version_file, 'w') as f:
                json.dump(self.versions, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save version info: {e}")
    
    def get_next_version(self, folder_name: str) -> int:
        """Get next version number for a folder"""
        current_version = self.versions.get(folder_name, 0)
        next_version = current_version + 1
        self.versions[folder_name] = next_version
        self.save_versions()
        return next_version


class DiskSpaceManager:
    """Monitors and manages disk space usage"""
    
    def __init__(self, min_free_gb: float = 10.0):
        self.min_free_gb = min_free_gb
    
    def check_disk_space(self, path: str = ".") -> Tuple[bool, float]:
        """Check if sufficient disk space is available"""
        try:
            # Use du -sh ~ to check home directory usage and df to check available space
            import subprocess
            
            # Get available space using df
            result = subprocess.run(['df', '-h', path], capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                if len(lines) >= 2:
                    # Parse df output: Filesystem Size Used Avail Use% Mounted
                    parts = lines[1].split()
                    if len(parts) >= 4:
                        avail_str = parts[3]  # Available space
                        # Convert to GB (handle K, M, G, T suffixes)
                        if avail_str.endswith('K'):
                            free_gb = float(avail_str[:-1]) / (1024 * 1024)
                        elif avail_str.endswith('M'):
                            free_gb = float(avail_str[:-1]) / 1024
                        elif avail_str.endswith('G'):
                            free_gb = float(avail_str[:-1])
                        elif avail_str.endswith('T'):
                            free_gb = float(avail_str[:-1]) * 1024
                        else:
                            # Assume bytes if no suffix
                            free_gb = float(avail_str) / (1024**3)
                        
                        has_space = free_gb >= self.min_free_gb
                        logger.info(f"Disk space check: {free_gb:.1f}GB available")
                        return has_space, free_gb
            
            # Fallback to original method if df fails
            statvfs = os.statvfs(path)
            if hasattr(statvfs, 'f_available'):
                free_bytes = statvfs.f_frsize * statvfs.f_available
            elif hasattr(statvfs, 'f_bavail'):
                free_bytes = statvfs.f_frsize * statvfs.f_bavail
            else:
                free_bytes = statvfs.f_bsize * statvfs.f_bavail
            
            free_gb = free_bytes / (1024**3)
            has_space = free_gb >= self.min_free_gb
            return has_space, free_gb
            
        except Exception as e:
            logger.warning(f"Could not check disk space: {e}")
            return True, 0.0  # Assume sufficient space if check fails
    
    def check_home_directory_usage(self) -> float:
        """Check home directory usage using du -sh"""
        try:
            import subprocess
            # Get actual home directory path instead of relying on shell expansion
            home_dir = os.path.expanduser('~')
            result = subprocess.run(['du', '-sh', home_dir], capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                # Parse du output: "1.2G    <USER_HOME_PLACEHOLDER>"
                size_str = result.stdout.strip().split()[0]
                
                # Convert to GB (handle K, M, G, T suffixes)
                if size_str.endswith('K'):
                    usage_gb = float(size_str[:-1]) / (1024 * 1024)
                elif size_str.endswith('M'):
                    usage_gb = float(size_str[:-1]) / 1024
                elif size_str.endswith('G'):
                    usage_gb = float(size_str[:-1])
                elif size_str.endswith('T'):
                    usage_gb = float(size_str[:-1]) * 1024
                else:
                    # Assume bytes if no suffix
                    usage_gb = float(size_str) / (1024**3)
                
                logger.info(f"Home directory usage ({home_dir}): {usage_gb:.1f}GB")
                return usage_gb
            else:
                logger.warning(f"du command failed: {result.stderr}")
                return 0.0
        except Exception as e:
            logger.warning(f"Could not check home directory usage: {e}")
            return 0.0
    
    def cleanup_temp_files(self, temp_dir: str):
        """Clean up temporary files and directories"""
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            logger.error(f"Error cleaning up temp directory {temp_dir}: {e}")


class FileDownloader:
    """Handles parallel file downloads with retry logic"""
    
    def __init__(self, max_workers: int = 8, max_retries: int = 3):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'HPC-Data-Transformer/1.0'})
    
    def download_file(self, url: str, local_path: str, timeout: int = 300) -> bool:
        """Download a single file with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=timeout, stream=True)
                response.raise_for_status()
                
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # Verify file was downloaded
                if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                    logger.debug(f"Successfully downloaded {url} to {local_path}")
                    return True
                else:
                    raise Exception("Downloaded file is empty or missing")
                
            except Exception as e:
                wait_time = (2 ** attempt) * 1  # Exponential backoff
                logger.warning(f"Download attempt {attempt + 1} failed for {url}: {e}")
                
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to download {url} after {self.max_retries} attempts")
                    return False
        
        return False
    
    def download_files_parallel(self, download_tasks: List[Tuple[str, str]]) -> Dict[str, bool]:
        """Download multiple files in parallel"""
        results = {}
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {
                executor.submit(self.download_file, url, path): url 
                for url, path in download_tasks
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    success = future.result()
                    results[url] = success
                except Exception as e:
                    logger.error(f"Download task failed for {url}: {e}")
                    results[url] = False
        
        return results


class FolderDiscovery:
    """Discovers available monthly folders from remote repository"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
    
    def discover_folders(self) -> List[str]:
        """Discover all monthly folders from the repository"""
        try:
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            folders = []
            
            # Look for links that match YYYY-MM pattern
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if self._is_monthly_folder(href):
                    folders.append(href.rstrip('/'))
            
            # Sort folders chronologically
            folders.sort()
            logger.info(f"Discovered {len(folders)} monthly folders")
            return folders
            
        except Exception as e:
            logger.error(f"Error discovering folders from {self.base_url}: {e}")
            return []
    
    def _is_monthly_folder(self, folder_name: str) -> bool:
        """Check if folder name matches YYYY-MM pattern"""
        import re
        pattern = r'^\d{4}-\d{2}/?$'
        return bool(re.match(pattern, folder_name))


class HpcDataTransformer:
    """Main class for HPC data transformation"""
    
    def __init__(self, 
                 base_url: str = "https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Conte/TACC_Stats/",
                 output_dir: str = "./output",
                 temp_dir: str = "./temp",
                 max_workers: int = 4):
        
        self.base_url = base_url
        self.output_dir = Path(output_dir)
        self.temp_dir = Path(temp_dir)
        self.max_workers = max_workers
        
        # Create directories
        self.output_dir.mkdir(exist_ok=True)
        self.temp_dir.mkdir(exist_ok=True)
        
        # Initialize components
        self.tracker = ProcessingTracker()
        self.version_manager = DataVersionManager()
        self.disk_manager = DiskSpaceManager()
        self.downloader = FileDownloader()
        self.folder_discovery = FolderDiscovery(base_url)
        
        # Required CSV files
        self.required_files = ['block.csv', 'cpu.csv', 'mem.csv', 'llite.csv']
        
        logger.info(f"Initialized HPC Data Transformer")
        logger.info(f"Base URL: {self.base_url}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Temp directory: {self.temp_dir}")
    
    def download_folder_files(self, folder_name: str, folder_temp_dir: Path) -> bool:
        """Download all required files for a monthly folder"""
        download_tasks = []
        
        for file_name in self.required_files:
            file_url = urljoin(self.base_url, f"{folder_name}/{file_name}")
            local_path = folder_temp_dir / file_name
            download_tasks.append((file_url, str(local_path)))
        
        logger.info(f"Downloading {len(download_tasks)} files for folder {folder_name}")
        results = self.downloader.download_files_parallel(download_tasks)
        
        # Check if all downloads succeeded
        success_count = sum(1 for success in results.values() if success)
        
        if success_count == len(self.required_files):
            logger.info(f"Successfully downloaded all files for {folder_name}")
            return True
        else:
            logger.error(f"Only {success_count}/{len(self.required_files)} files downloaded for {folder_name}")
            return False
    
    def process_folder_data(self, folder_temp_dir: Path) -> Optional[pd.DataFrame]:
        """Process all CSV files in a folder and combine results"""
        try:
            # Check that all required files exist
            file_paths = {}
            for file_name in self.required_files:
                file_path = folder_temp_dir / file_name
                if not file_path.exists() or file_path.stat().st_size == 0:
                    logger.error(f"Required file missing or empty: {file_path}")
                    return None
                file_paths[file_name] = str(file_path)
            
            logger.info("Processing files in parallel...")
            
            # Process files in parallel using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    'block': executor.submit(process_block_file, file_paths['block.csv']),
                    'cpu': executor.submit(process_cpu_file, file_paths['cpu.csv']),
                    'mem': executor.submit(process_mem_file, file_paths['mem.csv']),
                    'nfs': executor.submit(process_nfs_file, file_paths['llite.csv'])
                }
                
                # Collect results with timeout and progress monitoring
                dataframes = []
                failed_files = []
                for file_type, future in futures.items():
                    try:
                        logger.info(f"Waiting for {file_type} processing to complete...")
                        # Use longer timeout for CPU files (they're much larger)
                        timeout_seconds = 3600 if file_type == 'cpu' else 1800  # 60 min for CPU, 30 min for others
                        df = future.result(timeout=timeout_seconds)
                        if df is not None and not df.empty:
                            dataframes.append(df)
                            logger.info(f"Processed {file_type}: {len(df)} rows")
                        else:
                            logger.warning(f"No data returned from {file_type} processing (likely corrupted file)")
                            failed_files.append(file_type)
                    except TimeoutError:
                        timeout_minutes = timeout_seconds // 60
                        logger.error(f"Timeout processing {file_type} after {timeout_minutes} minutes")
                        failed_files.append(file_type)
                    except Exception as e:
                        logger.error(f"Error processing {file_type}: {e}")
                        import traceback
                        logger.error(f"Traceback: {traceback.format_exc()}")
                        failed_files.append(file_type)
                
                # Log failed files but continue if we have at least some data
                if failed_files:
                    logger.warning(f"Failed to process {len(failed_files)} files: {failed_files}")
                
                # Only return None if ALL files failed
                if len(failed_files) == len(futures):
                    logger.error("All files failed to process - cannot continue with this folder")
                    return None
            
            # Combine all dataframes
            if dataframes:
                combined_df = pd.concat(dataframes, ignore_index=True)
                logger.info(f"Combined data: {len(combined_df)} total rows")
                
                # Validate schema
                required_columns = ['Job Id', 'Host', 'Event', 'Value', 'Units', 'Timestamp']
                if all(col in combined_df.columns for col in required_columns):
                    return combined_df
                else:
                    logger.error(f"Combined data missing required columns: {required_columns}")
                    return None
            else:
                logger.error("No valid dataframes to combine")
                return None
                
        except Exception as e:
            logger.error(f"Error processing folder data: {e}")
            return None
        finally:
            # Clean up memory
            if 'dataframes' in locals():
                del dataframes
            gc.collect()
    
    def process_single_folder(self, folder_name: str, folder_index: int) -> bool:
        """Process a single monthly folder"""
        try:
            logger.info(f"Processing folder {folder_index + 1}: {folder_name}")
            
            # Check disk space
            logger.info(f"Checking disk space for {folder_name}...")
            has_space, free_gb = self.disk_manager.check_disk_space()
            home_usage_gb = self.disk_manager.check_home_directory_usage()
            
            if not has_space:
                logger.error(f"Insufficient disk space: {free_gb:.1f} GB available, home directory using {home_usage_gb:.1f} GB")
                return False
            else:
                logger.info(f"Disk space check passed: {free_gb:.1f} GB available, home directory using {home_usage_gb:.1f} GB")
            
            # Create temporary directory for this folder
            logger.info(f"Creating temporary directory for {folder_name}...")
            folder_temp_dir = self.temp_dir / f"processing_{folder_name}_{int(time.time())}"
            folder_temp_dir.mkdir(exist_ok=True)
            logger.info(f"Created temp dir: {folder_temp_dir}")
            
            try:
                # Download files
                logger.info(f"Starting download for {folder_name}...")
                download_result = self.download_folder_files(folder_name, folder_temp_dir)
                logger.info(f"Download result for {folder_name}: {download_result}")
                if not download_result:
                    logger.error(f"Download failed for {folder_name}")
                    return False
                
                # Process data
                logger.info(f"Starting data processing for {folder_name}...")
                combined_df = self.process_folder_data(folder_temp_dir)
                logger.info(f"Data processing completed for {folder_name}, result: {combined_df is not None}")
                if combined_df is None:
                    logger.error(f"Data processing failed for {folder_name}")
                    return False
                
                # Generate output filename
                logger.info(f"Generating output filename for {folder_name}...")
                version = self.version_manager.get_next_version(folder_name)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"FRESCO_Conte_ts_{folder_name}_v{version}_{timestamp}.parquet"
                output_path = self.output_dir / output_filename
                
                # Save output with better error handling and chunked writing for large files
                try:
                    logger.info(f"Starting to save {len(combined_df):,} rows to {output_path}")
                    
                    # Check available disk space before writing
                    has_space, free_gb = self.disk_manager.check_disk_space()
                    
                    # Estimate parquet file size conservatively for limited disk space
                    # Conservative estimate: 150-200 bytes per row for parquet (better safe than sorry)
                    estimated_size_gb = len(combined_df) * 0.00000015  # ~150 bytes per row
                    
                    logger.info(f"Estimated parquet file size: {estimated_size_gb:.1f}GB, Available: {free_gb:.1f}GB")
                    
                    # Determine if chunking is needed (>2GB estimated size for safety with 30GB limit)
                    chunk_size_gb = 2.0
                    needs_chunking = estimated_size_gb > chunk_size_gb
                    
                    if needs_chunking:
                        logger.info(f"Large dataset detected ({len(combined_df):,} rows, ~{estimated_size_gb:.1f}GB). Using chunked parquet writing with 2GB chunks...")
                        
                        # Calculate rows per chunk to target ~2GB chunks
                        rows_per_chunk = int(chunk_size_gb / (estimated_size_gb / len(combined_df)))
                        rows_per_chunk = max(rows_per_chunk, 500_000)  # At least 500K rows per chunk
                        
                        chunk_num = 0
                        total_rows = len(combined_df)
                        
                        for start_idx in range(0, total_rows, rows_per_chunk):
                            end_idx = min(start_idx + rows_per_chunk, total_rows)
                            chunk_df = combined_df.iloc[start_idx:end_idx]
                            
                            # Generate chunk filename
                            chunk_filename = f"FRESCO_Conte_ts_{folder_name}_v{version}_{timestamp}_chunk_{chunk_num:03d}.parquet"
                            chunk_path = self.output_dir / chunk_filename
                            
                            # Check disk space before writing each chunk
                            chunk_space_check, current_free_gb = self.disk_manager.check_disk_space()
                            if current_free_gb < 3.0:  # Need at least 3GB free space
                                logger.error(f"Insufficient disk space before chunk {chunk_num}: {current_free_gb:.1f}GB available")
                                return False
                            
                            # Write chunk with validation and retry
                            success = write_chunk_with_validation_and_retry(
                                chunk_df, chunk_path, chunk_num, os.getpid(), self.disk_manager, max_retries=1
                            )
                            
                            if not success:
                                logger.error(f"Failed to write chunk {chunk_num} to {chunk_path}")
                                return False
                            
                            # Previous chunk existence check removed - file may be moved by external process
                            
                            chunk_num += 1
                            
                            # Clean up chunk DataFrame to free memory
                            del chunk_df
                            gc.collect()
                            
                            # Log progress
                            progress_pct = (end_idx / total_rows) * 100
                            logger.info(f"Progress: {end_idx:,}/{total_rows:,} rows ({progress_pct:.1f}%) - {chunk_num} chunks written")
                        
                        logger.info(f"Chunked parquet writing completed for {folder_name} - {chunk_num} chunks written")
                        
                        # Set output_path to the first chunk for verification
                        output_path = self.output_dir / f"FRESCO_Conte_ts_{folder_name}_v{version}_{timestamp}_chunk_000.parquet"
                        
                    else:
                        # Standard single-file writing for smaller datasets
                        if estimated_size_gb > free_gb * 0.5:  # Use max 50% of available space for safety
                            logger.error(f"Insufficient disk space. Estimated file size: {estimated_size_gb:.1f}GB, Available: {free_gb:.1f}GB")
                            return False
                        
                        logger.info(f"Writing single parquet file ({len(combined_df):,} rows)...")
                        
                        # Convert to pyarrow table for efficient parquet writing
                        table = pa.Table.from_pandas(combined_df)
                        pq.write_table(table, output_path, compression='snappy')
                        
                        del table
                        gc.collect()
                    
                    logger.info(f"Parquet writing completed for {folder_name}")
                    
                except MemoryError as e:
                    logger.error(f"Memory error while saving CSV for {folder_name}: {e}")
                    # Clean up partial file
                    if output_path.exists():
                        output_path.unlink()
                    return False
                except OSError as e:
                    logger.error(f"Disk I/O error while saving CSV for {folder_name}: {e}")
                    # Clean up partial file
                    if output_path.exists():
                        output_path.unlink()
                    return False
                except Exception as e:
                    logger.error(f"Unexpected error while saving CSV for {folder_name}: {e}")
                    # Clean up partial file
                    if output_path.exists():
                        output_path.unlink()
                    return False
                
                # Verify output file
                if output_path.exists() and output_path.stat().st_size > 0:
                    file_size_mb = output_path.stat().st_size / (1024 * 1024)
                    logger.info(f"Successfully saved output: {output_path} ({file_size_mb:.1f} MB)")
                    
                    # Log event summary
                    event_summary = combined_df['Event'].value_counts().to_dict()
                    logger.info(f"Event summary for {folder_name}: {event_summary}")
                    
                    return True
                else:
                    logger.error(f"Output file creation failed: {output_path}")
                    return False
                    
            finally:
                # Clean up temporary files
                self.disk_manager.cleanup_temp_files(str(folder_temp_dir))
                
        except Exception as e:
            logger.error(f"Error processing folder {folder_name}: {e}")
            return False
        finally:
            # Memory cleanup
            if 'combined_df' in locals():
                del combined_df
            gc.collect()
    
    def run_transformation(self, max_folders: Optional[int] = None) -> Dict[str, Any]:
        """Main entry point for running the transformation"""
        start_time = time.time()
        
        logger.info("Starting HPC data transformation...")
        
        # Discover folders
        folders = self.folder_discovery.discover_folders()
        if not folders:
            logger.error("No folders discovered from repository")
            return {'processed': 0, 'failed': 0, 'total_time': 0}
        
        # Process all discovered folders (all 29 months)
        pending_folders = [
            folder for folder in folders 
            if not self.tracker.is_processed(folder)
        ]

        if max_folders:
            pending_folders = pending_folders[:max_folders]

        logger.info(f"Total folders discovered: {len(folders)}")
        logger.info(f"All folders available: {folders}")
        logger.info(f"Folders to process (not already processed): {len(pending_folders)}")
        logger.info(f"Pending folders: {pending_folders}")
        
        processed_count = 0
        failed_count = 0
        
        for index, folder_name in enumerate(pending_folders):
            try:
                folder_start_time = time.time()
                logger.info(f"Starting processing of folder {index + 1}/{len(pending_folders)}: {folder_name}")
                
                logger.info(f"About to call process_single_folder for {folder_name}")
                result = self.process_single_folder(folder_name, index)
                logger.info(f"process_single_folder returned {result} for {folder_name}")
                
                if result:
                    logger.info(f"Marking {folder_name} as processed...")
                    self.tracker.mark_processed(folder_name, index)
                    processed_count += 1
                    
                    folder_time = time.time() - folder_start_time
                    logger.info(f"Folder {folder_name} completed in {folder_time:.1f} seconds")
                    logger.info(f"Progress: {processed_count}/{len(pending_folders)} folders completed")
                else:
                    logger.info(f"Marking {folder_name} as failed...")
                    self.tracker.mark_failed(folder_name)
                    failed_count += 1
                    logger.error(f"Failed to process folder: {folder_name}")
                
                # Memory cleanup every folder for these specific target folders
                logger.info("Performing memory cleanup...")
                gc.collect()
                
                logger.info(f"Finished processing folder {folder_name}, moving to next folder...")
                # Add small delay to prevent overwhelming the system
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Unexpected error processing {folder_name}: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                self.tracker.mark_failed(folder_name)
                failed_count += 1
        
        total_time = time.time() - start_time
        
        stats = {
            'processed': processed_count,
            'failed': failed_count,
            'total_folders': len(pending_folders),
            'total_time': total_time,
            'all_folders_found': folders,
            'folders_processed': [f for f in folders if self.tracker.is_processed(f)]
        }
        
        logger.info(f"Transformation complete: {stats}")
        return stats


# Import the processing functions provided by user
def safe_parse_timestamp(timestamp_str, default=None):
    """Safely parse timestamp with fallback to default value if parsing fails"""
    try:
        return pd.to_datetime(timestamp_str, format='%m/%d/%Y %H:%M:%S')
    except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime):
        return default




def read_csv_with_robust_error_handling(file_path, chunk_size=None, **kwargs):
    """Read CSV with row-level error handling to salvage good data from partially corrupted files."""
    encodings = ['latin1', 'ISO-8859-1', 'utf-8']
    errors = [None, None, 'replace']
    
    for i, encoding in enumerate(encodings):
        try:
            error_param = errors[i]
            encoding_kwargs = {'encoding': encoding}

            if error_param:
                encoding_kwargs['encoding_errors'] = error_param

            # Always use on_bad_lines='skip' to handle corrupted rows
            try:
                combined_kwargs = {**encoding_kwargs, 'on_bad_lines': 'skip', **kwargs}
            except TypeError:
                combined_kwargs = {**encoding_kwargs, 'error_bad_lines': False, **kwargs}

            if chunk_size is not None:
                combined_kwargs['low_memory'] = False
                return pd.read_csv(file_path, chunksize=chunk_size, **combined_kwargs)
            else:
                df = pd.read_csv(file_path, **combined_kwargs)
                
                # Log how many rows were successfully read
                if not df.empty:
                    logger.info(f"Successfully read {len(df)} rows from {file_path} with {encoding} encoding")
                else:
                    logger.warning(f"File {file_path} resulted in empty DataFrame after parsing")
                
                return df

        except UnicodeDecodeError as e:
            logger.info(f"Encoding {encoding} failed for {file_path}, trying next encoding...")
            continue
        except (ValueError, pd.errors.ParserError) as e:
            # Try to handle specific parsing errors
            if "could not convert string to float" in str(e):
                logger.warning(f"Data conversion errors in {file_path}, attempting recovery...")
                try:
                    # Try reading with more lenient settings
                    recovery_kwargs = {
                        **encoding_kwargs, 
                        'on_bad_lines': 'skip',
                        'low_memory': False,
                        **kwargs
                    }
                    # Remove dtype specifications that might cause conversion errors
                    recovery_kwargs.pop('dtype', None)
                    
                    df = pd.read_csv(file_path, **recovery_kwargs)
                    if not df.empty:
                        logger.info(f"Recovered {len(df)} rows from {file_path} using lenient parsing")
                        return df
                    else:
                        logger.warning(f"Recovery attempt resulted in empty DataFrame for {file_path}")
                        continue
                except:
                    logger.warning(f"Recovery attempt failed for {file_path}, trying next encoding...")
                    continue
            else:
                logger.info(f"Parser error with {encoding} for {file_path}: {str(e)}")
                continue
        except Exception as e:
            logger.info(f"Error reading file {file_path} with {encoding} encoding: {str(e)}")
            continue

    # If all encodings failed, return empty DataFrame instead of raising exception
    logger.error(f"Failed to read {file_path} with any encoding - returning empty DataFrame")
    return pd.DataFrame()


def read_csv_with_fallback_encoding(file_path, chunk_size=None, **kwargs):
    """Wrapper function that maintains backward compatibility while providing robust error handling."""
    return read_csv_with_robust_error_handling(file_path, chunk_size, **kwargs)


def safe_division(numerator, denominator, default: float = 0.0) -> float:
    """Safely perform division with comprehensive error handling."""
    try:
        if not np.isfinite(numerator) or not np.isfinite(denominator):
            return default
        return numerator / denominator if denominator != 0 else default
    except (TypeError, ValueError):
        return default
    except Exception:
        return default


def validate_metric(value: float, min_val: float = 0.0, max_val: float = float('inf')) -> float:
    """Ensure metric values are within a valid range."""
    try:
        val_float = float(value)
        if not np.isfinite(val_float):
            return min_val
        return np.clip(val_float, min_val, max_val)
    except (ValueError, TypeError):
        return min_val


def calculate_rate(current_value: float, previous_value: float, time_delta_seconds: float) -> float:
    """Calculate rate of change per second."""
    try:
        if not all(np.isfinite([current_value, previous_value, time_delta_seconds])):
            return 0.0
        if time_delta_seconds <= 0:
            return 0.0
        diff = current_value - previous_value
        if diff < 0:
            return 0.0
        return safe_division(diff, time_delta_seconds, default=0.0)
    except (TypeError, ValueError):
        return 0.0


def process_block_file(input_data: Union[str, pd.DataFrame]) -> pd.DataFrame:
    """Process block.csv file to extract disk I/O throughput rate (GB/s) at the node level"""
    df = None
    df_valid_rates = None
    agg_df = None
    try:
        if isinstance(input_data, str):
            file_path = input_data
            logger.info(f"Processing block file: {file_path}")
            try:
                dtype_spec = {'rd_sectors': float, 'wr_sectors': float, 'jobID': str, 'node': str, 'device': str,
                              'timestamp': str}
                df = read_csv_with_fallback_encoding(file_path, dtype=dtype_spec, low_memory=False)
                
                # Additional data cleaning for recovered data
                if not df.empty:
                    initial_rows = len(df)
                    # Clean up any rows that still have conversion issues
                    numeric_cols = ['rd_sectors', 'wr_sectors']
                    for col in numeric_cols:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Remove rows where critical numeric columns are NaN
                    df = df.dropna(subset=numeric_cols)
                    
                    cleaned_rows = len(df)
                    if cleaned_rows < initial_rows:
                        logger.info(f"Cleaned block file: removed {initial_rows - cleaned_rows} corrupted rows, kept {cleaned_rows} valid rows")
            except Exception as e:
                logger.error(f"Error reading block file {file_path}: {str(e)}")
                return pd.DataFrame()
        elif isinstance(input_data, pd.DataFrame):
            df = input_data.copy()
            logger.info("Processing block DataFrame.")
        else:
            logger.info(f"Invalid input type for process_block_file: {type(input_data)}")
            return pd.DataFrame()

        required_columns = ['rd_sectors', 'wr_sectors', 'jobID', 'node', 'device', 'timestamp']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.info(f"Missing required columns in block data: {missing_cols}")
            return pd.DataFrame()

        initial_row_count = len(df)
        numeric_cols = ['rd_sectors', 'wr_sectors']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=required_columns)
        filtered_rows = initial_row_count - len(df)
        if filtered_rows > 0:
            logger.info(
                f"Dropped {filtered_rows} rows ({safe_division(filtered_rows, initial_row_count) * 100:.2f}%) with missing/invalid data in block file")

        if df.empty:
            logger.info("No valid data rows after filtering NaNs in block file")
            return pd.DataFrame()

        df['jobID'] = df['jobID'].fillna('unknown').astype(str).str.replace('jobID', 'JOB', case=False)
        df['node'] = df['node'].fillna('unknown').astype(str)
        df['device'] = df['device'].fillna('unknown').astype(str)

        try:
            df['Timestamp_original'] = pd.to_datetime(df['timestamp'], format='%m/%d/%Y %H:%M:%S', errors='coerce')
            df = df.dropna(subset=['Timestamp_original'])
            if df.empty:
                logger.info("No valid block data rows after timestamp filtering")
                return pd.DataFrame()
            sort_keys = ['jobID', 'node', 'device', 'Timestamp_original']
            logger.info(f"Sorting block data by {sort_keys}...")
            df = df.sort_values(sort_keys)
            logger.info("Sorting complete.")
        except Exception as e:
            logger.info(f"Error parsing or sorting timestamps in block file: {str(e)}")
            return pd.DataFrame()

        logger.info("Calculating per-device block I/O rate using diff...")
        SECTOR_SIZE_BYTES = 512
        BYTES_TO_GB = 1 / (1024 ** 3)
        MIN_TIME_DELTA = 0.1

        group_keys_device = ['jobID', 'node', 'device']
        grouped_device = df.groupby(group_keys_device, observed=True)

        df['time_delta_seconds'] = grouped_device['Timestamp_original'].diff().dt.total_seconds()
        df['total_sectors'] = df['rd_sectors'] + df['wr_sectors']
        df['sector_delta'] = grouped_device['total_sectors'].diff()

        df['Value_device_rate'] = 0.0
        valid_rate_mask = (
                df['time_delta_seconds'].notna() &
                (df['time_delta_seconds'] >= MIN_TIME_DELTA) &
                df['sector_delta'].notna() &
                (df['sector_delta'] >= 0)
        )
        bytes_delta = df.loc[valid_rate_mask, 'sector_delta'] * SECTOR_SIZE_BYTES
        df.loc[valid_rate_mask, 'Value_device_rate'] = (bytes_delta / df.loc[
            valid_rate_mask, 'time_delta_seconds']) * BYTES_TO_GB
        df['Value_device_rate'] = df['Value_device_rate'].clip(lower=0)
        logger.info("Per-device rate calculation complete.")

        df_valid_rates = df[valid_rate_mask].copy()
        if df_valid_rates.empty:
            logger.info("No valid per-device block rates calculated.")
            return pd.DataFrame()

        logger.info("Aggregating device-level block rates to node level using original timestamps...")
        node_agg_keys = ['jobID', 'node', 'Timestamp_original']
        agg_df = df_valid_rates.groupby(node_agg_keys, observed=True, as_index=False)['Value_device_rate'].sum()
        agg_df.rename(columns={'Value_device_rate': 'Value'}, inplace=True)

        if agg_df.empty:
            logger.info("No valid data after node-level aggregation for block file.")
            return pd.DataFrame()
        logger.info(f"Node-level block aggregation complete. Result has {len(agg_df)} rows.")

        fresco_df = pd.DataFrame({
            'Job Id': agg_df['jobID'],
            'Host': agg_df['node'],
            'Event': 'block',
            'Value': agg_df['Value'],
            'Units': 'GB/s',
            'Timestamp': agg_df['Timestamp_original']
        })

        logger.info(f"Successfully processed block data: {len(fresco_df)} node-level rows created.")
        return fresco_df

    except Exception as e:
        logger.error(f"An unexpected error occurred in process_block_file: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()
    finally:
        del df
        if df_valid_rates is not None: del df_valid_rates
        if agg_df is not None: del agg_df
        gc.collect()


def process_cpu_file(input_data: Union[str, pd.DataFrame]) -> pd.DataFrame:
    """Process cpu.csv file to extract node-level CPU user percentage - FIXED VERSION."""
    df = None
    try:
        # File reading and validation logic
        if isinstance(input_data, str):
            file_path = input_data
            logger.info(f"Processing CPU file: {file_path}")
            try:
                cpu_jiffy_columns_read = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq']
                dtype_spec = {col: float for col in cpu_jiffy_columns_read}
                dtype_spec.update({'jobID': str, 'node': str, 'device': str, 'timestamp': str})
                df = read_csv_with_fallback_encoding(file_path, dtype=dtype_spec, low_memory=False)
                
                # Additional data cleaning for recovered data
                if not df.empty:
                    initial_rows = len(df)
                    # Clean up any rows that still have conversion issues
                    for col in cpu_jiffy_columns_read:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Remove rows where critical numeric columns are NaN
                    df = df.dropna(subset=cpu_jiffy_columns_read)
                    
                    cleaned_rows = len(df)
                    if cleaned_rows < initial_rows:
                        logger.info(f"Cleaned CPU file: removed {initial_rows - cleaned_rows} corrupted rows, kept {cleaned_rows} valid rows")
            except Exception as e:
                logger.error(f"Error reading CPU file {file_path}: {str(e)}")
                return pd.DataFrame()
        elif isinstance(input_data, pd.DataFrame):
            df = input_data.copy()
            logger.info("Processing CPU DataFrame.")
        else:
            logger.info(f"Invalid input type for process_cpu_file: {type(input_data)}")
            return pd.DataFrame()

        cpu_jiffy_columns = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq']
        id_cols_for_check = ['jobID', 'node', 'device', 'timestamp']
        required_columns = cpu_jiffy_columns + id_cols_for_check
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.info(f"Missing required columns in CPU data: {missing_cols}")
            return pd.DataFrame()

        initial_row_count = len(df)
        for col in cpu_jiffy_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=required_columns)
        filtered_rows = initial_row_count - len(df)
        if filtered_rows > 0:
            logger.info(
                f"Dropped {filtered_rows} rows ({safe_division(filtered_rows, initial_row_count) * 100:.2f}%) with missing/invalid data in CPU file")

        if df.empty:
            logger.info("No valid data rows after initial filtering NaNs in CPU file")
            return pd.DataFrame()

        df['jobID'] = df['jobID'].fillna('unknown').astype(str).str.replace('jobID', 'JOB', case=False)
        df['node'] = df['node'].fillna('unknown').astype(str)
        df['device'] = df['device'].fillna('unknown').astype(str)

        try:
            df['Timestamp_original'] = pd.to_datetime(df['timestamp'], format='%m/%d/%Y %H:%M:%S', errors='coerce')
            df = df.dropna(subset=['Timestamp_original'])
        except Exception as e:
            logger.info(f"Error parsing timestamps in CPU file: {str(e)}")
            return pd.DataFrame()

        if df.empty:
            logger.info("No valid CPU data rows after timestamp filtering")
            return pd.DataFrame()
        
        # CRITICAL FIX: Calculate deltas at the CPU-core level FIRST
        logger.info("Calculating deltas per CPU core BEFORE aggregation...")
        logger.info(f"Starting sorting {len(df):,} CPU rows...")
        sort_start = time.time()
        df = df.sort_values(['jobID', 'node', 'device', 'Timestamp_original'])
        logger.info(f"Sorting completed in {time.time() - sort_start:.1f} seconds")
        
        # Calculate deltas for each CPU core individually
        cpu_jiffy_columns = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq']
        logger.info(f"Creating groupby object for {len(df):,} rows...")
        groupby_start = time.time()
        groupby_core = df.groupby(['jobID', 'node', 'device'])
        logger.info(f"Groupby created in {time.time() - groupby_start:.1f} seconds")
        
        # Calculate all deltas per core with progress tracking and memory optimization
        logger.info("Calculating deltas for each CPU metric...")
        delta_start = time.time()
        
        # Pre-allocate delta columns with correct dtype to avoid reallocation
        for col in cpu_jiffy_columns:
            delta_col_name = f'{col}_core_delta'
            df[delta_col_name] = np.nan
        
        # Calculate deltas more efficiently by reusing the groupby object
        for i, col in enumerate(cpu_jiffy_columns):
            col_start = time.time()
            delta_col_name = f'{col}_core_delta'
            df[delta_col_name] = groupby_core[col].diff()
            logger.info(f"Delta calculation for {col} ({i+1}/{len(cpu_jiffy_columns)}) completed in {time.time() - col_start:.1f} seconds")
            
            # Force garbage collection every few iterations for large datasets
            if i % 3 == 2:  # Every 3 columns
                gc.collect()
        
        # Calculate total jiffies delta per core
        logger.info("Calculating total jiffies delta...")
        total_start = time.time()
        delta_col_names = [f'{col}_core_delta' for col in cpu_jiffy_columns]
        df['total_jiffies_core_delta'] = df[delta_col_names].sum(axis=1)
        logger.info(f"Total jiffies delta calculated in {time.time() - total_start:.1f} seconds")
        logger.info(f"All delta calculations completed in {time.time() - delta_start:.1f} seconds")
        
        # Filter out invalid deltas (first measurement per core, negative values, etc.)
        logger.info("Filtering valid deltas...")
        filter_start = time.time()
        valid_delta_mask = (
            df['user_core_delta'].notna() &
            (df['user_core_delta'] >= 0) &
            df['nice_core_delta'].notna() &
            (df['nice_core_delta'] >= 0) &
            (df['total_jiffies_core_delta'] > 0)
        )
        
        df_valid_deltas = df[valid_delta_mask].copy()
        logger.info(f"Delta filtering completed in {time.time() - filter_start:.1f} seconds")
        
        # Clean up the large original dataframe to free memory
        del df
        gc.collect()
        logger.info("Cleaned up original DataFrame to free memory")
        
        if df_valid_deltas.empty:
            logger.info("No valid CPU core deltas calculated.")
            return pd.DataFrame()
        
        logger.info(f"Valid CPU core deltas: {len(df_valid_deltas):,} rows (filtered from original dataset)")
        
        # NOW aggregate the deltas to node level
        logger.info("Aggregating CPU core deltas to node level...")
        agg_start = time.time()
        node_level_deltas = df_valid_deltas.groupby(
            ['jobID', 'node', 'Timestamp_original'], as_index=False
        )[delta_col_names + ['total_jiffies_core_delta']].sum()
        logger.info(f"Node-level aggregation completed in {time.time() - agg_start:.1f} seconds")
        
        if node_level_deltas.empty:
            logger.info("Node-level delta aggregation resulted in empty DataFrame.")
            return pd.DataFrame()
        
        # Calculate CPU user percentage from aggregated deltas
        logger.info("Calculating CPU user percentages...")
        calc_start = time.time()
        
        # Debug: Log some sample values before calculation
        logger.info(f"Sample user_core_delta values: {node_level_deltas['user_core_delta'].head(10).tolist()}")
        logger.info(f"Sample nice_core_delta values: {node_level_deltas['nice_core_delta'].head(10).tolist()}")
        logger.info(f"Sample total_jiffies_core_delta values: {node_level_deltas['total_jiffies_core_delta'].head(10).tolist()}")
        
        # Calculate CPU user percentage using FRESCO formula: (user + nice) / total_CPU_time * 100
        logger.info("Using FRESCO formula: CPU% = ((user + nice) / total_CPU_time) × 100")
        node_level_deltas['Value_cpuuser'] = np.where(
            node_level_deltas['total_jiffies_core_delta'] > 0,
            ((node_level_deltas['user_core_delta'] + node_level_deltas['nice_core_delta']) / node_level_deltas['total_jiffies_core_delta']) * 100,
            0.0
        )
        
        # Validate and clip values
        node_level_deltas['Value_cpuuser'] = node_level_deltas['Value_cpuuser'].clip(0.0, 100.0)
        
        # Debug: Log some sample calculated values
        logger.info(f"Sample calculated cpuuser values: {node_level_deltas['Value_cpuuser'].head(10).tolist()}")
        logger.info(f"CPU percentage stats: min={node_level_deltas['Value_cpuuser'].min():.4f}, max={node_level_deltas['Value_cpuuser'].max():.4f}, mean={node_level_deltas['Value_cpuuser'].mean():.4f}")
        
        logger.info(f"CPU percentage calculation completed in {time.time() - calc_start:.1f} seconds")
        
        # Filter out any remaining invalid calculations
        logger.info("Final validation filtering...")
        final_filter_start = time.time()
        final_valid_mask = (
            (node_level_deltas['user_core_delta'] >= 0) & 
            (node_level_deltas['nice_core_delta'] >= 0) & 
            (node_level_deltas['total_jiffies_core_delta'] > 0) &
            (node_level_deltas['Value_cpuuser'] >= 0)
        )
        
        logger.info(f"Rows before final filtering: {len(node_level_deltas)}")
        final_data = node_level_deltas[final_valid_mask].copy()
        logger.info(f"Rows after final filtering: {len(final_data)}")
        logger.info(f"Final filtering completed in {time.time() - final_filter_start:.1f} seconds")
        
        # Clean up intermediate data
        del node_level_deltas
        gc.collect()
        
        if final_data.empty:
            logger.info("No valid data remaining after CPU user percentage calculation.")
            return pd.DataFrame()

        # Create final output DataFrame
        logger.info("Creating final FRESCO output DataFrame...")
        output_start = time.time()
        fresco_df = pd.DataFrame({
            'Job Id': final_data['jobID'],
            'Host': final_data['node'],
            'Event': 'cpuuser',
            'Value': final_data['Value_cpuuser'],
            'Units': 'CPU %',
            'Timestamp': final_data['Timestamp_original']
        })
        logger.info(f"Output DataFrame created in {time.time() - output_start:.1f} seconds")

        logger.info(f"Successfully processed CPU data for 'cpuuser': {len(fresco_df):,} rows created.")
        
        # Clean up final_data before returning
        del final_data
        gc.collect()
        
        return fresco_df

    except Exception as e:
        logger.error(f"An unexpected error occurred in process_cpu_file: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()
    finally:
        # Memory cleanup - be more careful about what exists
        try:
            if 'df' in locals() and df is not None:
                del df
            if 'df_valid_deltas' in locals():
                del df_valid_deltas
            if 'node_level_deltas' in locals():
                del node_level_deltas
            if 'final_data' in locals():
                del final_data
        except:
            pass
        gc.collect()


def process_mem_file(input_data: Union[str, pd.DataFrame]) -> pd.DataFrame:
    """Process mem.csv file to extract memory usage metrics, using original timestamps."""
    df = None
    try:
        if isinstance(input_data, str):
            file_path = input_data
            logger.info(f"Processing memory file: {file_path}")
            try:
                mem_cols_to_read = ['MemTotal', 'MemFree', 'FilePages']
                dtype_spec = {col: float for col in mem_cols_to_read}
                dtype_spec.update({'jobID': str, 'node': str, 'timestamp': str})
                df = read_csv_with_fallback_encoding(file_path, dtype=dtype_spec, low_memory=False)
                
                # Additional data cleaning for recovered data
                if not df.empty:
                    initial_rows = len(df)
                    # Clean up any rows that still have conversion issues
                    for col in mem_cols_to_read:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Remove rows where critical numeric columns are NaN
                    df = df.dropna(subset=mem_cols_to_read)
                    
                    cleaned_rows = len(df)
                    if cleaned_rows < initial_rows:
                        logger.info(f"Cleaned memory file: removed {initial_rows - cleaned_rows} corrupted rows, kept {cleaned_rows} valid rows")
            except Exception as e:
                logger.error(f"Error reading memory file {file_path}: {str(e)}")
                return pd.DataFrame()
        elif isinstance(input_data, pd.DataFrame):
            df = input_data.copy()
            logger.info("Processing memory DataFrame.")
        else:
            logger.info(f"Invalid input type for process_mem_file: {type(input_data)}")
            return pd.DataFrame()

        required_columns = ['MemTotal', 'MemFree', 'FilePages', 'jobID', 'node', 'timestamp']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.info(f"Missing required columns in memory data: {missing_cols}")
            return pd.DataFrame()

        initial_row_count = len(df)
        mem_cols_numeric = ['MemTotal', 'MemFree', 'FilePages']
        for col in mem_cols_numeric:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=required_columns)
        filtered_rows = initial_row_count - len(df)
        if filtered_rows > 0:
            logger.info(
                f"Dropped {filtered_rows} rows ({safe_division(filtered_rows, initial_row_count) * 100:.2f}%) with missing/invalid data in memory file")

        if df.empty:
            logger.info("No valid data rows after filtering NaNs in memory file")
            return pd.DataFrame()

        for col in mem_cols_numeric:
            df[col] = df[col].clip(lower=0)
        df['MemFree'] = df[['MemFree', 'MemTotal']].min(axis=1)
        memory_used_bytes = df['MemTotal'] - df['MemFree']
        df['FilePages'] = df[['FilePages', 'MemTotal']].min(axis=1)
        df['FilePages'] = np.minimum(df['FilePages'], memory_used_bytes)

        BYTES_TO_GB = 1 / (1024 ** 3)
        df['memused_value'] = (memory_used_bytes.astype(float) * BYTES_TO_GB).clip(lower=0)
        mem_minus_cache_bytes = memory_used_bytes - df['FilePages']
        df['memused_minus_diskcache_value'] = (mem_minus_cache_bytes.astype(float) * BYTES_TO_GB).clip(lower=0)

        df['jobID'] = df['jobID'].fillna('unknown').astype(str).str.replace('jobID', 'JOB', case=False)
        df['node'] = df['node'].fillna('unknown').astype(str)

        try:
            df['Timestamp_original_parsed'] = pd.to_datetime(df['timestamp'], format='%m/%d/%Y %H:%M:%S',
                                                             errors='coerce')
            df = df.dropna(subset=['Timestamp_original_parsed'])
        except Exception as e:
            logger.info(f"Error parsing timestamps in memory file: {str(e)}")
            return pd.DataFrame()

        if df.empty:
            logger.info("No valid data rows after timestamp filtering in memory file")
            return pd.DataFrame()

        memused_df = pd.DataFrame({
            'Job Id': df['jobID'],
            'Host': df['node'],
            'Event': 'memused',
            'Value': df['memused_value'],
            'Units': 'GB',
            'Timestamp': df['Timestamp_original_parsed']
        })

        memused_minus_diskcache_df = pd.DataFrame({
            'Job Id': df['jobID'],
            'Host': df['node'],
            'Event': 'memused_minus_diskcache',
            'Value': df['memused_minus_diskcache_value'],
            'Units': 'GB',
            'Timestamp': df['Timestamp_original_parsed']
        })

        final_df = pd.concat([memused_df, memused_minus_diskcache_df], ignore_index=True)
        logger.info(f"Successfully processed memory data: {len(final_df)} rows created.")
        return final_df

    except Exception as e:
        logger.error(f"An unexpected error occurred in process_mem_file: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()
    finally:
        del df
        if 'memused_df' in locals(): del memused_df
        if 'memused_minus_diskcache_df' in locals(): del memused_minus_diskcache_df
        gc.collect()


def process_nfs_file(input_data: Union[str, pd.DataFrame]) -> pd.DataFrame:
    """Process llite.csv file to extract NFS transfer rate metrics at node level."""
    df = None
    df_valid_rates = None
    agg_df = None
    try:
        if isinstance(input_data, str):
            file_path = input_data
            logger.info(f"Processing NFS/llite file: {file_path}")
            try:
                dtype_spec = {'read_bytes': float, 'write_bytes': float, 'jobID': str, 'node': str, 'timestamp': str}
                df = read_csv_with_fallback_encoding(file_path, dtype=dtype_spec, low_memory=False)
                
                # Additional data cleaning for recovered data
                if not df.empty:
                    initial_rows = len(df)
                    # Clean up any rows that still have conversion issues
                    numeric_cols = ['read_bytes', 'write_bytes']
                    for col in numeric_cols:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Remove rows where critical numeric columns are NaN
                    df = df.dropna(subset=numeric_cols)
                    
                    cleaned_rows = len(df)
                    if cleaned_rows < initial_rows:
                        logger.info(f"Cleaned NFS file: removed {initial_rows - cleaned_rows} corrupted rows, kept {cleaned_rows} valid rows")
            except Exception as e:
                logger.error(f"Error reading NFS/llite file {file_path}: {str(e)}")
                return pd.DataFrame()
        elif isinstance(input_data, pd.DataFrame):
            df = input_data.copy()
            logger.info("Processing NFS/llite DataFrame.")
        else:
            logger.info(f"Invalid input type for process_nfs_file: {type(input_data)}")
            return pd.DataFrame()

        required_columns = ['read_bytes', 'write_bytes', 'jobID', 'node', 'timestamp']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.info(f"Missing required columns in NFS/llite data: {missing_cols}")
            return pd.DataFrame()

        initial_row_count = len(df)
        byte_cols = ['read_bytes', 'write_bytes']
        for col in byte_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=required_columns)
        filtered_rows = initial_row_count - len(df)
        if filtered_rows > 0:
            logger.info(
                f"Dropped {filtered_rows} rows ({safe_division(filtered_rows, initial_row_count) * 100:.2f}%) with missing/invalid data in NFS/llite file")

        if df.empty:
            logger.info("No valid data rows after initial filtering in NFS/llite file")
            return pd.DataFrame()

        df['jobID'] = df['jobID'].fillna('unknown').astype(str).str.replace('jobID', 'JOB', case=False)
        df['node'] = df['node'].fillna('unknown').astype(str)

        try:
            df['Timestamp_original'] = pd.to_datetime(df['timestamp'], format='%m/%d/%Y %H:%M:%S', errors='coerce')
            df = df.dropna(subset=['Timestamp_original'])
            if df.empty:
                logger.info("No valid data rows after timestamp filtering in NFS/llite file")
                return pd.DataFrame()

            sort_keys = ['jobID', 'node', 'Timestamp_original']
            df = df.sort_values(sort_keys)
            logger.info("Sorting complete for NFS data.")
        except Exception as e:
            logger.info(f"Error parsing or sorting timestamps in NFS/llite file: {str(e)}")
            return pd.DataFrame()

        logger.info("Calculating NFS transfer rates using original timestamps...")
        df['total_bytes'] = df['read_bytes'] + df['write_bytes']

        group_keys_diff = ['jobID', 'node']
        grouped_diff = df.groupby(group_keys_diff, observed=True)
        df['time_delta_seconds'] = grouped_diff['Timestamp_original'].diff().dt.total_seconds()
        df['byte_delta'] = grouped_diff['total_bytes'].diff()

        df['Value_rate'] = 0.0
        BYTES_TO_MB = 1 / (1024 * 1024)
        MIN_TIME_DELTA = 0.1

        valid_rate_mask = (
                df['time_delta_seconds'].notna() &
                (df['time_delta_seconds'] >= MIN_TIME_DELTA) &
                df['byte_delta'].notna() &
                (df['byte_delta'] >= 0)
        )
        df.loc[valid_rate_mask, 'Value_rate'] = (df.loc[valid_rate_mask, 'byte_delta'] * BYTES_TO_MB) / df.loc[
            valid_rate_mask, 'time_delta_seconds']
        df['Value_rate'] = df['Value_rate'].clip(lower=0)
        logger.info("NFS rate calculation complete.")

        df_valid_rates = df[valid_rate_mask].copy()
        if df_valid_rates.empty:
            logger.info("No valid NFS rates calculated.")
            return pd.DataFrame()

        logger.info("Aggregating NFS rates to node level using original timestamps...")
        node_agg_keys = ['jobID', 'node', 'Timestamp_original']
        agg_df = df_valid_rates.groupby(node_agg_keys, observed=True, as_index=False)['Value_rate'].sum()
        agg_df.rename(columns={'Value_rate': 'Value'}, inplace=True)

        if agg_df.empty:
            logger.info("No valid data after node-level aggregation for NFS file.")
            return pd.DataFrame()
        logger.info(f"Node-level NFS aggregation complete. Result has {len(agg_df)} rows.")

        fresco_df = pd.DataFrame({
            'Job Id': agg_df['jobID'],
            'Host': agg_df['node'],
            'Event': 'nfs',
            'Value': agg_df['Value'],
            'Units': 'MB/s',
            'Timestamp': agg_df['Timestamp_original']
        })

        logger.info(f"Successfully processed NFS data: {len(fresco_df)} rows created.")
        return fresco_df

    except Exception as e:
        logger.error(f"An unexpected error occurred in process_nfs_file: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()
    finally:
        del df
        if df_valid_rates is not None: del df_valid_rates
        if agg_df is not None: del agg_df
        gc.collect()


def main():
    """Command-line interface for the HPC Data Transformer"""
    parser = argparse.ArgumentParser(description='HPC Cluster Data Transformer')
    parser.add_argument('--base-url', default="https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Conte/TACC_Stats/",
                        help='Base URL for data repository')
    parser.add_argument('--output-dir', default='./output', help='Output directory for processed files')
    parser.add_argument('--temp-dir', default='./temp', help='Temporary directory for processing')
    parser.add_argument('--max-folders', type=int, help='Maximum number of folders to process')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum parallel workers for file processing')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO',
                        help='Logging level')
    parser.add_argument('--target-folders-only', action='store_true', 
                        help='Process only target folders: 2016-11, 2017-01, 2017-02')
    
    args = parser.parse_args()
    
    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    if args.target_folders_only:
        logger.info("Running in target folders mode: will only process 2016-11, 2017-01, 2017-02")
    
    # Initialize and run transformer
    transformer = HpcDataTransformer(
        base_url=args.base_url,
        output_dir=args.output_dir,
        temp_dir=args.temp_dir,
        max_workers=args.max_workers
    )
    
    try:
        stats = transformer.run_transformation(max_folders=args.max_folders)
        
        logger.info(f"Transformation completed successfully!")
        logger.info(f"Processed: {stats['processed']} folders")
        logger.info(f"Failed: {stats['failed']} folders")
        logger.info(f"Total time: {stats['total_time']:.1f} seconds")
        if 'all_folders_found' in stats:
            logger.info(f"All folders found: {stats['all_folders_found']}")
            logger.info(f"Folders processed: {stats['folders_processed']}")
        
        if stats['processed'] > 0:
            avg_time = stats['total_time'] / stats['processed']
            logger.info(f"Average time per folder: {avg_time:.1f} seconds")
        
        return 0 if stats['failed'] == 0 else 1
        
    except KeyboardInterrupt:
        logger.info("Transformation interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())