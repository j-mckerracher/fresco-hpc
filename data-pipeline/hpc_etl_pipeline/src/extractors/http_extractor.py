"""HTTP-based data extractor for remote repositories."""

import os
import re
import time
from pathlib import Path
from typing import Iterator, Dict, Any, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
import logging

from .base_extractor import BaseExtractor
from ..core.exceptions import ExtractionError

logger = logging.getLogger(__name__)


class HttpExtractor(BaseExtractor):
    """Extractor for HTTP-based remote repositories."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = self.source_config.get('base_url')
        self.max_workers = config.get('processing', {}).get('max_workers', 4)
        self.temp_dir = Path(config.get('processing', {}).get('temp_directory', './temp'))
        self.temp_dir.mkdir(exist_ok=True, parents=True)
        
        # Initialize components
        self.downloader = FileDownloader(max_workers=self.max_workers)
        self.folder_discovery = FolderDiscovery(self.base_url)
        
        if not self.base_url:
            raise ExtractionError("base_url is required for HTTP extractor")
    
    def extract(self, source: Optional[str] = None) -> Iterator[Path]:
        """
        Extract files from HTTP source.
        
        Args:
            source: Optional specific folder to extract
            
        Yields:
            Path objects to extracted files
        """
        try:
            if source:
                # Extract specific folder
                yield from self._extract_folder(source)
            else:
                # Discover and extract all folders
                folders = self.folder_discovery.discover_folders()
                for folder in folders:
                    yield from self._extract_folder(folder)
                    
        except Exception as e:
            raise ExtractionError(f"Failed to extract from HTTP source: {e}")
    
    def _extract_folder(self, folder_name: str) -> Iterator[Path]:
        """Extract files from a specific folder."""
        folder_temp_dir = self.temp_dir / folder_name
        folder_temp_dir.mkdir(exist_ok=True, parents=True)
        
        # Download files
        if self._download_folder_files(folder_name, folder_temp_dir):
            # Yield all downloaded files
            file_patterns = self.get_file_patterns()
            for pattern in file_patterns:
                for file_path in folder_temp_dir.glob(pattern):
                    yield file_path
    
    def _download_folder_files(self, folder_name: str, folder_temp_dir: Path) -> bool:
        """Download all required files for a folder."""
        download_tasks = []
        file_patterns = self.get_file_patterns()
        
        for file_pattern in file_patterns:
            file_url = f"{self.base_url.rstrip('/')}/{folder_name}/{file_pattern}"
            local_path = folder_temp_dir / file_pattern
            download_tasks.append((file_url, str(local_path)))
        
        # Download files in parallel
        results = self.downloader.download_files_parallel(download_tasks)
        
        # Check if all downloads were successful
        success_count = sum(1 for success in results.values() if success)
        total_files = len(download_tasks)
        
        logger.info(f"Downloaded {success_count}/{total_files} files for folder {folder_name}")
        
        return success_count > 0  # Return True if at least one file was downloaded
    
    def validate_source(self) -> bool:
        """Validate that the HTTP source is accessible."""
        try:
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"HTTP source validation failed: {e}")
            return False
    
    def cleanup(self):
        """Clean up temporary files."""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            logger.info(f"Cleaned up temporary directory: {self.temp_dir}")


class FileDownloader:
    """Handles parallel file downloads with retry logic."""
    
    def __init__(self, max_workers: int = 8, max_retries: int = 3):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'HPC-ETL-Pipeline/1.0'})
    
    def download_file(self, url: str, local_path: str, timeout: int = 300) -> bool:
        """Download a single file with retry logic."""
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
        """Download multiple files in parallel."""
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
    """Discovers available folders from remote repository."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
    
    def discover_folders(self, folder_pattern: Optional[str] = None) -> List[str]:
        """
        Discover folders from the repository.
        
        Args:
            folder_pattern: Optional regex pattern for folder matching
            
        Returns:
            List of discovered folder names
        """
        try:
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            folders = []
            
            # Use provided pattern or default monthly pattern
            pattern = folder_pattern or r'^\d{4}-\d{2}/?$'
            
            # Look for links that match the pattern
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if self._matches_pattern(href, pattern):
                    folders.append(href.rstrip('/'))
            
            # Sort folders chronologically
            folders.sort()
            logger.info(f"Discovered {len(folders)} folders")
            return folders
            
        except Exception as e:
            logger.error(f"Error discovering folders from {self.base_url}: {e}")
            return []
    
    def _matches_pattern(self, folder_name: str, pattern: str) -> bool:
        """Check if folder name matches the given pattern."""
        return bool(re.match(pattern, folder_name))