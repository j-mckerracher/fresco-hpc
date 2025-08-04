
import os
import time
import requests
from pathlib import Path
from typing import List, Dict, Tuple
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from hpc_etl_pipeline.src.core.exceptions import ExtractionError
from hpc_etl_pipeline.src.extractors.base_extractor import BaseExtractor
from hpc_etl_pipeline.src.utils.logger import get_logger

logger = get_logger(__name__)

class HttpExtractor(BaseExtractor):
    """Extracts data from a remote HTTP source."""

    def __init__(self, max_workers: int = 8, max_retries: int = 3):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'HPC-ETL-Pipeline/1.0'})

    def extract(self, source_config: Dict) -> List[Path]:
        """
        Extracts files from the remote HTTP source.

        Args:
            source_config: The source configuration dictionary.

        Returns:
            A list of paths to the downloaded files.
        """
        base_url = source_config.get("base_url")
        if not base_url:
            raise ExtractionError("Missing 'base_url' in source configuration.")

        folder_pattern = source_config.get("folder_pattern")
        if not folder_pattern:
            raise ExtractionError("Missing 'folder_pattern' in source configuration.")

        file_patterns = source_config.get("file_patterns")
        if not file_patterns:
            raise ExtractionError("Missing 'file_patterns' in source configuration.")

        temp_dir = source_config.get("temp_directory", "./temp")
        Path(temp_dir).mkdir(exist_ok=True)

        discovered_folders = self._discover_folders(base_url, folder_pattern)
        if not discovered_folders:
            logger.warning(f"No folders found matching pattern: {folder_pattern}")
            return []

        download_tasks = []
        for folder in discovered_folders:
            for file_pattern in file_patterns:
                file_url = urljoin(base_url, f"{folder}/{file_pattern}")
                local_path = Path(temp_dir) / folder / file_pattern
                download_tasks.append((file_url, str(local_path)))

        downloaded_files = self._download_files_parallel(download_tasks)
        return [Path(p) for p, success in downloaded_files.items() if success]

    def _discover_folders(self, base_url: str, folder_pattern: str) -> List[str]:
        """Discovers folders matching a regex pattern from a URL."""
        try:
            response = self.session.get(base_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            folders = []
            import re
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if re.match(folder_pattern, href):
                    folders.append(href.rstrip('/'))
            folders.sort()
            logger.info(f"Discovered {len(folders)} folders.")
            return folders
        except Exception as e:
            raise ExtractionError(f"Error discovering folders from {base_url}: {e}")

    def _download_file(self, url: str, local_path: str, timeout: int = 300) -> bool:
        """Downloads a single file with retry logic."""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=timeout, stream=True)
                response.raise_for_status()
                Path(local_path).parent.mkdir(parents=True, exist_ok=True)
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                if Path(local_path).exists() and Path(local_path).stat().st_size > 0:
                    logger.debug(f"Successfully downloaded {url} to {local_path}")
                    return True
                else:
                    raise ExtractionError(f"Downloaded file is empty or missing: {local_path}")
            except Exception as e:
                wait_time = (2 ** attempt) * 1
                logger.warning(f"Download attempt {attempt + 1} failed for {url}: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
        logger.error(f"Failed to download {url} after {self.max_retries} attempts.")
        return False

    def _download_files_parallel(self, download_tasks: List[Tuple[str, str]]) -> Dict[str, bool]:
        """Downloads multiple files in parallel."""
        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self._download_file, url, path): path for url, path in download_tasks}
            for future in as_completed(future_to_url):
                path = future_to_url[future]
                try:
                    success = future.result()
                    results[path] = success
                except Exception as e:
                    logger.error(f"Download task failed for {path}: {e}")
                    results[path] = False
        return results
