
import os
import shutil
import subprocess
from typing import Tuple

from hpc_etl_pipeline.src.utils.logger import get_logger

logger = get_logger(__name__)

class DiskSpaceManager:
    """Monitors and manages disk space usage."""

    def __init__(self, min_free_gb: float = 10.0):
        self.min_free_gb = min_free_gb

    def check_disk_space(self, path: str = ".") -> Tuple[bool, float]:
        """Check if sufficient disk space is available."""
        try:
            total, used, free = shutil.disk_usage(path)
            free_gb = free / (1024**3)
            has_space = free_gb >= self.min_free_gb
            logger.info(f"Disk space check: {free_gb:.1f}GB available.")
            return has_space, free_gb
        except Exception as e:
            logger.warning(f"Could not check disk space: {e}. Assuming sufficient space.")
            return True, float('inf')

    def cleanup_temp_files(self, temp_dir: str):
        """Clean up temporary files and directories."""
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            logger.error(f"Error cleaning up temp directory {temp_dir}: {e}")
