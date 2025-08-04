
import json
import threading
from datetime import datetime
from pathlib import Path

from hpc_etl_pipeline.src.utils.logger import get_logger

logger = get_logger(__name__)

class ProcessingTracker:
    """Tracks processing state and enables resumption from failures."""

    def __init__(self, status_file: str = "processing_status.json"):
        self.status_file = Path(status_file)
        self.processed_items = set()
        self.failed_items = set()
        self.lock = threading.RLock()
        self.load_status()

    def load_status(self):
        """Load processing status from JSON file."""
        if self.status_file.exists():
            try:
                with open(self.status_file, 'r') as f:
                    data = json.load(f)
                    self.processed_items = set(data.get('processed', []))
                    self.failed_items = set(data.get('failed', []))
                logger.info(f"Loaded status: {len(self.processed_items)} processed, {len(self.failed_items)} failed.")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load processing status: {e}")

    def save_status(self):
        """Save processing status to JSON file."""
        with self.lock:
            try:
                data = {
                    'processed': list(self.processed_items),
                    'failed': list(self.failed_items),
                    'last_updated': datetime.now().isoformat()
                }
                with open(self.status_file, 'w') as f:
                    json.dump(data, f, indent=2)
            except IOError as e:
                logger.error(f"Could not save processing status: {e}")

    def mark_processed(self, item: str):
        """Mark an item as successfully processed."""
        with self.lock:
            self.processed_items.add(item)
            self.failed_items.discard(item)
            self.save_status()

    def mark_failed(self, item: str):
        """Mark an item as failed."""
        with self.lock:
            self.failed_items.add(item)
            self.processed_items.discard(item)
            self.save_status()

    def is_processed(self, item: str) -> bool:
        """Check if an item has been processed."""
        return item in self.processed_items

class DataVersionManager:
    """Manages output file versioning."""

    def __init__(self, version_file: str = "version_info.json"):
        self.version_file = Path(version_file)
        self.versions = {}
        self.load_versions()

    def load_versions(self):
        """Load version information from JSON file."""
        if self.version_file.exists():
            try:
                with open(self.version_file, 'r') as f:
                    self.versions = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load version info: {e}")

    def save_versions(self):
        """Save version information to JSON file."""
        try:
            with open(self.version_file, 'w') as f:
                json.dump(self.versions, f, indent=2)
        except IOError as e:
            logger.error(f"Could not save version info: {e}")

    def get_next_version(self, item_name: str) -> int:
        """Get the next version number for an item."""
        current_version = self.versions.get(item_name, 0)
        next_version = current_version + 1
        self.versions[item_name] = next_version
        self.save_versions()
        return next_version
