
import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from hpc_etl_pipeline.src.core.pipeline import Pipeline
from hpc_etl_pipeline.src.utils.logger import get_logger

logger = get_logger(__name__)

class FileWatcher(FileSystemEventHandler):
    """Watches for new files and triggers the ETL pipeline."""

    def __init__(self, pipeline: Pipeline, watch_dir: str):
        self.pipeline = pipeline
        self.watch_dir = Path(watch_dir)

    def on_created(self, event):
        """Called when a file or directory is created."""
        if not event.is_directory:
            logger.info(f"New file detected: {event.src_path}")
            # This is where the pipeline would be triggered with the new file.
            # self.pipeline.process_file(event.src_path)

    def start(self):
        """Starts the file watcher."""
        observer = Observer()
        observer.schedule(self, str(self.watch_dir), recursive=True)
        observer.start()
        logger.info(f"Watching for new files in: {self.watch_dir}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()
