"""File watcher for Globus integration."""

import time
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from ..core.pipeline import Pipeline

logger = logging.getLogger(__name__)


class PipelineFileHandler(FileSystemEventHandler):
    """File system event handler that triggers pipeline processing."""
    
    def __init__(self, pipeline: Pipeline, 
                 wait_delay_seconds: int = 10,
                 max_move_attempts: int = 3,
                 retry_delay_seconds: int = 3):
        """
        Initialize file handler.
        
        Args:
            pipeline: Pipeline instance to use for processing
            wait_delay_seconds: Delay to wait for file writing to complete
            max_move_attempts: Maximum attempts to process a file
            retry_delay_seconds: Delay between retry attempts
        """
        self.pipeline = pipeline
        self.wait_delay_seconds = wait_delay_seconds
        self.max_move_attempts = max_move_attempts
        self.retry_delay_seconds = retry_delay_seconds
        
        # Get file patterns from pipeline configuration
        self.file_patterns = pipeline.config['source'].get('file_patterns', [])
        
    def on_created(self, event):
        """Handle file creation events."""
        if event.is_directory:
            return
        
        file_path = Path(event.src_path)
        
        # Check if file matches expected patterns
        if not self._matches_patterns(file_path):
            logger.debug(f"Ignoring file (doesn't match patterns): {file_path}")
            return
        
        logger.info(f"Detected new file: {file_path}")
        
        # Wait for file writing to complete
        if self.wait_delay_seconds > 0:
            logger.info(f"Waiting {self.wait_delay_seconds} seconds for file writing to complete...")
            time.sleep(self.wait_delay_seconds)
        
        # Process the file with retry logic
        self._process_file_with_retry(file_path)
    
    def _matches_patterns(self, file_path: Path) -> bool:
        """Check if file matches configured patterns."""
        if not self.file_patterns:
            return True  # No patterns configured, accept all files
        
        file_name = file_path.name.lower()
        return any(pattern.lower() in file_name for pattern in self.file_patterns)
    
    def _process_file_with_retry(self, file_path: Path):
        """Process file with retry logic."""
        for attempt in range(self.max_move_attempts):
            try:
                if not file_path.exists():
                    logger.error(f"File no longer exists: {file_path}")
                    return
                
                # Check if file is still being written (size changing)
                if self._is_file_stable(file_path):
                    logger.info(f"Processing file (attempt {attempt + 1}): {file_path}")
                    
                    # Process through pipeline
                    success = self.pipeline.process_file(str(file_path))
                    
                    if success:
                        logger.info(f"Successfully processed: {file_path}")
                        return
                    else:
                        logger.error(f"Pipeline processing failed for: {file_path}")
                        if attempt < self.max_move_attempts - 1:
                            logger.info(f"Retrying in {self.retry_delay_seconds} seconds...")
                            time.sleep(self.retry_delay_seconds)
                else:
                    logger.info(f"File still being written, waiting... (attempt {attempt + 1})")
                    time.sleep(self.retry_delay_seconds)
                    
            except Exception as e:
                logger.error(f"Error processing file {file_path} (attempt {attempt + 1}): {e}")
                if attempt < self.max_move_attempts - 1:
                    logger.info(f"Retrying in {self.retry_delay_seconds} seconds...")
                    time.sleep(self.retry_delay_seconds)
        
        logger.error(f"Failed to process file after {self.max_move_attempts} attempts: {file_path}")
    
    def _is_file_stable(self, file_path: Path, stability_time: int = 2) -> bool:
        """Check if file size is stable (not being actively written)."""
        try:
            initial_size = file_path.stat().st_size
            time.sleep(stability_time)
            final_size = file_path.stat().st_size
            return initial_size == final_size
        except Exception as e:
            logger.warning(f"Could not check file stability for {file_path}: {e}")
            return True  # Assume stable if we can't check


class FileWatcher:
    """File watcher for monitoring directories and triggering pipeline processing."""
    
    def __init__(self, pipeline: Pipeline, watch_config: Optional[Dict[str, Any]] = None):
        """
        Initialize file watcher.
        
        Args:
            pipeline: Pipeline instance to use for processing
            watch_config: Optional watcher configuration
        """
        self.pipeline = pipeline
        self.watch_config = watch_config or {}
        
        # Configuration
        self.source_dir = Path(self.watch_config.get('source_dir', './input'))
        self.wait_delay_seconds = self.watch_config.get('wait_delay_seconds', 10)
        self.max_move_attempts = self.watch_config.get('max_move_attempts', 3)
        self.retry_delay_seconds = self.watch_config.get('retry_delay_seconds', 3)
        
        # Create handler and observer
        self.event_handler = PipelineFileHandler(
            pipeline=self.pipeline,
            wait_delay_seconds=self.wait_delay_seconds,
            max_move_attempts=self.max_move_attempts,
            retry_delay_seconds=self.retry_delay_seconds
        )
        self.observer = Observer()
        
        # Ensure source directory exists
        self.source_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized file watcher for directory: {self.source_dir}")
    
    def start(self):
        """Start watching for file changes."""
        try:
            self.observer.schedule(
                self.event_handler,
                str(self.source_dir),
                recursive=self.watch_config.get('recursive', False)
            )
            
            self.observer.start()
            logger.info(f"Started file watcher on: {self.source_dir}")
            
            # Keep the watcher running
            try:
                while True:
                    time.sleep(1)
                    
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, stopping file watcher...")
                self.stop()
                
        except Exception as e:
            logger.error(f"Error starting file watcher: {e}")
            raise
    
    def stop(self):
        """Stop the file watcher."""
        try:
            self.observer.stop()
            self.observer.join()
            logger.info("File watcher stopped")
        except Exception as e:
            logger.error(f"Error stopping file watcher: {e}")
    
    def is_running(self) -> bool:
        """Check if the watcher is currently running."""
        return self.observer.is_alive() if self.observer else False