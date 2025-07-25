import sys
import time
import logging
import os
import shutil
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- Configuration ---
SOURCE_DIR = r"U:\projects\conte-to-fresco-etl\step-1\output"
DESTINATION_DIR = r"<LOCAL_PATH_PLACEHOLDER>\Documents\Conte\conte-transformed-by-step-1"
# Add a delay to wait for file writing to potentially finish
# Longer delay for parquet chunks to ensure writing is complete
WAIT_DELAY_SECONDS = 10
MAX_MOVE_ATTEMPTS = 3  # Number of times to retry moving before giving up
RETRY_DELAY_SECONDS = 3  # Delay between move retries
# --- End Configuration ---

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class MoveEventHandler(FileSystemEventHandler):
    """Handles file creation events."""

    def __init__(self, source_path: Path, dest_path: Path):
        self.source_path = source_path
        self.dest_path = dest_path
        # Ensure destination exists
        self.dest_path.mkdir(parents=True, exist_ok=True)
        logging.info(f"Monitoring directory: {self.source_path}")
        logging.info(f"Moving new files to: {self.dest_path}")

    def on_created(self, event):
        """Called when a file or directory is created."""
        if event.is_directory:
            return  # Ignore directory creation events

        src_file_path = Path(event.src_path)
        
        # Only process .parquet files
        if src_file_path.suffix.lower() != '.parquet':
            logging.debug(f"Ignoring non-parquet file: {src_file_path}")
            return
        
        dest_file_path = self.dest_path / src_file_path.name

        logging.info(f"Detected new parquet file: {src_file_path}")

        # Wait briefly to allow the file writing process to potentially complete
        time.sleep(WAIT_DELAY_SECONDS)

        moved_successfully = False
        for attempt in range(MAX_MOVE_ATTEMPTS):
            try:
                # Check if source file still exists before attempting move
                if not src_file_path.exists():
                    logging.warning(f"Source file {src_file_path} disappeared before move attempt {attempt + 1}.")
                    # If it disappeared, maybe another process moved it? Consider it handled.
                    moved_successfully = True  # Or False depending on desired logic
                    break

                logging.info(f"Attempt {attempt + 1}/{MAX_MOVE_ATTEMPTS}: Moving {src_file_path} to {dest_file_path}")
                shutil.move(str(src_file_path), str(dest_file_path))  # Use strings for shutil compatibility

                # Double-check if move succeeded and source is gone
                if dest_file_path.exists() and not src_file_path.exists():
                    logging.info(f"Successfully moved {src_file_path.name} to {self.dest_path}")
                    moved_successfully = True
                    break  # Exit retry loop on success
                else:
                    # This case is less likely with shutil.move but possible on weird errors
                    logging.warning(
                        f"Move attempt {attempt + 1} for {src_file_path.name} seemed complete, but source/destination state inconsistent.")
                    # Decide whether to retry or mark as failed

            except (OSError, shutil.Error, PermissionError, FileNotFoundError) as e:
                logging.error(f"Error moving {src_file_path.name} on attempt {attempt + 1}: {e}")
                if attempt < MAX_MOVE_ATTEMPTS - 1:
                    logging.info(f"Retrying in {RETRY_DELAY_SECONDS} seconds...")
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    logging.error(f"Failed to move {src_file_path.name} after {MAX_MOVE_ATTEMPTS} attempts.")
            except Exception as e:
                logging.exception(
                    f"An unexpected error occurred moving {src_file_path.name} on attempt {attempt + 1}: {e}")
                # Break on unexpected errors, may not be recoverable by retry
                break


def process_existing_files(source_path: Path, event_handler: MoveEventHandler):
    """Process any existing files in the source directory at startup."""
    logging.info(f"Checking for existing parquet files in {source_path}")
    existing_files = [f for f in source_path.iterdir() if f.is_file() and f.suffix.lower() == '.parquet']

    if not existing_files:
        logging.info("No existing parquet files found to process.")
        return

    logging.info(f"Found {len(existing_files)} existing parquet files to process.")
    for file_path in existing_files:
        # Create a mock event to reuse the existing handler logic
        mock_event = type('MockEvent', (), {'src_path': str(file_path), 'is_directory': False})
        event_handler.on_created(mock_event)


if __name__ == "__main__":
    source_path = Path(SOURCE_DIR)
    dest_path = Path(DESTINATION_DIR)

    if not source_path.is_dir():
        logging.error(f"Source directory '{SOURCE_DIR}' does not exist or is not a directory.")
        sys.exit(1)
    if not dest_path.exists():
        logging.info(f"Destination directory '{DESTINATION_DIR}' does not exist. Creating it.")
        try:
            dest_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logging.error(f"Could not create destination directory '{DESTINATION_DIR}': {e}")
            sys.exit(1)
    elif not dest_path.is_dir():
        logging.error(f"Destination path '{DESTINATION_DIR}' exists but is not a directory.")
        sys.exit(1)

    event_handler = MoveEventHandler(source_path, dest_path)

    # Process any existing files before starting the observer
    process_existing_files(source_path, event_handler)

    observer = Observer()
    observer.schedule(event_handler, path=str(source_path), recursive=False)  # Monitor only the top level

    logging.info("Starting observer...")
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping observer due to KeyboardInterrupt...")
        observer.stop()
    except Exception as e:
        logging.exception("An unexpected error occurred in the main loop:")
        observer.stop()
    finally:
        observer.join()
        logging.info("Observer stopped.")