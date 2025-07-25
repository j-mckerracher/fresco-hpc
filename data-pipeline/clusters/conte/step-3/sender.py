import os
import shutil
import time
import logging
from pathlib import Path
from typing import List
import signal
import sys

# --- Configuration ---
SOURCE_DIR = Path(r"<LOCAL_PATH_PLACEHOLDER>\Documents\Conte\conte-transformed-by-step-2.5")
DESTINATION_DIR = Path(r"U:\projects\conte-to-fresco-etl\step-3\input")
CHECK_INTERVAL = 30  # seconds between checks for new files
TEMP_SUFFIX = ".tmp"  # temporary suffix during transfer

# Global flag for graceful shutdown
shutdown_requested = False

# Create logs directory if it doesn't exist
Path('logs').mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/sender.log'),
        logging.StreamHandler()
    ]
)


def signal_handler(signum, frame):
    """Handle SIGINT (Ctrl+C) gracefully"""
    global shutdown_requested
    if not shutdown_requested:
        shutdown_requested = True
        print("\n--- Graceful shutdown requested (Ctrl+C detected) ---")
        print("Waiting for current transfer to complete...")
        logging.info("Graceful shutdown requested via SIGINT")
    else:
        print("\nForce quit requested. Exiting immediately...")
        logging.warning("Force quit requested via second SIGINT")
        sys.exit(1)


# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)


def is_file_complete(file_path: Path) -> bool:
    """
    Check if a file is completely written by attempting to open it exclusively.
    Returns True if file is complete and ready for transfer.
    """
    try:
        # Try to open the file in exclusive mode
        with open(file_path, 'r+b') as f:
            # If we can open it exclusively, it's not being written to
            pass
        return True
    except (PermissionError, OSError):
        # File is still being written to or locked
        return False


def get_file_size(file_path: Path) -> int:
    """Get file size, return 0 if file doesn't exist or can't be accessed"""
    try:
        return file_path.stat().st_size
    except (FileNotFoundError, PermissionError):
        return 0


def wait_for_stable_file(file_path: Path, timeout: int = 60) -> bool:
    """
    Wait for file to be stable (not changing size) for at least 3 seconds.
    Returns True if file is stable, False if timeout reached.
    """
    start_time = time.time()
    last_size = get_file_size(file_path)
    stable_time = 0
    
    while time.time() - start_time < timeout:
        if shutdown_requested:
            return False
            
        time.sleep(1)
        current_size = get_file_size(file_path)
        
        if current_size == last_size and current_size > 0:
            stable_time += 1
            if stable_time >= 3:  # File stable for 3 seconds
                return True
        else:
            stable_time = 0
            last_size = current_size
    
    return False


def copy_file_safely(source: Path, destination: Path) -> bool:
    """
    Copy file safely using temporary name to prevent consumer from accessing
    incomplete files. Returns True if successful, False otherwise.
    """
    temp_destination = destination.with_suffix(destination.suffix + TEMP_SUFFIX)
    
    try:
        # Ensure destination directory exists
        destination.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy to temporary file first
        logging.info(f"Copying {source.name} to {temp_destination.name}")
        shutil.copy2(source, temp_destination)
        
        # Verify the copy was successful by comparing file sizes
        if source.stat().st_size != temp_destination.stat().st_size:
            logging.error(f"Size mismatch after copy: {source.name}")
            temp_destination.unlink(missing_ok=True)
            return False
        
        # Rename to final name (atomic operation)
        temp_destination.rename(destination)
        
        logging.info(f"Successfully transferred: {source.name} -> {destination.name}")
        return True
        
    except Exception as e:
        logging.error(f"Error copying {source.name}: {e}")
        # Clean up temporary file if it exists
        temp_destination.unlink(missing_ok=True)
        return False


def find_parquet_files(directory: Path) -> List[Path]:
    """Find all .parquet files in the directory (non-recursive)"""
    try:
        return [f for f in directory.iterdir() if f.suffix.lower() == '.parquet' and f.is_file()]
    except Exception as e:
        logging.error(f"Error scanning directory {directory}: {e}")
        return []


def process_files() -> None:
    """Main processing loop to find and transfer files"""
    transferred_files = set()  # Track files we've already processed
    
    while not shutdown_requested:
        try:
            # Find all parquet files in source directory
            parquet_files = find_parquet_files(SOURCE_DIR)
            
            # Filter out files we've already processed
            new_files = [f for f in parquet_files if f not in transferred_files]
            
            if new_files:
                logging.info(f"Found {len(new_files)} new files to process")
                
                for file_path in new_files:
                    if shutdown_requested:
                        break
                    
                    # Wait for file to be stable
                    if not wait_for_stable_file(file_path):
                        logging.warning(f"File {file_path.name} not stable, skipping for now")
                        continue
                    
                    # Check if file is complete
                    if not is_file_complete(file_path):
                        logging.info(f"File {file_path.name} still being written, skipping")
                        continue
                    
                    # Copy file to destination
                    destination_path = DESTINATION_DIR / file_path.name
                    
                    if copy_file_safely(file_path, destination_path):
                        transferred_files.add(file_path)
                        logging.info(f"Added {file_path.name} to transferred files list")
                    else:
                        logging.error(f"Failed to transfer {file_path.name}")
            
            # Sleep before next check
            for _ in range(CHECK_INTERVAL):
                if shutdown_requested:
                    break
                time.sleep(1)
                
        except Exception as e:
            logging.error(f"Error in main processing loop: {e}")
            time.sleep(5)  # Wait before retrying


def main():
    """Main function"""
    # Validate directories
    if not SOURCE_DIR.exists():
        logging.error(f"Source directory does not exist: {SOURCE_DIR}")
        return
    
    if not DESTINATION_DIR.exists():
        logging.error(f"Destination directory does not exist: {DESTINATION_DIR}")
        return
    
    # Logs directory already created at module level
    
    logging.info("=" * 60)
    logging.info("SENDER SCRIPT STARTED")
    logging.info(f"Source directory: {SOURCE_DIR}")
    logging.info(f"Destination directory: {DESTINATION_DIR}")
    logging.info(f"Check interval: {CHECK_INTERVAL} seconds")
    logging.info("=" * 60)
    
    try:
        process_files()
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        logging.info("Sender script shutting down")
        if shutdown_requested:
            print("Sender script stopped gracefully")
        else:
            print("Sender script stopped")


if __name__ == "__main__":
    main()