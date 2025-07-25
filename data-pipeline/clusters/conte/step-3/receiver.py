import os
import shutil
import time
import logging
import hashlib
from pathlib import Path
from typing import List, Optional
import signal
import sys

# --- Configuration ---
SOURCE_DIR = Path(r"U:\projects\conte-to-fresco-etl\step-3\output")
DESTINATION_DIR = Path(r"<LOCAL_PATH_PLACEHOLDER>\Documents\Conte\conte-transformed-by-step-3")
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
        logging.FileHandler('logs/receiver.log'),
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


def calculate_file_hash(file_path: Path) -> Optional[str]:
    """Calculate MD5 hash of file for integrity verification"""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logging.error(f"Error calculating hash for {file_path}: {e}")
        return None


def is_file_ready_for_transfer(file_path: Path) -> bool:
    """
    Check if a file is ready for transfer by ensuring it's not being written to
    and doesn't have a temporary suffix.
    """
    # Skip temporary files
    if file_path.suffix == TEMP_SUFFIX:
        return False
    
    try:
        # Try to open the file in exclusive mode to check if it's locked
        with open(file_path, 'r+b') as f:
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
    Wait for file to be stable (not changing size) for at least 5 seconds.
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
            if stable_time >= 5:  # File stable for 5 seconds
                return True
        else:
            stable_time = 0
            last_size = current_size
    
    return False


def move_file_safely(source: Path, destination: Path) -> bool:
    """
    Move file safely with integrity verification and cleanup.
    Returns True if successful, False otherwise.
    """
    temp_destination = destination.with_suffix(destination.suffix + TEMP_SUFFIX)
    
    try:
        # Ensure destination directory exists
        destination.parent.mkdir(parents=True, exist_ok=True)
        
        # Calculate source file hash before transfer
        logging.info(f"Calculating hash for {source.name}")
        source_hash = calculate_file_hash(source)
        if not source_hash:
            logging.error(f"Failed to calculate hash for {source.name}")
            return False
        
        # Copy to temporary file first
        logging.info(f"Copying {source.name} to {temp_destination.name}")
        shutil.copy2(source, temp_destination)
        
        # Verify the copy was successful by comparing file sizes
        if source.stat().st_size != temp_destination.stat().st_size:
            logging.error(f"Size mismatch after copy: {source.name}")
            temp_destination.unlink(missing_ok=True)
            return False
        
        # Verify integrity using hash
        logging.info(f"Verifying integrity of {temp_destination.name}")
        destination_hash = calculate_file_hash(temp_destination)
        if not destination_hash or source_hash != destination_hash:
            logging.error(f"Hash mismatch for {source.name}. Transfer corrupted.")
            temp_destination.unlink(missing_ok=True)
            return False
        
        # Rename to final name (atomic operation)
        temp_destination.rename(destination)
        
        # Delete source file only after successful verification
        logging.info(f"Deleting source file: {source.name}")
        source.unlink()
        
        logging.info(f"Successfully transferred and deleted: {source.name} -> {destination.name}")
        return True
        
    except Exception as e:
        logging.error(f"Error moving {source.name}: {e}")
        # Clean up temporary file if it exists
        temp_destination.unlink(missing_ok=True)
        return False


def find_parquet_files(directory: Path) -> List[Path]:
    """Find all .parquet files in the directory recursively"""
    try:
        return [f for f in directory.rglob("*.parquet") if f.is_file()]
    except Exception as e:
        logging.error(f"Error scanning directory {directory}: {e}")
        return []


def process_files() -> None:
    """Main processing loop to find and transfer files"""
    processed_files = set()  # Track files we've already processed
    
    while not shutdown_requested:
        try:
            # Find all parquet files in source directory (recursive)
            parquet_files = find_parquet_files(SOURCE_DIR)
            
            # Filter out files we've already processed and temporary files
            new_files = [
                f for f in parquet_files 
                if f not in processed_files and is_file_ready_for_transfer(f)
            ]
            
            if new_files:
                logging.info(f"Found {len(new_files)} new files to process")
                
                for file_path in new_files:
                    if shutdown_requested:
                        break
                    
                    # Wait for file to be stable
                    if not wait_for_stable_file(file_path):
                        logging.warning(f"File {file_path.name} not stable, skipping for now")
                        continue
                    
                    # Calculate destination path maintaining directory structure
                    try:
                        relative_path = file_path.relative_to(SOURCE_DIR)
                        destination_path = DESTINATION_DIR / relative_path
                    except ValueError:
                        logging.error(f"Cannot determine relative path for {file_path}")
                        processed_files.add(file_path)  # Mark as processed to avoid infinite retry
                        continue
                    
                    # Move file to destination
                    if move_file_safely(file_path, destination_path):
                        processed_files.add(file_path)
                        logging.info(f"Successfully processed {file_path.name}")
                    else:
                        logging.error(f"Failed to transfer {file_path.name}")
                        # Don't add to processed_files to allow retry on next iteration
            
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
    
    # Create destination directory if it doesn't exist
    DESTINATION_DIR.mkdir(parents=True, exist_ok=True)
    
    # Logs directory already created at module level
    
    logging.info("=" * 60)
    logging.info("RECEIVER SCRIPT STARTED")
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
        logging.info("Receiver script shutting down")
        if shutdown_requested:
            print("Receiver script stopped gracefully")
        else:
            print("Receiver script stopped")


if __name__ == "__main__":
    main()