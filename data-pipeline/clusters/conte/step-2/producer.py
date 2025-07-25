import os
import shutil
import time
import logging
from pathlib import Path
import signal
import re
from typing import Union, Tuple, Optional

# --- Configuration ---
METRICS_SOURCE_DIR = Path(r"<LOCAL_PATH_PLACEHOLDER>\conte-transformed-by-step-1-daily")
METRICS_DEST_DIR = Path(r"U:\projects\conte-to-fresco-etl\step-2\cache\input\metrics")

# New configuration for accounting files
ACCOUNTING_SOURCE_DIR = Path(r"<LOCAL_PATH_PLACEHOLDER>\conte-job-accounting-1")
ACCOUNTING_DEST_DIR = Path(r"U:\projects\conte-to-fresco-etl\step-2\cache\accounting")

# Signal directories
READY_SIGNAL_DIR = Path(r"U:\projects\conte-to-fresco-etl\step-2\cache\ready")
COMPOSER_READY_DIR = Path(r"U:\projects\conte-to-fresco-etl\step-2\cache\composer_ready")

# Pattern for files to move
METRICS_FILE_PATTERN = "*.parquet"
ACCOUNTING_FILE_PATTERN = "*.csv"
TEMP_EXTENSION = ".tmp"  # Temporary extension for files during transfer

MAX_FILES_IN_DESTINATION = 31  # This applies to metrics files only
POLL_INTERVAL_SECONDS = 10  # Check source directory every 10 seconds
DEST_FULL_WAIT_SECONDS = 30  # Wait longer if destination is full

LOG_FILE = "producer_script.log"
LOG_LEVEL = logging.INFO

# --- Global variable for graceful shutdown ---
shutdown_requested = False


def signal_handler(signum, frame):
    """Handles shutdown signals."""
    global shutdown_requested
    logging.info(f"Signal {signum} received. Requesting shutdown...")
    shutdown_requested = True


def setup_logging():
    """Configures logging for the script."""
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )


def extract_year_month_day_from_metric_file(filename: str) -> Optional[Tuple[str, str, str]]:
    """
    Extract year, month, and day from a metric filename following patterns:
    - FRESCO_Conte_ts_YYYY-MM-DD_vX.parquet
    - FRESCO_Conte_ts_YYYY-MM-DD.parquet

    Returns a tuple of (year, month, day) or None if pattern doesn't match.
    """
    # Match YYYY-MM-DD in the filename
    match = re.search(r'_(\d{4})-(\d{2})-(\d{2})(?:_v\d+)?\.parquet$', filename)
    if match:
        return match.group(1), match.group(2), match.group(3)  # year, month, day
    return None


def create_ready_signal(year: str, month: str, day: str):
    """
    Creates a ready signal file for a specific year-month-day in both ready signal directories.
    """
    for signal_dir in [READY_SIGNAL_DIR, COMPOSER_READY_DIR]:
        signal_dir.mkdir(parents=True, exist_ok=True)
        ready_file = signal_dir / f"{year}-{month}-{day}.ready"

        try:
            # Write an empty file as the signal
            ready_file.touch(exist_ok=True)
            logging.info(f"Created daily ready signal: {ready_file}")
        except Exception as e:
            logging.error(f"Failed to create daily ready signal {ready_file}: {e}")


def process_metric_file(metric_file: Path) -> bool:
    """
    Process a metric file, ensuring its corresponding accounting file is
    copied first if available.
    """
    # Extract year, month, and day from metric file
    year_month_day = extract_year_month_day_from_metric_file(metric_file.name)
    if not year_month_day:
        logging.warning(f"Cannot extract year-month-day from metric file {metric_file.name}. Skipping.")
        return False

    year, month, day = year_month_day

    # Check for corresponding accounting file in source and destination
    accounting_file = ACCOUNTING_SOURCE_DIR / f"{year}-{month}.csv"
    accounting_dest_file = ACCOUNTING_DEST_DIR / f"{year}-{month}.csv"

    # If accounting file doesn't exist at destination, it must exist at source
    if not accounting_dest_file.exists():
        if not accounting_file.exists():
            logging.warning(
                f"Accounting file not found at source {accounting_file} or destination {accounting_dest_file} "
                f"for metric file {metric_file.name}. Skipping."
            )
            return False

        # Copy the accounting file
        accounting_success = copy_file(accounting_file, ACCOUNTING_DEST_DIR)
        if not accounting_success:
            logging.error(f"Failed to copy accounting file {accounting_file.name}. Skipping metric file.")
            return False
    else:
        logging.info(f"Accounting file {accounting_dest_file.name} already exists at destination.")

    # Now copy the metric file
    # Ensure destination directory exists for daily file (YYYY-MM structure)
    daily_dest_dir = METRICS_DEST_DIR / f"{year}-{month}"
    daily_dest_dir.mkdir(parents=True, exist_ok=True)

    metric_success = copy_file(metric_file, daily_dest_dir)

    if metric_success:
        # Create ready signal after both files are successfully copied
        create_ready_signal(year, month, day)

    return metric_success


def get_final_files_in_destination(dest_dir: Path) -> list[Path]:
    """Counts non-temporary files in the specified destination directory and its subdirectories."""
    if not dest_dir.exists():
        return []

    result = []
    # Check both direct files in the directory and files in subdirectories
    for item in dest_dir.glob('**/*'):
        if item.is_file() and not item.name.endswith(TEMP_EXTENSION):
            result.append(item)

    return result


def copy_file(source_file_path: Path, dest_dir: Path) -> bool:
    """
    Copies a file from source to destination with atomicity using a temp file.
    Does NOT delete the source file.
    """
    file_name = source_file_path.name
    final_dest_path = dest_dir / file_name
    temp_dest_path = dest_dir / (file_name + TEMP_EXTENSION)

    logging.info(f"Copying file: {source_file_path} to {dest_dir}")

    try:
        # Pre-transfer checks
        if final_dest_path.exists():
            logging.info(f"File {final_dest_path} already exists at destination. Skipping copy.")
            return True  # File already exists, consider it a success

        if temp_dest_path.exists():
            logging.warning(f"Temporary file {temp_dest_path} exists. Deleting to ensure clean transfer.")
            try:
                temp_dest_path.unlink()
            except OSError as e:
                logging.error(f"Could not delete existing temporary file {temp_dest_path}: {e}. Skipping copy.")
                return False

        # Ensure destination directory exists
        dest_dir.mkdir(parents=True, exist_ok=True)

        # 1. Copy to temporary file
        logging.info(f"Copying {source_file_path} to {temp_dest_path}...")
        shutil.copy2(source_file_path, temp_dest_path)
        logging.info(f"Successfully copied to {temp_dest_path}.")

        # 2. Rename temporary file to final name (atomic operation)
        logging.info(f"Renaming {temp_dest_path} to {final_dest_path}...")
        os.rename(temp_dest_path, final_dest_path)  # os.rename is generally preferred for atomicity
        logging.info(f"Successfully renamed to {final_dest_path}.")

        return True

    except FileNotFoundError:
        logging.error(
            f"Source file not found during copy: {source_file_path}. It might have been moved or deleted.")
        return False
    except Exception as e:
        logging.error(f"Failed to copy {source_file_path}: {e}")
        # Cleanup: if temp file exists after error, try to delete it
        if temp_dest_path.exists() and not final_dest_path.exists():
            try:
                logging.warning(f"Error during copy. Deleting incomplete temporary file: {temp_dest_path}")
                temp_dest_path.unlink()
            except OSError as clean_e:
                logging.error(f"Could not delete temporary file {temp_dest_path} after error: {clean_e}")
        return False


def main():
    """Main function to run the producer script."""
    setup_logging()
    logging.info("Producer script started.")
    logging.info(f"Metrics source directory: {METRICS_SOURCE_DIR}")
    logging.info(f"Metrics destination directory: {METRICS_DEST_DIR}")
    logging.info(f"Accounting source directory: {ACCOUNTING_SOURCE_DIR}")
    logging.info(f"Accounting destination directory: {ACCOUNTING_DEST_DIR}")
    logging.info(f"Ready signal directory: {READY_SIGNAL_DIR}")
    logging.info(f"Composer ready signal directory: {COMPOSER_READY_DIR}")
    logging.info(f"Max metric files in destination: {MAX_FILES_IN_DESTINATION}")

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if not METRICS_SOURCE_DIR.exists():
        logging.error(f"Metrics source directory {METRICS_SOURCE_DIR} does not exist. Exiting.")
        return

    if not ACCOUNTING_SOURCE_DIR.exists():
        logging.error(f"Accounting source directory {ACCOUNTING_SOURCE_DIR} does not exist. Exiting.")
        return

    # Track the last processed month folder to ensure progress across months
    last_processed_month = None

    # Increase the per-month limit to process all files in a month
    # Remove the arbitrary 10 file limit
    files_per_month_limit = MAX_FILES_IN_DESTINATION  # Process all files in a month up to the max limit

    try:
        while not shutdown_requested:
            # Get a precise count of current destination files before processing
            current_metrics_files = []
            if METRICS_DEST_DIR.exists():
                for month_dir in METRICS_DEST_DIR.iterdir():
                    if month_dir.is_dir():
                        current_metrics_files.extend(
                            [f for f in month_dir.iterdir() if f.is_file() and not f.name.endswith(TEMP_EXTENSION)])

            current_file_count = len(current_metrics_files)
            logging.info(
                f"Current file count in destination: {current_file_count} (limit is {MAX_FILES_IN_DESTINATION})")

            if current_file_count >= MAX_FILES_IN_DESTINATION:
                logging.info(
                    f"Metrics destination has {current_file_count} files "
                    f"(limit is {MAX_FILES_IN_DESTINATION}). Waiting..."
                )
                time.sleep(DEST_FULL_WAIT_SECONDS)
                continue

            # Process files immediately as they're found
            files_processed_this_cycle = 0
            total_folders_checked = 0
            months_processed_this_cycle = 0
            available_slots = MAX_FILES_IN_DESTINATION - current_file_count

            if available_slots <= 0:
                logging.info(f"No available slots to process files. Waiting...")
                time.sleep(DEST_FULL_WAIT_SECONDS)
                continue

            logging.info(f"Available slots for new files: {available_slots}")

            # Iterate through each month folder in the source directory
            try:
                # Get all month folders and sort them
                all_month_folders = sorted([d for d in METRICS_SOURCE_DIR.iterdir() if d.is_dir()])

                if not all_month_folders:
                    logging.info(f"No month folders found in {METRICS_SOURCE_DIR}. Waiting...")
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                # If we have a last processed month, start from the next one
                if last_processed_month:
                    try:
                        last_index = all_month_folders.index(last_processed_month)
                        # Start from the next month, or from beginning if we reached the end
                        if last_index < len(all_month_folders) - 1:
                            month_folders = all_month_folders[last_index + 1:] + all_month_folders[:last_index + 1]
                        else:
                            month_folders = all_month_folders  # Start over from beginning
                        logging.info(f"Continuing from month after {last_processed_month.name}")
                    except ValueError:
                        # Last processed month not found, start from beginning
                        month_folders = all_month_folders
                        logging.info(f"Last processed month {last_processed_month} not found. Starting from beginning.")
                else:
                    month_folders = all_month_folders

                logging.info(f"Found {len(month_folders)} month folders to process")

                for month_folder in month_folders:
                    total_folders_checked += 1
                    if shutdown_requested:
                        break

                    # Track this as our last processed month
                    last_processed_month = month_folder

                    # Get metric files in this month folder, sorted by modification time
                    try:
                        metric_files = sorted(
                            [f for f in month_folder.glob(METRICS_FILE_PATTERN) if f.is_file()],
                            key=os.path.getmtime
                        )
                    except Exception as folder_e:
                        logging.error(f"Error scanning folder {month_folder}: {folder_e}")
                        continue

                    if not metric_files:
                        logging.debug(f"No files found in {month_folder} matching '{METRICS_FILE_PATTERN}'")
                        continue

                    logging.info(f"Found {len(metric_files)} metric files in {month_folder.name}")

                    # Limit how many files we process per month to ensure we don't exceed available slots
                    files_processed_in_this_month = 0
                    month_limit = min(available_slots, files_per_month_limit)

                    if month_limit <= 0:
                        logging.info(
                            f"No available slots left to process files from {month_folder.name}. Stopping cycle.")
                        break

                    # Process each file in this month folder immediately
                    for metric_file in metric_files:
                        if shutdown_requested:
                            break

                        # Check if we've run out of available slots
                        if files_processed_this_cycle >= available_slots:
                            logging.info(f"Reached available slot limit of {available_slots} files this cycle")
                            break

                        # Process the metric file (including its accounting file)
                        if process_metric_file(metric_file):
                            logging.info(f"Successfully processed {metric_file.name} with its accounting file")
                            files_processed_this_cycle += 1
                            files_processed_in_this_month += 1
                            available_slots -= 1  # Decrease available slots as we process files

                            # Check if we've processed enough files from this month
                            if files_processed_in_this_month >= month_limit:
                                logging.info(
                                    f"Processed {files_processed_in_this_month} files from {month_folder.name}, moving to next month")
                                break

                        else:
                            logging.warning(
                                f"Processing failed for {metric_file.name}. Will try again later."
                            )
                            # Short pause after a failure
                            time.sleep(1)

                    # Mark that we processed a month
                    months_processed_this_cycle += 1

                    # Break the folder loop if we've hit the overall file limit
                    if files_processed_this_cycle >= available_slots:
                        break

                # End of cycle info
                if files_processed_this_cycle > 0:
                    logging.info(
                        f"Processed {files_processed_this_cycle} files from {months_processed_this_cycle} months")
                else:
                    # If we checked folders but found no files to process, longer sleep
                    logging.info(f"No new files to process from {total_folders_checked} folders. Waiting...")
                    time.sleep(POLL_INTERVAL_SECONDS)

            except Exception as e:
                logging.error(f"Error scanning metrics source directory {METRICS_SOURCE_DIR}: {e}")
                time.sleep(POLL_INTERVAL_SECONDS / 2)
                continue

    except Exception as e:
        logging.critical(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
    finally:
        logging.info("Producer script shutting down.")


if __name__ == "__main__":
    main()