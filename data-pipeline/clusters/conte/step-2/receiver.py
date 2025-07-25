import os
import shutil
import time
import logging
from pathlib import Path
import signal
import re
from typing import List, Dict, Tuple

# Configure logging
log_dir = "."
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "file_receiver.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Path configuration
SERVER_SOURCE_DIR = Path(r"U:\projects\conte-to-fresco-etl\step-2\cache\output_consumer")
LOCAL_DEST_DIR = Path(r"<LOCAL_PATH_PLACEHOLDER>\Documents\Conte\conte-transformed-by-step-2")
SIGNAL_DIR = Path(r"U:\projects\conte-to-fresco-etl\step-2\cache\composer_ready")
INPUT_METRICS_DIR = Path(r"U:\projects\conte-to-fresco-etl\step-2\cache\input\metrics")

# Signal status patterns
COMPLETE_PATTERN = r'(\d{4}-\d{2}-\d{2})\.complete'
PROCESSING_PATTERN = r'(\d{4}-\d{2}-\d{2})\.processing'
FAILED_PATTERN = r'(\d{4}-\d{2}-\d{2})\.failed'

# Control variables
POLL_INTERVAL = 30  # seconds
RETRY_INTERVAL = 60  # seconds
MAX_RETRIES = 3
terminate_requested = False


def setup_signal_handlers():
    """Set up handlers for termination signals"""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def signal_handler(sig, frame):
    """Handle termination signals"""
    global terminate_requested
    logger.warning(f"Signal {sig} received. Requesting graceful termination.")
    terminate_requested = True


def find_completed_files() -> List[Tuple[str, Path]]:
    """Find files with .complete signals in the signal directory that haven't been transferred yet"""
    completed_files = []

    # Log all signal files for debugging
    all_signals = list(SIGNAL_DIR.glob("*.complete"))
    logger.info(f"Found {len(all_signals)} .complete signal files: {[f.name for f in all_signals]}")

    # Check for both daily signals (YYYY-MM-DD.complete) and monthly signals (YYYY-MM.complete)
    daily_pattern = r'(\d{4}-\d{2}-\d{2})\.complete'
    monthly_pattern = r'(\d{4}-\d{2})\.complete'

    # Process all complete signals
    for signal_file in all_signals:
        # Try daily pattern first
        daily_match = re.match(daily_pattern, signal_file.name)
        if daily_match:
            date_str = daily_match.group(1)

            # Check if this date already has a .transferred signal
            transferred_signal = SIGNAL_DIR / f"{date_str}.transferred"
            if transferred_signal.exists():
                # Check if the .complete signal is newer than .transferred signal (stale transfer)
                complete_mtime = signal_file.stat().st_mtime
                transferred_mtime = transferred_signal.stat().st_mtime
                
                if complete_mtime > transferred_mtime:
                    logger.info(f"Stale transfer detected for {date_str}: complete signal is newer than transferred signal")
                    # Remove the old transferred signal so we can re-transfer
                    try:
                        transferred_signal.unlink()
                        logger.info(f"Removed stale transferred signal: {transferred_signal}")
                    except Exception as e:
                        logger.warning(f"Could not remove stale transferred signal {transferred_signal}: {e}")
                        continue
                    # Continue processing this file
                else:
                    logger.info(f"Signal {signal_file.name} already has a corresponding .transferred signal. Skipping.")
                    continue

            source_file = SERVER_SOURCE_DIR / f"perf_metrics_{date_str}.parquet"

            logger.info(f"Found daily signal {signal_file.name}, looking for source file: {source_file}")

            if source_file.exists():
                completed_files.append((date_str, source_file))
                logger.info(f"Found matching source file for daily signal: {source_file}")
            else:
                logger.warning(f"Daily signal file {signal_file.name} exists but source file {source_file} not found")
            continue

        # Try monthly pattern if daily doesn't match
        monthly_match = re.match(monthly_pattern, signal_file.name)
        if monthly_match:
            month_str = monthly_match.group(1)

            # Check if this month already has a .transferred signal
            transferred_signal = SIGNAL_DIR / f"{month_str}.transferred"
            if transferred_signal.exists():
                logger.info(f"Signal {signal_file.name} already has a corresponding .transferred signal. Skipping.")
                continue

            # Look for all files in that month (could be multiple days)
            pattern = f"perf_metrics_{month_str}-*.parquet"
            month_files = list(SERVER_SOURCE_DIR.glob(pattern))

            logger.info(f"Found monthly signal {signal_file.name}, looking for source files matching: {pattern}")
            logger.info(f"Found {len(month_files)} matching daily files: {[f.name for f in month_files]}")

            for source_file in month_files:
                # Check if this specific file already has a transferred signal
                file_match = re.search(r'perf_metrics_(\d{4}-\d{2}-\d{2})\.parquet', source_file.name)
                if file_match:
                    date_str = file_match.group(1)

                    # Skip if this specific day already has a transferred signal
                    day_transferred_signal = SIGNAL_DIR / f"{date_str}.transferred"
                    if day_transferred_signal.exists():
                        # Check if the monthly complete signal is newer than the daily transferred signal
                        monthly_complete_mtime = signal_file.stat().st_mtime
                        day_transferred_mtime = day_transferred_signal.stat().st_mtime
                        
                        if monthly_complete_mtime > day_transferred_mtime:
                            logger.info(f"Stale daily transfer detected for {date_str}: monthly complete signal is newer")
                            # Remove the old transferred signal so we can re-transfer
                            try:
                                day_transferred_signal.unlink()
                                logger.info(f"Removed stale daily transferred signal: {day_transferred_signal}")
                            except Exception as e:
                                logger.warning(f"Could not remove stale transferred signal {day_transferred_signal}: {e}")
                                continue
                            # Continue processing this file
                        else:
                            logger.info(f"Daily file {source_file.name} already has a corresponding .transferred signal. Skipping.")
                            continue

                    completed_files.append((date_str, source_file))
                    logger.info(f"Adding source file for monthly signal: {source_file}")

    # Check if network paths are accessible if no files found
    if not completed_files and all_signals:
        if not os.path.exists(str(SERVER_SOURCE_DIR)):
            logger.error(f"Server source directory not accessible: {SERVER_SOURCE_DIR}")
        elif not os.access(str(SERVER_SOURCE_DIR), os.R_OK):
            logger.error(f"No read permission for server source directory: {SERVER_SOURCE_DIR}")

    if completed_files:
        logger.info(f"Found {len(completed_files)} files ready for transfer")
    else:
        logger.info("No files ready for transfer")

    return completed_files


def find_input_file(date_str: str) -> List[Path]:
    """Find the corresponding input file(s) for a given date string"""
    year, month, day = date_str.split('-')
    input_dir = INPUT_METRICS_DIR / f"{year}-{month}"

    if not input_dir.exists():
        logger.warning(f"Input directory {input_dir} not found for date {date_str}")
        return []

    # Look for input files matching the date pattern
    pattern = f"*{date_str}*.parquet"
    input_files = list(input_dir.glob(pattern))

    if not input_files:
        logger.warning(f"No input files found matching {pattern} in {input_dir}")
    else:
        logger.debug(f"Found {len(input_files)} input file(s) for {date_str}: {[f.name for f in input_files]}")

    return input_files


def cleanup_input_files(date_str: str) -> bool:
    """Delete the corresponding input files after successful transfer"""
    input_files = find_input_file(date_str)

    if not input_files:
        return False

    success = True
    for input_file in input_files:
        try:
            logger.info(f"Removing input file: {input_file}")
            input_file.unlink()
            logger.info(f"Successfully removed input file: {input_file.name}")
        except Exception as e:
            logger.error(f"Failed to remove input file {input_file}: {e}")
            success = False

    return success


def transfer_file(source_path: Path, date_str: str) -> bool:
    """Transfer a file from server to local destination and cleanup input files and signals"""
    dest_path = LOCAL_DEST_DIR / source_path.name
    temp_dest_path = LOCAL_DEST_DIR / f"temp_{source_path.name}"

    logger.info(f"Transferring file: {source_path} to {dest_path}")

    try:
        # Create destination directory if needed
        LOCAL_DEST_DIR.mkdir(parents=True, exist_ok=True)

        # Check if file already exists at destination
        if dest_path.exists():
            logger.info(f"File {dest_path} already exists at destination.")
            # Even if file exists, we should still cleanup input files and signals
            cleanup_success = cleanup_input_files(date_str)
            if cleanup_success:
                logger.info(f"Successfully cleaned up input files for {date_str}")

            # Create transferred signal
            transferred_signal = SIGNAL_DIR / f"{date_str}.transferred"
            transferred_signal.touch()
            logger.info(f"Created transferred signal: {transferred_signal}")

            # Remove the complete signal file
            complete_signal = SIGNAL_DIR / f"{date_str}.complete"
            if complete_signal.exists():
                try:
                    complete_signal.unlink()
                    logger.info(f"Removed complete signal file: {complete_signal}")
                except Exception as e:
                    logger.warning(f"Could not remove complete signal file {complete_signal}: {e}")

            # Also remove any monthly signal that might exist for this date
            month_str = date_str.rsplit('-', 1)[0]  # Extract YYYY-MM from YYYY-MM-DD
            month_signal = SIGNAL_DIR / f"{month_str}.complete"
            if month_signal.exists():
                # Only remove the monthly signal if there are no more daily files for this month
                remaining_files = list(SERVER_SOURCE_DIR.glob(f"perf_metrics_{month_str}-*.parquet"))
                if not remaining_files:
                    try:
                        month_signal.unlink()
                        logger.info(
                            f"Removed monthly complete signal file: {month_signal} (no more files for this month)")
                    except Exception as e:
                        logger.warning(f"Could not remove monthly complete signal file {month_signal}: {e}")

            # Also remove the output file from the server to save space
            try:
                logger.info(f"Removing output file from server: {source_path}")
                source_path.unlink()
                logger.info(f"Successfully removed server output file: {source_path.name}")
            except Exception as e:
                logger.warning(f"Could not remove server output file {source_path}: {e}")

            return True

        # Copy to temporary file first
        shutil.copy2(source_path, temp_dest_path)

        # Move temp file to final name (atomic operation)
        os.rename(temp_dest_path, dest_path)

        logger.info(f"Successfully transferred {source_path.name} to local destination")

        # Cleanup input files
        cleanup_success = cleanup_input_files(date_str)
        if cleanup_success:
            logger.info(f"Successfully cleaned up input files for {date_str}")

        # Create transferred signal
        transferred_signal = SIGNAL_DIR / f"{date_str}.transferred"
        transferred_signal.touch()
        logger.info(f"Created transferred signal: {transferred_signal}")

        # Remove the complete signal file
        complete_signal = SIGNAL_DIR / f"{date_str}.complete"
        if complete_signal.exists():
            try:
                complete_signal.unlink()
                logger.info(f"Removed complete signal file: {complete_signal}")
            except Exception as e:
                logger.warning(f"Could not remove complete signal file {complete_signal}: {e}")

        # Also remove any monthly signal that might exist for this date
        month_str = date_str.rsplit('-', 1)[0]  # Extract YYYY-MM from YYYY-MM-DD
        month_signal = SIGNAL_DIR / f"{month_str}.complete"
        if month_signal.exists():
            # Only remove the monthly signal if there are no more daily files for this month
            remaining_files = list(SERVER_SOURCE_DIR.glob(f"perf_metrics_{month_str}-*.parquet"))
            if not remaining_files:
                try:
                    month_signal.unlink()
                    logger.info(f"Removed monthly complete signal file: {month_signal} (no more files for this month)")
                except Exception as e:
                    logger.warning(f"Could not remove monthly complete signal file {month_signal}: {e}")

        # Also remove the output file from the server to save space
        try:
            logger.info(f"Removing output file from server: {source_path}")
            source_path.unlink()
            logger.info(f"Successfully removed server output file: {source_path.name}")
        except Exception as e:
            logger.warning(f"Could not remove server output file {source_path}: {e}")

        return True

    except Exception as e:
        logger.error(f"Failed to transfer {source_path}: {e}")

        # Clean up temp file if it exists
        if temp_dest_path.exists():
            try:
                temp_dest_path.unlink()
                logger.debug(f"Cleaned up temp file {temp_dest_path}")
            except Exception as cleanup_e:
                logger.warning(f"Failed to clean up temp file {temp_dest_path}: {cleanup_e}")

        return False


def main_loop():
    """Main processing loop"""
    global terminate_requested

    logger.info("File receiver started. Watching for completed files to transfer.")

    retry_counter = {}  # Track retry attempts for each file

    while not terminate_requested:
        try:
            # Find files with 'complete' signals
            completed_files = find_completed_files()

            if completed_files:
                logger.info(f"Found {len(completed_files)} completed files to transfer")

                for date_str, source_path in completed_files:
                    if terminate_requested:
                        break

                    file_key = str(source_path)

                    # Check if we've exceeded retry limit
                    if file_key in retry_counter and retry_counter[file_key] >= MAX_RETRIES:
                        logger.warning(f"Maximum retries ({MAX_RETRIES}) reached for {source_path.name}. Skipping.")

                        # Create a failed transfer signal
                        failed_signal = SIGNAL_DIR / f"{date_str}.transfer_failed"
                        failed_signal.write_text(f"Failed after {MAX_RETRIES} attempts")

                        # Remove from retry counter to avoid indefinite skipping
                        del retry_counter[file_key]
                        continue

                    # Attempt transfer
                    success = transfer_file(source_path, date_str)

                    if success:
                        if file_key in retry_counter:
                            del retry_counter[file_key]
                    else:
                        # Increment retry counter
                        retry_counter[file_key] = retry_counter.get(file_key, 0) + 1
                        logger.info(
                            f"Will retry {source_path.name} later (attempt {retry_counter[file_key]}/{MAX_RETRIES})")
            else:
                logger.debug("No new completed files found. Waiting...")

            # Sleep before next check
            time.sleep(POLL_INTERVAL)

        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(RETRY_INTERVAL)


if __name__ == "__main__":
    setup_signal_handlers()
    logger.info("Receiver script starting up")

    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("Receiver interrupted by KeyboardInterrupt")
    finally:
        logger.info("Receiver shutting down")