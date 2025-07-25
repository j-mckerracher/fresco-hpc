#!/usr/bin/env python3
"""
File Processor and CSV Composer for ETL Pipeline

This component monitors the staging directory for parquet files,
processes them, and appends their content to CSV files organized by year and month.
"""

import os
import sys
import logging
import pandas as pd
import time
import re
import shutil
import json
from pathlib import Path
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("file_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# File paths
STAGING_DIR = Path("./file-composer/incoming/staging")  # Monitor this directory for staged parquet files
CSV_OUTPUT_DIR = Path("file-composer/csv-in-process")  # Storage for CSV files
TRACKING_DIR = Path("tracking")  # Directory for tracking processed files

# Configuration
CHECK_INTERVAL = 15  # Check for new files every 15 seconds

# Global termination flag
terminate_requested = False


class ProcessedFileTracker:
    """Tracks which files have been processed to avoid duplicates"""

    def __init__(self, tracking_dir=TRACKING_DIR):
        """Initialize the tracker with the tracking directory"""
        self.tracking_dir = Path(tracking_dir)
        self.tracking_dir.mkdir(exist_ok=True, parents=True)

        # Load processed files from tracking directory
        self.processed_files = self._load_processed_files()

    def _load_processed_files(self):
        """Load the list of processed files from tracking file"""
        processed_files_path = self.tracking_dir / "csv_processed_files.json"
        if processed_files_path.exists():
            try:
                with open(processed_files_path, 'r') as f:
                    return set(json.load(f))
            except Exception as e:
                logger.error(f"Error loading processed files: {str(e)}")
                return set()
        return set()

    def save_processed_files(self):
        """Save the current list of processed files"""
        processed_files_path = self.tracking_dir / "csv_processed_files.json"
        try:
            with open(processed_files_path, 'w') as f:
                json.dump(list(self.processed_files), f)
        except Exception as e:
            logger.error(f"Error saving processed files: {str(e)}")

    def is_processed(self, file_path):
        """Check if a file has already been processed"""
        return str(file_path) in self.processed_files

    def mark_as_processed(self, file_path):
        """Mark a file as processed"""
        self.processed_files.add(str(file_path))
        # Save after each new file to avoid losing tracking data
        self.save_processed_files()


def extract_date_from_filename(filename):
    """Extract year, month, and day from a filename"""
    # Pattern for perf_metrics_YYYY-MM-DD.parquet
    pattern = re.compile(r'perf_metrics_(\d{4})-(\d{2})-(\d{2})\.parquet')
    match = pattern.search(filename)

    if match:
        year, month, day = match.groups()
        return year, month, day

    # Alternative pattern for YYYY_MM_DD format
    alt_pattern = re.compile(r'(\d{4})[-_](\d{2})[-_](\d{2})')
    match = alt_pattern.search(filename)

    if match:
        year, month, day = match.groups()
        return year, month, day

    # If no pattern matches, try to extract just year and month
    year_month_pattern = re.compile(r'(\d{4})[-_](\d{2})')
    match = year_month_pattern.search(filename)

    if match:
        year, month = match.groups()
        # Extract day number from elsewhere in the filename if possible
        day_pattern = re.compile(r'day(\d{1,2})')
        day_match = day_pattern.search(filename)
        day = day_match.group(1).zfill(2) if day_match else "00"
        return year, month, day

    return None, None, None


def get_csv_filename(year, month, day):
    """Generate the CSV filename for a given date"""
    return f"{year}-{month}-{day}.csv"


def convert_parquet_to_csv(parquet_file, csv_file, append=True):
    """Convert a parquet file to CSV and optionally append to existing CSV"""
    try:
        # Read the parquet file
        df = pd.read_parquet(parquet_file)
        logger.info(f"Transforming {parquet_file} to CSV ")

        # Check if the CSV file already exists
        file_exists = os.path.exists(csv_file)

        # If appending and file exists, don't write header
        if append and file_exists:
            df.to_csv(csv_file, mode='a', index=False, header=False)
            logger.info(f"Appended {len(df)} rows to {csv_file}")
        else:
            # Otherwise create a new file with headers
            df.to_csv(csv_file, index=False)
            logger.info(f"Created new CSV file {csv_file} with {len(df)} rows")

        return True
    except Exception as e:
        error_msg = f"Error converting {parquet_file} to CSV: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        return False


def process_parquet_file(parquet_file, file_tracker):
    """Process a single parquet file"""
    try:
        file_path = Path(parquet_file)

        # Skip if already processed
        if file_tracker.is_processed(file_path):
            logger.debug(f"File already processed: {file_path}")
            return True

        # Extract date components from filename
        year, month, day = extract_date_from_filename(file_path.name)

        if not all([year, month, day]):
            logger.warning(f"Could not extract date from filename: {file_path.name}")
            return False

        # Create year-month string and directory
        year_month_dir = f"{year}-{month}"
        output_dir = CSV_OUTPUT_DIR / year_month_dir

        # Create the output directory if it doesn't exist
        output_dir.mkdir(exist_ok=True, parents=True)

        # Generate CSV path
        csv_filename = get_csv_filename(year, month, day)
        csv_path = output_dir / csv_filename

        # Convert and append to CSV
        success = convert_parquet_to_csv(file_path, csv_path)

        if success:
            # Mark file as processed
            file_tracker.mark_as_processed(file_path)

            # Delete the processed parquet file
            try:
                file_path.unlink()
                logger.info(f"Deleted processed file: {file_path}")
            except Exception as e:
                logger.error(f"Error deleting processed file {file_path}: {str(e)}")

            logger.info(f"Successfully processed {file_path} ({year_month_dir} day {day})")
            return True
        else:
            logger.error(f"Failed to process {file_path}")
            return False

    except Exception as e:
        logger.error(f"Error processing parquet file {parquet_file}: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def scan_staging_directory():
    """Scan the staging directory for parquet files"""
    parquet_files = []

    try:
        # Walk through the staging directory
        for root, _, filenames in os.walk(STAGING_DIR):
            for filename in filenames:
                if filename.endswith('.parquet'):
                    file_path = os.path.join(root, filename)
                    parquet_files.append(file_path)
    except Exception as e:
        logger.error(f"Error scanning staging directory: {str(e)}")

    return parquet_files


def process_files(files, file_tracker):
    """Process multiple files sequentially"""
    if not files:
        return [], []

    processed_files = []
    failed_files = []

    for file_path in files:
        try:
            if Path(file_path).exists():
                success = process_parquet_file(file_path, file_tracker)
                if success:
                    processed_files.append(file_path)
                else:
                    failed_files.append(file_path)
            else:
                logger.warning(f"File disappeared before processing: {file_path}")
                failed_files.append(file_path)
        except Exception as e:
            logger.error(f"Exception processing {file_path}: {str(e)}")
            failed_files.append(file_path)

    return processed_files, failed_files


def monitor_staging():
    """Main function to continuously monitor and process files"""
    global terminate_requested

    logger.info("Starting file processor and CSV composer")

    # Initialize file tracker
    file_tracker = ProcessedFileTracker()

    # Create required directories
    STAGING_DIR.mkdir(exist_ok=True, parents=True)
    CSV_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

    logger.info(f"Monitoring staging directory: {STAGING_DIR}")
    logger.info(f"CSV output directory: {CSV_OUTPUT_DIR}")

    try:
        while not terminate_requested:
            # Scan for parquet files
            parquet_files = scan_staging_directory()

            # Filter out already processed files
            new_files = [f for f in parquet_files if not file_tracker.is_processed(f)]

            if new_files:
                logger.info(f"Found {len(new_files)} new parquet files to process")

                # Process files
                processed_files, failed_files = process_files(new_files, file_tracker)

                logger.info(
                    f"Processed {len(processed_files)} files, {len(failed_files)} failed")

            # Wait before checking again
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down")
        terminate_requested = True

    except Exception as e:
        error_msg = f"Unexpected error in monitor_staging: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        terminate_requested = True

    finally:
        # Save tracking data before exiting
        file_tracker.save_processed_files()
        logger.info("File processor and CSV composer shutting down")


if __name__ == "__main__":
    monitor_staging()