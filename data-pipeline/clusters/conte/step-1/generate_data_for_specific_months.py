import json
import re
from builtins import int, str
from pathlib import Path
import shutil
import threading
from queue import Queue
import time
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import os
import gc
from typing import Optional, Dict, Callable, Union, List
import logging
from datetime import datetime

"""
FRESCO Dataset Transformation Functions
======================================

This module contains the data transformation functions used to convert HPC cluster
monitoring data (specifically from Conte, Stampede, and Anvil clusters) into the
standardized FRESCO dataset format. The FRESCO dataset captures resource usage
metrics of HPC workloads across multiple hosts with detailed time-series information.

Each function in this module processes a specific type of resource metric:
- Block I/O performance (disk access rates)
- CPU utilization (percentage in user mode)
- Memory usage (both total and excluding disk cache)
- Network File System (NFS) transfer rates

Assumptions and Implementation Notes:
------------------------------------
1. Sector size: All block I/O calculations assume 512-byte sectors, which is the traditional
   sector size used in many filesystems. Modern hardware may use 4KB (or larger) physical
   sectors, but logical sectors are often still reported as 512 bytes.

2. Time intervals: Rate calculations require at least a 0.1-second interval to prevent
   anomalies from extremely small time deltas.

3. Data filtering: Records with missing or invalid data in critical fields are excluded
   to maintain data integrity. Typically, 1-5% of raw data points may be filtered out.

4. Timestamp format: Input timestamps are expected in '%m/%d/%Y %H:%M:%S' format.
   If timestamps contain timezone information, it should be consistent across all inputs.

Unit Conversions:
----------------
- Memory: Bytes → GB (divide by 1024³)
- Block I/O: Sectors → GB/s (multiply by 512, divide by ticks, then by 1024³)
- NFS: Bytes → MB/s (divide by time delta, then by 1024²)

"""


# Configure logging
def setup_logging():
    """Set up logging configuration to write to file with timestamps."""
    # Create logs directory if it doesn't exist
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"transformer_{timestamp}.log")

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()  # Also log to console
        ]
    )

    # Log the log file location
    logging.info(f"Logging started. Log file: {log_filename}")
    return log_filename


# Set up logging at module level
LOG_FILE = setup_logging()
logger = logging.getLogger(__name__)


# --- Existing data processing functions (read_csv_with_fallback_encoding, safe_division, etc.) ---
# --- process_block_file, process_cpu_file, process_mem_file, process_nfs_file ---
# --- safe_parse_timestamp, ThreadedDownloader, download_folder_threaded ---
# --- ProcessingTracker, DataVersionManager, get_folder_urls ---
# --- process_folder_data, check_disk_space, logger.info_progress ---
# --- [No changes needed in the functions above this point] ---

def read_csv_with_fallback_encoding(file_path, chunk_size=None, **kwargs):
    """
    Attempt to read a CSV file with multiple encodings, falling back if one fails.

    This function addresses the common issue of unknown or incorrect file encodings
    in large datasets by trying multiple encodings in sequence. It starts with more
    specific encodings and falls back to more permissive ones that can handle a wider
    range of byte values.

    Args:
        file_path: Path to the CSV file
        chunk_size: If not None, read file in chunks of this size
        **kwargs: Additional arguments to pass to pd.read_csv

    Returns:
        DataFrame or TextFileReader if using chunks

    Raises:
        ValueError: If all encoding attempts fail
    """
    # Try these encodings in order - latin1 rarely fails as it can read any byte
    encodings = ['latin1', 'ISO-8859-1', 'utf-8']

    # For the last attempt with utf-8, we'll use error handling
    errors = [None, None, 'replace']

    last_exception = None

    for i, encoding in enumerate(encodings):
        try:
            error_param = errors[i]
            encoding_kwargs = {'encoding': encoding}

            # Add error handling if specified
            if error_param:
                encoding_kwargs['encoding_errors'] = error_param

            # Add error_bad_lines=False to skip bad lines (parameter name depends on pandas version)
            try:
                # For newer pandas versions (1.3+)
                combined_kwargs = {**encoding_kwargs, 'on_bad_lines': 'skip', **kwargs}
            except TypeError:
                # For older pandas versions
                combined_kwargs = {**encoding_kwargs, 'error_bad_lines': False, **kwargs}

            if chunk_size is not None:
                # Ensure low_memory=False is set for chunked reading to avoid dtype warnings
                combined_kwargs['low_memory'] = False
                return pd.read_csv(file_path, chunksize=chunk_size, **combined_kwargs)
            else:
                return pd.read_csv(file_path, **combined_kwargs)

        except UnicodeDecodeError as e:
            last_exception = e
            logger.info(f"Encoding {encoding} failed for {file_path}, trying next encoding...")
            continue
        except Exception as e:
            # If it's not an encoding error, re-raise
            logger.info(f"Error reading file {file_path} with {encoding} encoding: {str(e)}")
            raise

    # If we get here, all encodings failed
    raise ValueError(f"Failed to read file {file_path} with any encoding: {last_exception}")


def safe_division(numerator, denominator, default: float = 0.0) -> float:
    """
    Safely perform division with comprehensive error handling.

    This function prevents division by zero errors and handles
    other exceptions that might occur during division operations.

    Mathematical formula:
        result = numerator / denominator if denominator != 0 else default

    Args:
        numerator: The division numerator
        denominator: The division denominator
        default: The value to return if division cannot be performed (default: 0.0)

    Returns:
        The result of division or the default value if division fails
    """
    try:
        # Check for NaN or infinite values before division
        if not np.isfinite(numerator) or not np.isfinite(denominator):
            return default
        # Proceed with division if denominator is finite and non-zero
        return numerator / denominator if denominator != 0 else default
    except (TypeError, ValueError):
        # Handle cases where inputs might not be numeric
        return default
    except Exception:
        # Catch any other unexpected errors during division
        return default


def validate_metric(value: float, min_val: float = 0.0, max_val: float = float('inf')) -> float:
    """
    Ensure metric values are within a valid range.

    Many resource metrics should be non-negative (e.g., memory usage),
    and some have natural upper bounds (e.g., CPU percentage <= 100).
    This function ensures values stay within appropriate bounds.
    Handles non-numeric or NaN inputs gracefully.

    Args:
        value: The metric value to validate
        min_val: Minimum allowed value (default: 0.0)
        max_val: Maximum allowed value (default: infinity)

    Returns:
        The value clamped to the specified range, or min_val if input is invalid
    """
    try:
        # Attempt conversion to float and check for NaN/infinity
        val_float = float(value)
        if not np.isfinite(val_float):
            return min_val  # Default to min_val if not finite
        # Use np.clip for efficient clamping
        return np.clip(val_float, min_val, max_val)
    except (ValueError, TypeError):
        # Handle cases where value cannot be converted to float
        return min_val


def calculate_rate(current_value: float, previous_value: float,
                   time_delta_seconds: float) -> float:
    """
    Calculate rate of change per second.

    Used for metrics like I/O or network throughput where
    we need to determine change over time. Handles potential non-numeric inputs.

    Mathematical formula:
        rate = (current_value - previous_value) / time_delta_seconds if time_delta_seconds > 0 else 0

    Args:
        current_value: Current metric value
        previous_value: Previous metric value
        time_delta_seconds: Time interval in seconds

    Returns:
        Rate of change per second, or 0.0 if calculation is not possible
    """
    try:
        # Ensure inputs are finite numeric values
        if not all(np.isfinite([current_value, previous_value, time_delta_seconds])):
            return 0.0
        # Use safe_division to handle potential division by zero or small time deltas
        # Ensure time delta is positive before calculating rate
        if time_delta_seconds <= 0:
            return 0.0
        diff = current_value - previous_value
        # Only calculate rate if the difference is non-negative (for cumulative counters)
        # Or simply calculate the rate regardless of sign if it can represent decrease
        # Assuming cumulative counters here, so rate should be non-negative
        if diff < 0:
            # Reset or unusual counter behavior, treat as zero rate for this interval
            return 0.0

        return safe_division(diff, time_delta_seconds, default=0.0)

    except (TypeError, ValueError):
        # Handle cases where inputs might not be numeric
        return 0.0


def process_block_file(input_data: Union[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Process block.csv file to extract disk I/O throughput rate (GB/s) at the node level,
    using original timestamps.

    Calculates rate per device based on cumulative sectors over time intervals defined
    by original timestamps. Then, aggregates these device rates to the node level by
    summing rates from devices that report at the exact same original timestamp for the same node.
    """
    df = None
    df_valid_rates = None
    agg_df = None
    try:
        if isinstance(input_data, str):
            file_path = input_data
            logger.info(f"Processing block file: {file_path}")
            try:
                dtype_spec = {'rd_sectors': float, 'wr_sectors': float, 'jobID': str, 'node': str, 'device': str,
                              'timestamp': str}
                df = read_csv_with_fallback_encoding(file_path, dtype=dtype_spec, low_memory=False)
            except Exception as e:
                logger.info(f"Error reading block file {file_path}: {str(e)}")
                return pd.DataFrame()
        elif isinstance(input_data, pd.DataFrame):
            df = input_data.copy()
            logger.info("Processing block DataFrame.")
        else:
            logger.info(f"Invalid input type for process_block_file: {type(input_data)}")
            return pd.DataFrame()

        required_columns = ['rd_sectors', 'wr_sectors', 'jobID', 'node', 'device', 'timestamp']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.info(f"Missing required columns in block data: {missing_cols}")
            return pd.DataFrame()

        initial_row_count = len(df)
        numeric_cols = ['rd_sectors', 'wr_sectors']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=required_columns)  # Includes device and timestamp
        filtered_rows = initial_row_count - len(df)
        if filtered_rows > 0:
            logger.info(
                f"Dropped {filtered_rows} rows ({safe_division(filtered_rows, initial_row_count) * 100:.2f}%) with missing/invalid data in block file")

        if df.empty:
            logger.info("No valid data rows after filtering NaNs in block file")
            return pd.DataFrame()

        df['jobID'] = df['jobID'].fillna('unknown').astype(str).str.replace('jobID', 'JOB', case=False)
        df['node'] = df['node'].fillna('unknown').astype(str)
        df['device'] = df['device'].fillna('unknown').astype(str)

        try:
            df['Timestamp_original'] = pd.to_datetime(df['timestamp'], format='%m/%d/%Y %H:%M:%S', errors='coerce')
            df = df.dropna(subset=['Timestamp_original'])
            if df.empty:
                logger.info("No valid block data rows after timestamp filtering")
                return pd.DataFrame()
            sort_keys = ['jobID', 'node', 'device', 'Timestamp_original']
            logger.info(f"Sorting block data by {sort_keys}...")
            df = df.sort_values(sort_keys)
            logger.info("Sorting complete.")
        except Exception as e:
            logger.info(f"Error parsing or sorting timestamps in block file: {str(e)}")
            return pd.DataFrame()

        logger.info("Calculating per-device block I/O rate using diff...")
        SECTOR_SIZE_BYTES = 512
        BYTES_TO_GB = 1 / (1024 ** 3)
        MIN_TIME_DELTA = 0.1

        group_keys_device = ['jobID', 'node', 'device']
        grouped_device = df.groupby(group_keys_device, observed=True)

        df['time_delta_seconds'] = grouped_device['Timestamp_original'].diff().dt.total_seconds()
        df['total_sectors'] = df['rd_sectors'] + df['wr_sectors']
        df['sector_delta'] = grouped_device['total_sectors'].diff()

        df['Value_device_rate'] = 0.0
        valid_rate_mask = (
                df['time_delta_seconds'].notna() &
                (df['time_delta_seconds'] >= MIN_TIME_DELTA) &
                df['sector_delta'].notna() &
                (df['sector_delta'] >= 0)
        )
        bytes_delta = df.loc[valid_rate_mask, 'sector_delta'] * SECTOR_SIZE_BYTES
        df.loc[valid_rate_mask, 'Value_device_rate'] = (bytes_delta / df.loc[
            valid_rate_mask, 'time_delta_seconds']) * BYTES_TO_GB
        df['Value_device_rate'] = df['Value_device_rate'].clip(lower=0)
        logger.info("Per-device rate calculation complete.")

        df_valid_rates = df[valid_rate_mask].copy()
        if df_valid_rates.empty:
            logger.info("No valid per-device block rates calculated.")
            return pd.DataFrame()

        logger.info("Aggregating device-level block rates to node level using original timestamps...")
        node_agg_keys = ['jobID', 'node', 'Timestamp_original']
        agg_df = df_valid_rates.groupby(node_agg_keys, observed=True, as_index=False)['Value_device_rate'].sum()
        agg_df.rename(columns={'Value_device_rate': 'Value'}, inplace=True)

        if agg_df.empty:
            logger.info("No valid data after node-level aggregation for block file.")
            return pd.DataFrame()
        logger.info(f"Node-level block aggregation complete. Result has {len(agg_df)} rows.")

        fresco_df = pd.DataFrame({
            'Job Id': agg_df['jobID'],
            'Host': agg_df['node'],
            'Event': 'block',
            'Value': agg_df['Value'],
            'Units': 'GB/s',
            'Timestamp': agg_df['Timestamp_original']
        })

        logger.info(f"Successfully processed block data: {len(fresco_df)} node-level rows created.")
        return fresco_df

    except Exception as e:
        logger.error(f"An unexpected error occurred in process_block_file: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()
    finally:
        del df
        if df_valid_rates is not None: del df_valid_rates
        if agg_df is not None: del agg_df
        gc.collect()


def process_cpu_file(input_data: Union[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Process cpu.csv file to extract node-level CPU user percentage.

    Aggregates raw per-core CPU jiffy/tick counters to the node level for each
    original timestamp. Then, calculates the change (delta) in these counters
    between consecutive timestamps for each node. Finally, computes the 'cpuuser'
    percentage based on the delta of user time relative to the delta of total time.

    Mathematical formula (on node-level, time-delta values):
        user_jiffies_delta_node = current_total_user_jiffies_on_node - previous_total_user_jiffies_on_node
        total_jiffies_delta_node = sum_of_deltas_for_all_cpu_states_on_node
        cpuuser % = (user_jiffies_delta_node / total_jiffies_delta_node) * 100
                   if total_jiffies_delta_node > 0 else 0

    Args:
        input_data: Either a file path to the cpu.csv file or a DataFrame
                   containing the raw per-core CPU utilization data.

    Returns:
        DataFrame formatted according to the FRESCO schema with node-level
        'cpuuser' metrics, using original timestamps.
    """
    df = None
    node_level_jiffies = None  # Define for finally block
    try:
        if isinstance(input_data, str):
            file_path = input_data
            logger.info(f"Processing CPU file: {file_path}")
            try:
                cpu_jiffy_columns_read = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq']
                dtype_spec = {col: float for col in cpu_jiffy_columns_read}
                dtype_spec.update({'jobID': str, 'node': str, 'device': str, 'timestamp': str})
                df = read_csv_with_fallback_encoding(file_path, dtype=dtype_spec, low_memory=False)
            except Exception as e:
                logger.info(f"Error reading CPU file {file_path}: {str(e)}")
                return pd.DataFrame()
        elif isinstance(input_data, pd.DataFrame):
            df = input_data.copy()
            logger.info("Processing CPU DataFrame.")
        else:
            logger.info(f"Invalid input type for process_cpu_file: {type(input_data)}")
            return pd.DataFrame()

        cpu_jiffy_columns = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq']
        id_cols_for_check = ['jobID', 'node', 'device', 'timestamp']
        required_columns = cpu_jiffy_columns + id_cols_for_check
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.info(f"Missing required columns in CPU data: {missing_cols}")
            return pd.DataFrame()

        initial_row_count = len(df)
        for col in cpu_jiffy_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=required_columns)
        filtered_rows = initial_row_count - len(df)
        if filtered_rows > 0:
            logger.info(
                f"Dropped {filtered_rows} rows ({safe_division(filtered_rows, initial_row_count) * 100:.2f}%) with missing/invalid data in CPU file")

        if df.empty:
            logger.info("No valid data rows after initial filtering NaNs in CPU file")
            return pd.DataFrame()

        df['jobID'] = df['jobID'].fillna('unknown').astype(str).str.replace('jobID', 'JOB', case=False)
        df['node'] = df['node'].fillna('unknown').astype(str)
        df['device'] = df['device'].fillna('unknown').astype(str)

        try:
            df['Timestamp_original'] = pd.to_datetime(df['timestamp'], format='%m/%d/%Y %H:%M:%S', errors='coerce')
            df = df.dropna(subset=['Timestamp_original'])
        except Exception as e:
            logger.info(f"Error parsing timestamps in CPU file: {str(e)}")
            return pd.DataFrame()

        if df.empty:
            logger.info("No valid CPU data rows after timestamp filtering")
            return pd.DataFrame()

        logger.info(f"Aggregating raw per-core jiffies to node level by jobID, node, and original timestamp...")
        node_level_jiffies = df.groupby(
            ['jobID', 'node', 'Timestamp_original'], as_index=False
        )[cpu_jiffy_columns].sum()
        logger.info(f"Node-level raw jiffy aggregation complete. Result has {len(node_level_jiffies)} rows.")

        if node_level_jiffies.empty:
            logger.info("Node-level jiffy aggregation resulted in an empty DataFrame.")
            return pd.DataFrame()

        logger.info("Calculating deltas for node-level jiffies...")
        node_level_jiffies = node_level_jiffies.sort_values(['jobID', 'node', 'Timestamp_original'])

        delta_col_names = []
        for col in cpu_jiffy_columns:
            delta_col_name = f'{col}_node_delta'
            node_level_jiffies[delta_col_name] = node_level_jiffies.groupby(
                ['jobID', 'node']
            )[col].diff()
            delta_col_names.append(delta_col_name)

        node_level_jiffies['total_jiffies_node_delta'] = node_level_jiffies[delta_col_names].sum(axis=1)
        logger.info("Delta calculation complete.")

        # Calculate 'cpuuser'
        node_level_jiffies['Value_cpuuser'] = safe_division(
            node_level_jiffies['user_node_delta'],  # Uses the 'user_node_delta' column
            node_level_jiffies['total_jiffies_node_delta']
        ) * 100
        node_level_jiffies['Value_cpuuser'] = node_level_jiffies['Value_cpuuser'].apply(
            lambda x: validate_metric(x, min_val=0.0, max_val=100.0)
        )

        initial_rows_after_delta_calc = len(node_level_jiffies)
        # Drop rows where deltas are NaN (first entry for each group) or total_jiffies_node_delta is not positive
        node_level_jiffies = node_level_jiffies.dropna(subset=['user_node_delta', 'total_jiffies_node_delta'])
        node_level_jiffies = node_level_jiffies[node_level_jiffies['total_jiffies_node_delta'] > 0]

        filtered_rows_after_delta = initial_rows_after_delta_calc - len(node_level_jiffies)
        if filtered_rows_after_delta > 0:
            logger.info(
                f"Dropped {filtered_rows_after_delta} rows due to NaN/invalid deltas for CPU 'cpuuser' percentage calculation.")

        if node_level_jiffies.empty:
            logger.info("No valid data remaining after CPU 'cpuuser' percentage calculation.")
            return pd.DataFrame()

        fresco_df = pd.DataFrame({
            'Job Id': node_level_jiffies['jobID'],
            'Host': node_level_jiffies['node'],
            'Event': 'cpuuser',
            'Value': node_level_jiffies['Value_cpuuser'],
            'Units': 'CPU %',
            'Timestamp': node_level_jiffies['Timestamp_original']
        })

        logger.info(f"Successfully processed CPU data for 'cpuuser': {len(fresco_df)} rows created.")
        return fresco_df

    except Exception as e:
        logger.error(f"An unexpected error occurred in process_cpu_file: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()
    finally:
        del df
        if node_level_jiffies is not None:  # Check if it was assigned
            del node_level_jiffies
        gc.collect()


def process_mem_file(input_data: Union[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Process mem.csv file (assuming Byte units) to extract memory usage metrics,
    using original timestamps.

    Calculates two memory metrics, converting from Bytes to Gibibytes (GiB):
    1. Total memory used (memused)
    2. Memory used excluding disk cache (memused_minus_diskcache)
    This function already uses original timestamps and its core logic remains
    largely unchanged, other than standardizing 'Timestamp_original' if needed for
    internal consistency (though output 'Timestamp' is fine).
    """
    df = None
    try:
        if isinstance(input_data, str):
            file_path = input_data
            logger.info(f"Processing memory file: {file_path}")
            try:
                mem_cols_to_read = ['MemTotal', 'MemFree', 'FilePages']
                dtype_spec = {col: float for col in mem_cols_to_read}
                dtype_spec.update({'jobID': str, 'node': str, 'timestamp': str})
                df = read_csv_with_fallback_encoding(file_path, dtype=dtype_spec, low_memory=False)
            except Exception as e:
                logger.info(f"Error reading memory file {file_path}: {str(e)}")
                return pd.DataFrame()
        elif isinstance(input_data, pd.DataFrame):
            df = input_data.copy()
            logger.info("Processing memory DataFrame.")
        else:
            logger.info(f"Invalid input type for process_mem_file: {type(input_data)}")
            return pd.DataFrame()

        required_columns = ['MemTotal', 'MemFree', 'FilePages', 'jobID', 'node', 'timestamp']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.info(f"Missing required columns in memory data: {missing_cols}")
            return pd.DataFrame()

        initial_row_count = len(df)
        mem_cols_numeric = ['MemTotal', 'MemFree', 'FilePages']
        for col in mem_cols_numeric:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=required_columns)
        filtered_rows = initial_row_count - len(df)
        if filtered_rows > 0:
            logger.info(
                f"Dropped {filtered_rows} rows ({safe_division(filtered_rows, initial_row_count) * 100:.2f}%) with missing/invalid data in memory file")

        if df.empty:
            logger.info("No valid data rows after filtering NaNs in memory file")
            return pd.DataFrame()

        for col in mem_cols_numeric:  # Ensure non-negative after NaN drop
            df[col] = df[col].clip(lower=0)
        df['MemFree'] = df[['MemFree', 'MemTotal']].min(axis=1)
        memory_used_bytes = df['MemTotal'] - df['MemFree']
        df['FilePages'] = df[['FilePages', 'MemTotal']].min(axis=1)
        df['FilePages'] = np.minimum(df['FilePages'],
                                     memory_used_bytes)  # Use np.minimum for element-wise minimum with series

        BYTES_TO_GB = 1 / (1024 ** 3)
        df['memused_value'] = (memory_used_bytes.astype(float) * BYTES_TO_GB).clip(lower=0)
        mem_minus_cache_bytes = memory_used_bytes - df['FilePages']
        df['memused_minus_diskcache_value'] = (mem_minus_cache_bytes.astype(float) * BYTES_TO_GB).clip(lower=0)

        df['jobID'] = df['jobID'].fillna('unknown').astype(str).str.replace('jobID', 'JOB', case=False)
        df['node'] = df['node'].fillna('unknown').astype(str)

        try:
            # Using 'Timestamp_original' for internal consistency if other functions adopt it,
            # but the output column will be 'Timestamp'.
            df['Timestamp_original_parsed'] = pd.to_datetime(df['timestamp'], format='%m/%d/%Y %H:%M:%S',
                                                             errors='coerce')
            df = df.dropna(subset=['Timestamp_original_parsed'])
        except Exception as e:
            logger.info(f"Error parsing timestamps in memory file: {str(e)}")
            return pd.DataFrame()

        if df.empty:
            logger.info("No valid data rows after timestamp filtering in memory file")
            return pd.DataFrame()

        memused_df = pd.DataFrame({
            'Job Id': df['jobID'],
            'Host': df['node'],
            'Event': 'memused',
            'Value': df['memused_value'],
            'Units': 'GB',
            'Timestamp': df['Timestamp_original_parsed']  # Outputting the original parsed timestamp
        })

        memused_minus_diskcache_df = pd.DataFrame({
            'Job Id': df['jobID'],
            'Host': df['node'],
            'Event': 'memused_minus_diskcache',
            'Value': df['memused_minus_diskcache_value'],
            'Units': 'GB',
            'Timestamp': df['Timestamp_original_parsed']  # Outputting the original parsed timestamp
        })

        final_df = pd.concat([memused_df, memused_minus_diskcache_df], ignore_index=True)
        logger.info(f"Successfully processed memory data: {len(final_df)} rows created.")
        return final_df

    except Exception as e:
        logger.error(f"An unexpected error occurred in process_mem_file: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()
    finally:
        del df
        # Explicitly delete other DataFrames created if they exist in locals()
        if 'memused_df' in locals(): del memused_df
        if 'memused_minus_diskcache_df' in locals(): del memused_minus_diskcache_df
        gc.collect()


def process_nfs_file(input_data: Union[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Process llite.csv file to extract NFS transfer rate metrics at node level,
    using original timestamps.

    Calculates rate of data transfer per original time interval. If data involves
    multiple devices/streams per node, their rates are summed if they occur at the
    exact same original timestamp for the same node.
    """
    df = None
    df_valid_rates = None
    agg_df = None
    try:
        if isinstance(input_data, str):
            file_path = input_data
            logger.info(f"Processing NFS/llite file: {file_path}")
            try:
                dtype_spec = {'read_bytes': float, 'write_bytes': float, 'jobID': str, 'node': str, 'timestamp': str}
                # Check if 'device' column exists to include it in dtype_spec if necessary
                # For simplicity, assuming 'device' is not primarily used for llite node aggregation,
                # but if it were, it would be handled similarly to block.csv.
                # temp_df_cols = pd.read_csv(file_path, nrows=0).columns # More robust check if needed
                # if 'device' in temp_df_cols:
                #     dtype_spec['device'] = str
                df = read_csv_with_fallback_encoding(file_path, dtype=dtype_spec, low_memory=False)
            except Exception as e:
                logger.info(f"Error reading NFS/llite file {file_path}: {str(e)}")
                return pd.DataFrame()
        elif isinstance(input_data, pd.DataFrame):
            df = input_data.copy()
            logger.info("Processing NFS/llite DataFrame.")
        else:
            logger.info(f"Invalid input type for process_nfs_file: {type(input_data)}")
            return pd.DataFrame()

        required_columns = ['read_bytes', 'write_bytes', 'jobID', 'node', 'timestamp']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.info(f"Missing required columns in NFS/llite data: {missing_cols}")
            return pd.DataFrame()

        initial_row_count = len(df)
        byte_cols = ['read_bytes', 'write_bytes']
        for col in byte_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=required_columns)
        filtered_rows = initial_row_count - len(df)
        if filtered_rows > 0:
            logger.info(
                f"Dropped {filtered_rows} rows ({safe_division(filtered_rows, initial_row_count) * 100:.2f}%) with missing/invalid data in NFS/llite file")

        if df.empty:
            logger.info("No valid data rows after initial filtering in NFS/llite file")
            return pd.DataFrame()

        df['jobID'] = df['jobID'].fillna('unknown').astype(str).str.replace('jobID', 'JOB', case=False)
        df['node'] = df['node'].fillna('unknown').astype(str)

        try:
            df['Timestamp_original'] = pd.to_datetime(df['timestamp'], format='%m/%d/%Y %H:%M:%S', errors='coerce')
            df = df.dropna(subset=['Timestamp_original'])
            if df.empty:
                logger.info("No valid data rows after timestamp filtering in NFS/llite file")
                return pd.DataFrame()

            # Sort by job, node, and original timestamp for correct diff calculation.
            # If 'device' were relevant for llite rates (e.g. multiple NFS mounts tracked separately),
            # it would be included here, similar to block.csv.
            sort_keys = ['jobID', 'node', 'Timestamp_original']
            # if 'device' in df.columns: sort_keys.insert(2, 'device') # If device matters
            df = df.sort_values(sort_keys)
            logger.info("Sorting complete for NFS data.")
        except Exception as e:
            logger.info(f"Error parsing or sorting timestamps in NFS/llite file: {str(e)}")
            return pd.DataFrame()

        logger.info("Calculating NFS transfer rates using original timestamps...")
        df['total_bytes'] = df['read_bytes'] + df['write_bytes']

        # Group by job and node (and device if relevant for llite data structure)
        group_keys_diff = ['jobID', 'node']
        # if 'device' in df.columns and 'device' in sort_keys: group_keys_diff.append('device')

        grouped_diff = df.groupby(group_keys_diff, observed=True)
        df['time_delta_seconds'] = grouped_diff['Timestamp_original'].diff().dt.total_seconds()
        df['byte_delta'] = grouped_diff['total_bytes'].diff()

        df['Value_rate'] = 0.0
        BYTES_TO_MB = 1 / (1024 * 1024)
        MIN_TIME_DELTA = 0.1

        valid_rate_mask = (
                df['time_delta_seconds'].notna() &
                (df['time_delta_seconds'] >= MIN_TIME_DELTA) &
                df['byte_delta'].notna() &
                (df['byte_delta'] >= 0)
        )
        df.loc[valid_rate_mask, 'Value_rate'] = (df.loc[valid_rate_mask, 'byte_delta'] * BYTES_TO_MB) / df.loc[
            valid_rate_mask, 'time_delta_seconds']
        df['Value_rate'] = df['Value_rate'].clip(lower=0)
        logger.info("NFS rate calculation complete.")

        df_valid_rates = df[valid_rate_mask].copy()
        if df_valid_rates.empty:
            logger.info("No valid NFS rates calculated.")
            return pd.DataFrame()

        # Aggregate rates if multiple records/devices contribute to the same node at the same original timestamp
        logger.info("Aggregating NFS rates to node level using original timestamps...")
        node_agg_keys = ['jobID', 'node', 'Timestamp_original']
        agg_df = df_valid_rates.groupby(node_agg_keys, observed=True, as_index=False)['Value_rate'].sum()
        agg_df.rename(columns={'Value_rate': 'Value'}, inplace=True)

        if agg_df.empty:
            logger.info("No valid data after node-level aggregation for NFS file.")
            return pd.DataFrame()
        logger.info(f"Node-level NFS aggregation complete. Result has {len(agg_df)} rows.")

        fresco_df = pd.DataFrame({
            'Job Id': agg_df['jobID'],
            'Host': agg_df['node'],
            'Event': 'nfs',
            'Value': agg_df['Value'],
            'Units': 'MB/s',
            'Timestamp': agg_df['Timestamp_original']
        })

        logger.info(f"Successfully processed NFS data: {len(fresco_df)} rows created.")
        return fresco_df

    except Exception as e:
        logger.error(f"An unexpected error occurred in process_nfs_file: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()
    finally:
        del df
        if df_valid_rates is not None: del df_valid_rates
        if agg_df is not None: del agg_df
        gc.collect()


def safe_parse_timestamp(timestamp_str, default=None):
    """Safely parse timestamp with fallback to default value if parsing fails"""
    try:
        return pd.to_datetime(timestamp_str, format='%m/%d/%Y %H:%M:%S')
    except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime):  # Added OutOfBounds
        # logger.info(f"Warning: Couldn't parse timestamp '{timestamp_str}', using default value")
        return default  # Return None or pd.NaT which pd.to_datetime handles


class ThreadedDownloader:
    def __init__(self, num_threads: int = 4, max_retries: int = 3, timeout: int = 300):
        self.num_threads = num_threads
        self.max_retries = max_retries
        self.timeout = timeout
        self.download_queue = Queue()
        self.results = {}
        self.lock = threading.Lock()
        self.completed_downloads = 0
        self.total_downloads = 0
        self.print_lock = threading.Lock()  # Lock for printing progress

    def download_worker(self, headers: dict):
        """Worker thread function to process download queue"""
        while True:
            task = self.download_queue.get()
            if task is None:  # Sentinel value received
                self.download_queue.task_done()  # Mark sentinel as done
                break  # Exit loop

            url, local_path = task
            success = False
            error_msg = "Unknown error"

            # Ensure parent directory exists
            try:
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
            except OSError as e:
                error_msg = f"Cannot create directory for {local_path}: {e}"
                logger.info(error_msg)  # logger.info directory creation error immediately
                # Update results and progress even on directory error
                with self.lock:
                    self.results[url] = False
                    self.completed_downloads += 1
                    with self.print_lock:
                        # Progress update for directory creation error
                        logger.debug(f'Download progress: {self.completed_downloads}/{self.total_downloads} files')
                self.download_queue.task_done()  # Mark task as done
                continue  # Skip to next task

            # Attempt download with retries
            for attempt in range(self.max_retries):
                try:
                    # Use stream=True for potentially large files, handle content chunk by chunk
                    response = requests.get(url, headers=headers, timeout=self.timeout, stream=True)
                    response.raise_for_status()  # Check for HTTP errors

                    # Write content to file
                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):  # Process in 8KB chunks
                            f.write(chunk)

                    # Explicitly close response connection
                    response.close()

                    # Check if file is non-empty after download
                    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                        success = True
                        break  # Successful download, exit retry loop
                    else:
                        # Handle case where download seemed successful but file is empty/missing
                        error_msg = f"Downloaded file {local_path} is empty or missing."
                        if os.path.exists(local_path): os.remove(local_path)  # Clean up empty file

                except requests.exceptions.RequestException as e:
                    error_msg = f"Attempt {attempt + 1} failed for {url}: {str(e)}"
                    if attempt < self.max_retries - 1:
                        # Exponential backoff
                        wait_time = (2 ** attempt) + np.random.uniform(0, 1)
                        # logger.info(f"Retrying {url} in {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                    else:
                        logger.info(f"\nFailed to download {url} after {self.max_retries} attempts: {str(e)}")
                except Exception as e:
                    # Catch other potential errors during file writing etc.
                    error_msg = f"Unexpected error during download/write for {url}: {str(e)}"
                    logger.info(f"\n{error_msg}")
                    # Break retry loop on unexpected non-request errors
                    break

            # If download ultimately failed, logger.info the last error
            if not success:
                logger.info(f"\nDownload failed for {url}. Last error: {error_msg}")
                # Attempt cleanup of potentially partial file
                if os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                    except OSError as e:
                        logger.info(f"Could not remove failed download file {local_path}: {e}")

            # Update results and progress outside the retry loop
            with self.lock:
                self.results[url] = success
                self.completed_downloads += 1
                # Progress update
                with self.print_lock:
                    logger.debug(f'Download progress: {self.completed_downloads}/{self.total_downloads} files')

            # Mark task as done regardless of success/failure
            self.download_queue.task_done()

    def download_files(self, file_list: List[tuple], headers: dict) -> Dict[str, bool]:
        """
        Download multiple files concurrently

        Args:
            file_list: List of tuples containing (url, local_path)
            headers: Request headers

        Returns:
            Dictionary mapping URLs to download success status
        """
        if not file_list:
            logger.info("No files to download.")
            return {}

        # Reset tracking variables
        self.results = {}
        self.completed_downloads = 0
        self.total_downloads = len(file_list)
        # Clear the queue if it was used before
        while not self.download_queue.empty():
            try:
                self.download_queue.get_nowait()
                self.download_queue.task_done()
            except Queue.Empty:
                break

        logger.info(f"\nStarting download of {self.total_downloads} files...")

        # Start worker threads
        threads = []
        for i in range(self.num_threads):
            thread = threading.Thread(
                target=self.download_worker,
                args=(headers,),
                daemon=True,  # Allows main thread to exit even if workers are blocked
                name=f"Downloader-{i + 1}"
            )
            thread.start()
            threads.append(thread)

        # Add download tasks to queue
        for url, local_path in file_list:
            self.download_queue.put((url, local_path))

        # Wait for all tasks in the queue to be processed
        self.download_queue.join()

        # Add sentinel values to signal workers to stop
        for _ in range(self.num_threads):
            self.download_queue.put(None)

        # Wait for all worker threads to terminate
        for thread in threads:
            thread.join()

        # Ensure progress bar finishes cleanly
        # logger.info() # Add a newline after the progress bar completes

        logger.info(f"\nDownload process finished for {self.total_downloads} files.")
        successful_downloads = sum(1 for success in self.results.values() if success)
        logger.info(f"Successfully downloaded {successful_downloads}/{self.total_downloads} files.")

        return self.results


def download_folder_threaded(
        folder_url: str,
        local_folder: str,
        required_files: List[str],
        headers: dict,
        num_threads: int = 4,
        timeout: int = 30  # Timeout for fetching folder listing
) -> bool:
    """Download all required files from a folder using threads"""
    if not os.path.exists(local_folder):
        try:
            os.makedirs(local_folder)
            # logger.info(f"Created local folder: {local_folder}") # Optional: info message
        except OSError as e:
            logger.info(f"Error creating directory {local_folder}: {e}")
            return False

    try:
        # Get folder contents (HTML page listing files)
        logger.info(f"Fetching file list from: {folder_url}")
        response = requests.get(folder_url, headers=headers, timeout=timeout)
        response.raise_for_status()  # Check for HTTP errors (4xx, 5xx)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Build download list
        download_list = []
        files_found_in_html = set()  # Use a set for efficient lookup

        # Look for links whose text exactly matches the required files
        for link in soup.find_all('a', href=True):  # Ensure 'href' exists
            link_text = link.text.strip()
            if link_text in required_files:
                files_found_in_html.add(link_text)
                # Construct absolute URL if href is relative
                csv_url = urljoin(folder_url, link['href'])
                # Construct local path
                csv_path = os.path.join(local_folder, link_text)
                download_list.append((csv_url, csv_path))

        # Check if all required files were found in the HTML listing
        required_set = set(required_files)
        missing_files = required_set - files_found_in_html
        if missing_files:
            # logger.info a warning but continue if *some* required files were found
            logger.info(f"Warning: Could not find links for all required files in {folder_url}.")
            logger.info(f"Missing files: {', '.join(sorted(list(missing_files)))}")
            if not download_list:
                logger.info("No required files found to download.")
                return False  # Return False if *none* of the required files were found

        if not download_list:
            logger.info(f"No required files found at URL {folder_url}")
            return False  # Should ideally be caught above, but double-check

        # Download the found files using the threaded downloader
        # Reuse the ThreadedDownloader instance logic if needed, or instantiate here
        downloader = ThreadedDownloader(num_threads=num_threads)  # Consider passing timeout from args
        results = downloader.download_files(download_list, headers)

        # Check if *all* downloads scheduled were successful
        all_successful = all(results.get(url, False) for url, _ in download_list)

        if not all_successful:
            logger.info(f"Warning: Not all files were downloaded successfully for folder {folder_url}.")
            # Optionally list failed files:
            failed_downloads = [url for url, path in download_list if not results.get(url, False)]
            if failed_downloads:
                logger.info(f"Failed URLs: {', '.join(failed_downloads)}")

        # Return True only if all files attempted were downloaded successfully
        # Or adjust logic if partial success is acceptable for processing
        return all_successful

    except requests.exceptions.RequestException as e:
        logger.info(f"Error fetching folder listing {folder_url}: {str(e)}")
        return False
    except Exception as e:
        # Catch other potential errors (e.g., BeautifulSoup issues)
        logger.info(f"Unexpected error processing folder URL {folder_url}: {str(e)}")
        return False


class ProcessingTracker:
    def __init__(self, base_dir, reset=False):
        self.base_dir = Path(base_dir)  # Use Path object
        self.tracker_file = self.base_dir / "processing_status.json"
        self.lock = threading.RLock()  # Lock for thread-safe file access
        self.status = {}

        if reset and self.tracker_file.exists():
            try:
                self.tracker_file.unlink()
                logger.info("Resetting processing status.")
            except OSError as e:
                logger.info(f"Warning: Could not delete tracker file {self.tracker_file}: {e}")

        self.load_status()

    def load_status(self):
        with self.lock:
            if self.tracker_file.exists():
                try:
                    with open(self.tracker_file, 'r', encoding='utf-8') as f:
                        self.status = json.load(f)
                    # Ensure default keys exist if loading an older/corrupted file
                    self.status.setdefault('processed_folders', [])
                    self.status.setdefault('failed_folders', [])
                    self.status.setdefault('last_processed_index', -1)
                    logger.info(
                        f"Loaded processing status: {len(self.status['processed_folders'])} processed, {len(self.status['failed_folders'])} failed.")
                except (json.JSONDecodeError, IOError) as e:
                    logger.info(f"Error loading status file {self.tracker_file}: {e}. Initializing fresh status.")
                    self._initialize_status()
            else:
                logger.info("No existing status file found. Initializing fresh status.")
                self._initialize_status()
                self.save_status()  # Save the initial state

    def _initialize_status(self):
        """Helper to set default status dictionary."""
        self.status = {
            'processed_folders': [],
            'failed_folders': [],
            'last_processed_index': -1
        }

    def save_status(self):
        logger.info("Saving status")
        with self.lock:
            try:
                # Ensure parent directory exists
                self.tracker_file.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Saving to {self.tracker_file}")
                with open(self.tracker_file, 'w', encoding='utf-8') as f:
                    json.dump(self.status, f, indent=4)  # Add indent for readability
                logger.info(f"Processing status saved: {self.status}")
            except IOError as e:
                logger.info(f"Error saving status file {self.tracker_file}: {e}")
            except Exception as e:
                logger.info(f"Unexpected error saving status: {e}")

    def mark_folder_processed(self, folder_name: str, index: int):
        """Mark a folder as successfully processed."""
        with self.lock:
            if folder_name not in self.status['processed_folders']:
                self.status['processed_folders'].append(folder_name)
                # Remove from failed list if it was there previously
                if folder_name in self.status['failed_folders']:
                    self.status['failed_folders'].remove(folder_name)
                # Update last processed index *only if* this index is greater
                self.status['last_processed_index'] = max(self.status['last_processed_index'], index)
                self.save_status()
                logger.info(f"Marked folder '{folder_name}' (index {index}) as processed.")

    def mark_folder_failed(self, folder_name: str):
        """Mark a folder as failed."""
        with self.lock:
            # Only add to failed list if it's not already marked as processed
            if folder_name not in self.status['processed_folders'] and \
                    folder_name not in self.status['failed_folders']:
                self.status['failed_folders'].append(folder_name)
                self.save_status()
                logger.info(f"Marked folder '{folder_name}' as failed.")

    def is_folder_processed(self, folder_name: str) -> bool:
        """Check if a folder is in the processed list."""
        with self.lock:
            return folder_name in self.status['processed_folders']

    def get_last_processed_index(self) -> int:
        """Get the index of the last successfully processed folder."""
        with self.lock:
            return self.status.get('last_processed_index', -1)

    # get_next_index might be misleading if processing is not strictly sequential
    # Renamed to reflect its actual behavior based on saved state
    def get_start_index(self) -> int:
        """Determine the index from which to potentially resume processing."""
        # Start from the item *after* the last successfully processed one
        with self.lock:
            return self.status.get('last_processed_index', -1) + 1


class DataVersionManager:
    def __init__(self, base_dir):
        self.base_dir = Path(base_dir)
        self.version_file = self.base_dir / "version_info.json"
        self.lock = threading.RLock()
        self.load_version_info()

    def load_version_info(self):
        with self.lock:
            if self.version_file.exists():
                try:
                    with open(self.version_file, 'r', encoding='utf-8') as f:
                        self.version_info = json.load(f)
                    # Ensure default keys exist
                    self.version_info.setdefault('current_version', 1)
                    self.version_info.setdefault('processed_versions', [])  # Renamed for clarity
                    logger.info(f"Loaded version info: current v{self.version_info['current_version']}")
                except (json.JSONDecodeError, IOError) as e:
                    logger.info(
                        f"Error loading version file {self.version_file}: {e}. Initializing fresh version info.")
                    self._initialize_version_info()
            else:
                logger.info("No existing version file found. Initializing.")
                self._initialize_version_info()
                self.save_version_info()  # Save initial state

    def _initialize_version_info(self):
        """Helper to set default version info dictionary."""
        self.version_info = {
            'current_version': 1,
            'processed_versions': []
        }

    def save_version_info(self):
        with self.lock:
            try:
                self.version_file.parent.mkdir(parents=True, exist_ok=True)
                with open(self.version_file, 'w', encoding='utf-8') as f:
                    json.dump(self.version_info, f, indent=4)
                # logger.info("Version info saved.") # Can be verbose
            except IOError as e:
                logger.info(f"Error saving version file {self.version_file}: {e}")
            except Exception as e:
                logger.info(f"Unexpected error saving version info: {e}")

    def get_current_version_tag(self) -> str:
        """Get the tag for the current version (e.g., 'v1')."""
        with self.lock:
            return f"v{self.version_info['current_version']}"

    def increment_version(self):
        """Record the current version as processed and increment to the next."""
        with self.lock:
            current_v = self.version_info['current_version']
            if current_v not in self.version_info['processed_versions']:
                self.version_info['processed_versions'].append(current_v)
            self.version_info['current_version'] += 1
            logger.info(f"Incremented version. Current version is now v{self.version_info['current_version']}")
            self.save_version_info()


def get_folder_urls(base_url: str, headers: dict, timeout: int = 60) -> List[tuple]:
    """Get list of all date folder URLs matching the expected pattern."""
    logger.info(f"Fetching folder list from: {base_url}")
    folders = []
    try:
        response = requests.get(base_url, headers=headers, timeout=timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Regex to match YYYY-MM/ format specifically
        # Ensures it ends with a slash and contains the date format
        date_pattern = re.compile(r'^(\d{4}-\d{2})/$')

        for link in soup.find_all('a', href=True):
            link_text = link.text.strip()
            match = date_pattern.match(link_text)
            if match:
                folder_name = match.group(1)  # Extract YYYY-MM part
                folder_url = urljoin(base_url, link['href'])
                # Ensure URL ends with a slash for consistency
                if not folder_url.endswith('/'):
                    folder_url += '/'
                folders.append((folder_name, folder_url))

        # Sort folders chronologically based on the name (YYYY-MM)
        folders.sort(key=lambda x: x[0])
        logger.info(f"Found {len(folders)} date folders.")
        return folders

    except requests.exceptions.RequestException as e:
        logger.info(f"Error accessing base URL {base_url}: {str(e)}")
        return []  # Return empty list on error
    except Exception as e:
        logger.info(f"Error parsing folder list from {base_url}: {str(e)}")
        return []


def process_folder_data(folder_path: str) -> Optional[pd.DataFrame]:
    """
    Process all required CSV files within a downloaded folder.

    ENHANCED: Now validates that all 4 required files are available and successfully processed.
    Logs detailed information about missing or failed files.

    Uses ThreadPoolExecutor for parallel processing of individual files.
    Handles large files using chunking. Performs cleanup of processed DataFrames.

    Args:
        folder_path: Path to the local folder containing downloaded CSVs.

    Returns:
        A combined DataFrame of all processed metrics, or None if any required file
        is missing or fails processing.
    """
    logger.info(f"Starting processing for folder: {folder_path}")
    folder_name = os.path.basename(folder_path)

    # Track processing status for each required file
    required_files = ['block.csv', 'cpu.csv', 'mem.csv', 'llite.csv']
    file_status = {filename: {'exists': False, 'processed': False, 'rows': 0, 'error': None}
                   for filename in required_files}

    results = []
    # Map filenames to their respective processing functions
    file_processors: Dict[str, Callable[[Union[str, pd.DataFrame]], pd.DataFrame]] = {
        'block.csv': process_block_file,
        'cpu.csv': process_cpu_file,
        'mem.csv': process_mem_file,
        'llite.csv': process_nfs_file
    }

    # STEP 1: Check that all required files exist before processing
    missing_files = []
    for filename in required_files:
        file_path = os.path.join(folder_path, filename)
        if os.path.exists(file_path):
            file_status[filename]['exists'] = True
            logger.info(f"✓ Found required file: {filename}")
        else:
            missing_files.append(filename)
            logger.warning(f"✗ Missing required file: {filename}")

    if missing_files:
        logger.error(f"Cannot process folder {folder_name} - missing required files: {missing_files}")
        logger.info(f"Required files: {required_files}")
        available_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')] if os.path.exists(
            folder_path) else []
        logger.info(f"Available files in {folder_path}: {available_files}")
        return None

    # Determine max workers based on CPU count, capped for resource limits
    max_workers = min(len(file_processors), os.cpu_count() or 1, 4)  # Cap at 4 workers
    logger.info(f"Using {max_workers} workers for parallel file processing.")
    logger.info(f"All {len(required_files)} required files found. Beginning processing...")

    # Create a dictionary to store results for each file
    file_results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Store futures mapped to filenames for tracking
        future_to_file = {}

        # Submit tasks for each required file (we know they all exist now)
        for filename, processor in file_processors.items():
            file_path = os.path.join(folder_path, filename)
            logger.info(f"Submitting {filename} for processing.")
            # Submit the processing task
            future = executor.submit(processor, file_path)  # Pass file path directly
            future_to_file[future] = filename

        # Process completed futures
        for future in as_completed(future_to_file):
            filename = future_to_file[future]
            try:
                # Get the result (DataFrame or None)
                result_df = future.result()

                if result_df is not None and not result_df.empty:
                    logger.info(f"✓ Successfully processed {filename}, found {len(result_df)} valid rows.")
                    file_status[filename]['processed'] = True
                    file_status[filename]['rows'] = len(result_df)
                    # Store the result DataFrame for this file
                    file_results[filename] = result_df
                elif result_df is not None and result_df.empty:
                    logger.warning(f"✗ Processed {filename}, but no valid data rows were extracted.")
                    file_status[filename]['error'] = "No valid data rows extracted"
                else:  # result_df is None
                    logger.error(f"✗ Processing function for {filename} returned None (likely an error).")
                    file_status[filename]['error'] = "Processing function returned None"

            except Exception as e:
                error_msg = f"Exception during processing: {str(e)}"
                logger.error(f"✗ Error processing file {filename} in parallel task: {error_msg}")
                file_status[filename]['error'] = error_msg
                # Optionally log traceback here: import traceback; traceback.logger.info_exc()

            finally:
                # Attempt to remove the source file *after* processing attempt
                file_path = os.path.join(folder_path, filename)
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        logger.debug(f"Removed processed source file: {file_path}")
                    except OSError as e:
                        logger.warning(f"Error removing source file {file_path}: {str(e)}")
                # Trigger garbage collection more explicitly after handling a future
                gc.collect()

    # STEP 2: Validate that all required files were successfully processed
    successfully_processed = [filename for filename, status in file_status.items() if status['processed']]
    failed_files = [filename for filename, status in file_status.items() if not status['processed']]

    logger.info(f"=== PROCESSING SUMMARY for {folder_name} ===")
    logger.info(f"Successfully processed files ({len(successfully_processed)}/4): {successfully_processed}")
    if failed_files:
        logger.warning(f"Failed to process files ({len(failed_files)}/4): {failed_files}")
        for filename in failed_files:
            error = file_status[filename]['error']
            logger.warning(f"  - {filename}: {error if error else 'Unknown error'}")

    total_rows = sum(status['rows'] for status in file_status.values())
    logger.info(f"Total rows across all processed files: {total_rows}")
    logger.info("=" * 50)

    # STEP 3: Only return results if ALL required files were processed successfully
    if len(successfully_processed) == len(required_files):
        logger.info(f"✓ All {len(required_files)} required files processed successfully for {folder_name}")

        # Collect all successfully processed DataFrames
        results = []
        for filename in required_files:
            if filename in file_results:
                results.append(file_results[filename])

        # Combine results if any were successful
        if results:
            logger.info(f"Combining results from {len(results)} successfully processed files...")
            try:
                # Concatenate results, ignoring index to create a continuous index
                final_df = pd.concat(results, ignore_index=True)
                logger.info(f"Combined DataFrame created with {len(final_df)} total rows.")

                # Log event type distribution for verification
                if 'Event' in final_df.columns:
                    event_counts = final_df['Event'].value_counts()
                    logger.info("Event type distribution in combined DataFrame:")
                    for event, count in event_counts.items():
                        logger.info(f"  - {event}: {count} rows")

                # Clean up the list of individual DataFrames
                del results
                del file_results
                gc.collect()

                # Perform a final check for empty DataFrame after concat (shouldn't happen if results list wasn't empty)
                if final_df.empty:
                    logger.warning("Combined DataFrame is empty after concatenation.")
                    return None
                return final_df
            except Exception as e:
                logger.error(f"Error combining processing results: {str(e)}")
                # Clean up if concatenation fails
                del results
                del file_results
                gc.collect()
                return None
        else:
            logger.error(
                f"No results to combine for {folder_name} (this shouldn't happen if all files processed successfully).")
            return None
    else:
        logger.error(
            f"✗ INCOMPLETE PROCESSING: Only {len(successfully_processed)}/{len(required_files)} required files processed successfully for {folder_name}")
        logger.error("Folder processing marked as FAILED due to incomplete file processing.")
        # Clean up any partial results
        if file_results:
            del file_results
            gc.collect()
        return None


def check_disk_space(path_to_check: str = '.', warning_gb: float = 20.0, critical_gb: float = 5.0) -> tuple[bool, bool]:
    """
    Check available disk space on the filesystem containing the given path.

    Args:
        path_to_check: The path to check (e.g., output directory, home directory). Defaults to current dir.
        warning_gb: Threshold in GB below which space is considered low (returns False for is_abundant).
        critical_gb: Threshold in GB below which space is critical (returns False for is_safe).

    Returns:
        tuple: (is_safe, is_abundant)
               is_safe (bool): True if available space > critical_gb.
               is_abundant (bool): True if available space > warning_gb.
    """
    try:
        # Get disk usage statistics for the filesystem containing the path
        usage = shutil.disk_usage(path_to_check)
        available_bytes = usage.free
        available_gb = available_bytes / (1024 ** 3)

        logger.info(f"Disk space check ({path_to_check}): Available = {available_gb:.2f} GB")

        is_safe = available_gb > critical_gb
        is_abundant = available_gb > warning_gb

        if not is_safe:
            logger.info(
                f"CRITICAL: Available disk space ({available_gb:.2f} GB) is below threshold ({critical_gb} GB).")
        elif not is_abundant:
            logger.info(
                f"WARNING: Available disk space ({available_gb:.2f} GB) is below warning threshold ({warning_gb} GB).")

        return is_safe, is_abundant

    except FileNotFoundError:
        logger.info(f"Error checking disk space: Path not found '{path_to_check}'. Assuming critical state.")
        return (False, False)
    except Exception as e:
        logger.info(f"Error checking disk space for '{path_to_check}': {str(e)}. Assuming critical state.")
        # Default to critical state if check fails for any reason
        return (False, False)


def save_monthly_data(monthly_data: Dict[str, pd.DataFrame], output_dir: str, version_manager: DataVersionManager) -> \
        List[str]:
    """
    Save monthly data DataFrames to CSV files in the specified output directory.

    Appends data if a file for the month and version already exists.

    Args:
        monthly_data: Dictionary mapping month strings ('YYYY_MM') to DataFrames.
        output_dir: The target directory to save the files.
        version_manager: The DataVersionManager instance to get the version tag.

    Returns:
        List of file paths that were successfully saved or updated.
    """
    # Ensure the output directory exists
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        logger.info(f"Error creating output directory {output_dir}: {e}")
        return []  # Return empty list if directory cannot be created

    version_suffix = version_manager.get_current_version_tag()
    saved_files = []
    logger.info(f"Saving monthly data to: {output_dir} with version tag: {version_suffix}")

    for month, df_new_month_data in monthly_data.items():
        if df_new_month_data is None or df_new_month_data.empty:
            logger.info(f"Skipping empty data for month {month}.")
            continue

        # Construct the file path using the provided output directory
        file_path = os.path.join(output_dir, f"FRESCO_Conte_ts_{month}_{version_suffix}.csv")

        try:
            if os.path.exists(file_path):
                logger.info(f"Appending data to existing file: {file_path}")
                # Append new data without reading the whole existing file
                # Use header=False to avoid writing headers again
                df_new_month_data.to_csv(file_path, mode='a', header=False, index=False)
                # Note: This append doesn't handle duplicates. If needed, read, concat, drop_duplicates, save.
                # For simplicity and performance on potentially large files, simple append is used here.
                # Consider adding a post-processing step to handle duplicates if necessary.
            else:
                logger.info(f"Creating new file: {file_path}")
                # Write new file with header
                df_new_month_data.to_csv(file_path, index=False)

            saved_files.append(file_path)
            logger.info(f"Successfully saved/updated data for {month} to {os.path.basename(file_path)}")

        except IOError as e:
            logger.info(f"Error writing file {file_path}: {e}")
        except Exception as e:
            logger.info(f"Unexpected error saving data for month {month} to {file_path}: {e}")

        finally:
            # Clean up the DataFrame for the month to free memory
            del df_new_month_data
            gc.collect()

    return saved_files


# Removed upload_to_s3 function
#
#
# def print_info(current: int, total: int, prefix: str = '', suffix: str = '', bar_length: int = 50):
#     """logger.info progress as a percentage with a progress bar."""
#     if total == 0:  # Avoid division by zero
#         percent_str = "N/A"
#         bar = '-' * bar_length
#     else:
#         percent = (current / total) * 100
#         percent_str = f"{percent:.1f}%"
#         filled_length = int(bar_length * current // total)
#         # Ensure filled_length doesn't exceed bar_length
#         filled_length = min(bar_length, filled_length)
#         bar = '█' * filled_length + '-' * (bar_length - filled_length)
#
#     # Use \r to return to the beginning of the line, flush ensures immediate output
#     logger.info(f'\{prefix} |{bar}| {percent_str} {suffix}   ', end='',
#           flush=True)  # Added spaces to clear previous longer suffix
#
#     if current >= total:  # >= handles potential overshoots
#         logger.info()  # logger.info a newline when done


def save_folder_data_locally(result_df: pd.DataFrame, folder_name: str, output_dir: str,
                             version_manager: DataVersionManager) -> bool:
    """
    Save the processed DataFrame for a folder to the specified local directory as Parquet format.

    Args:
        result_df: The combined DataFrame from processing a folder's files.
        folder_name: The name of the folder being processed (e.g., 'YYYY-MM').
        output_dir: The target local directory to save the Parquet files.
        version_manager: The DataVersionManager instance.

    Returns:
        True if data was successfully saved, False otherwise.
    """
    if result_df is None or result_df.empty:
        logger.info(f"No data to save for folder {folder_name}.")
        return False  # Nothing to save, consider this not a failure of saving itself, but no output

    logger.info(f"\nSaving processed data for folder {folder_name} locally as Parquet...")
    try:
        # Ensure Timestamp column is datetime type
        if not pd.api.types.is_datetime64_any_dtype(result_df['Timestamp']):
            result_df['Timestamp'] = pd.to_datetime(result_df['Timestamp'], errors='coerce')
            result_df = result_df.dropna(subset=['Timestamp'])  # Drop rows where conversion failed

        if result_df.empty:
            logger.info(f"No valid timestamp data remaining for folder {folder_name} after conversion.")
            return False

        # Get version suffix from version manager
        version_suffix = version_manager.get_current_version_tag()

        # Construct the output file path with .parquet extension
        file_path = os.path.join(output_dir, f"FRESCO_Conte_ts_{folder_name}_{version_suffix}.parquet")

        try:
            if os.path.exists(file_path):
                logger.info(f"Appending data to existing Parquet file: {file_path}")
                # Read existing data and combine with new data
                existing_df = pd.read_parquet(file_path)
                combined_df = pd.concat([existing_df, result_df], ignore_index=True)
                # Write the combined data back to Parquet with compression
                combined_df.to_parquet(file_path, index=False, compression='snappy')
                del existing_df
                del combined_df
                gc.collect()
            else:
                logger.info(f"Creating new Parquet file: {file_path}")
                # Write new file with compression
                result_df.to_parquet(file_path, index=False, compression='snappy')

            logger.info(f"Successfully saved data for {folder_name} to {os.path.basename(file_path)}")
            return True  # Indicate successful save operation

        except Exception as e:
            logger.info(f"Error writing Parquet file {file_path}: {e}")
            return False

    except Exception as e:
        logger.info(f"Error saving data locally for folder {folder_name}: {str(e)}")
        return False  # Indicate failure

    finally:
        # Clean up the DataFrame to free memory
        del result_df
        gc.collect()


# Global list to track folders with incomplete processing
INCOMPLETE_FOLDERS = []


def main():
    # Define base directory and target local output directory
    base_dir = os.getcwd()  # Directory where the script runs, stores status files
    # --- USER CONFIGURATION: Set the final output directory ---
    LOCAL_OUTPUT_DIR = "<LOCAL_PATH_PLACEHOLDER>/projects/conte-to-fresco-etl/step-1/output"
    # --- END USER CONFIGURATION ---

    logger.info("=" * 70)
    logger.info("TRANSFORMER.PY STARTED - PROCESSING 2017-01 AND 2017-02 ONLY")
    logger.info("=" * 70)
    logger.info(f"Base directory: {base_dir}")
    logger.info(f"Output directory: {LOCAL_OUTPUT_DIR}")

    base_url = "https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Conte/TACC_Stats/"
    required_files = ['block.csv', 'cpu.csv', 'mem.csv', 'llite.csv']
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    # Ensure the final output directory exists
    try:
        os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)
        logger.info(f"Output directory ensured: {LOCAL_OUTPUT_DIR}")
    except OSError as e:
        logger.critical(f"Cannot create output directory {LOCAL_OUTPUT_DIR}. Exiting. Error: {e}")
        return  # Cannot proceed without output directory

    # Initialize managers
    logger.info("Initializing ProcessingTracker...")
    try:
        tracker = ProcessingTracker(base_dir, reset=False)  # Load or initialize status
        logger.info("ProcessingTracker initialized successfully.")
    except Exception as e:
        logger.critical(f"ERROR initializing ProcessingTracker: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return  # Exit if tracker fails

    logger.info("Initializing DataVersionManager...")
    try:
        version_manager = DataVersionManager(base_dir)  # Load or initialize version
        logger.info("DataVersionManager initialized successfully.")
    except Exception as e:
        logger.critical(f"ERROR initializing DataVersionManager: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return  # Exit if version manager fails

    # Configure number of download threads (adjust based on system resources/network)
    num_download_threads = min(os.cpu_count() or 1, 8)  # Use up to 8 threads or CPU count
    logger.info(f"Using {num_download_threads} download threads.")
    logger.info("Managers initialized successfully.")

    try:
        # 1. Get list of folders from the web server
        logger.info("Getting folder URLs from remote server...")
        all_folders = get_folder_urls(base_url, headers)
        logger.info(f"Found {len(all_folders)} folders from URL.")
        if all_folders:
            logger.debug(f"First few folders found: {all_folders[:5]}")

        if not all_folders:
            logger.error("No date folders found at the specified URL. Exiting.")
            return

        # MODIFIED: Filter to only include 2017-01 and 2017-02
        target_folders = ['2017-01', '2017-02']
        filtered_folders = [(name, url) for name, url in all_folders if name in target_folders]

        if not filtered_folders:
            logger.error(
                f"Target folders {target_folders} not found in available folders. Available folders: {[name for name, _ in all_folders[:10]]}")
            return

        logger.info(
            f"FILTERED: Processing only {len(filtered_folders)} target folders: {[name for name, _ in filtered_folders]}")
        all_folders = filtered_folders  # Replace all_folders with filtered list

        # Create a map for easy lookup: folder_name -> (index, url)
        folder_map = {name: (i, url) for i, (name, url) in enumerate(all_folders)}
        logger.debug("Created folder map.")

        # 2. Determine which folders to process
        processed_set = set(tracker.status.get('processed_folders', []))
        failed_set = set(tracker.status.get('failed_folders', []))
        logger.info(f"Folders already processed: {len(processed_set)}")
        logger.info(f"Folders previously failed: {len(failed_set)}")

        # MODIFIED: Only process the target folders, regardless of previous status
        folders_to_process_names = [name for name, _ in all_folders]  # Process all target folders

        # Optionally remove from processed set if you want to force reprocessing
        for folder_name in folders_to_process_names:
            if folder_name in processed_set:
                logger.info(f"Forcing reprocessing of previously processed folder: {folder_name}")
                # Optionally remove from tracker to force reprocessing
                # tracker.status['processed_folders'].remove(folder_name)

        total_folders_to_attempt = len(folders_to_process_names)
        logger.info(f"Determined {total_folders_to_attempt} target folders to process: {folders_to_process_names}")

        if not folders_to_process_names:
            logger.info("No target folders to process. Nothing to do.")
            return

        logger.info(f"Processing {len(all_folders)} target folders only.")
        logger.info(f"Output will be saved to: {LOCAL_OUTPUT_DIR}")

        # 3. Process folders one by one
        overall_processed_count = 0
        for current_attempt_idx, folder_name in enumerate(folders_to_process_names):
            # Get original index and URL from map
            if folder_name not in folder_map:
                logger.info(
                    f"Warning: Folder name '{folder_name}' not found in folder map. Skipping.")
                continue
            original_index, folder_url = folder_map[folder_name]

            # --- Progress Update ---
            logger.info("-" * 70)
            logger.info(
                f"Processing folder {current_attempt_idx + 1}/{total_folders_to_attempt}: {folder_name} (Original Index: {original_index})")
            # Display overall progress based on original indices if possible, or just attempt count
            # Using attempt count here for simplicity
            progress_pct = (current_attempt_idx / total_folders_to_attempt * 100) if total_folders_to_attempt > 0 else 0
            logger.info(
                f'Overall Progress: {progress_pct:.1f}% ({current_attempt_idx}/{total_folders_to_attempt}) - {folder_name}')

            # --- Check if already processed (skip this check if forcing reprocessing) ---
            if tracker.is_folder_processed(folder_name):
                logger.info(
                    f"Folder {folder_name} is already marked as processed. Processing anyway for target folders.")
                # Continue processing instead of skipping

            # --- Check Disk Space ---
            # Check space in the *output* directory's filesystem
            is_safe, is_abundant = check_disk_space(LOCAL_OUTPUT_DIR, critical_gb=5.0)  # 5GB critical limit
            if not is_safe:
                logger.critical("CRITICAL disk space reached. Stopping processing to prevent filling disk.")
                break  # Stop the entire loop

            # Define temporary download location for this folder
            temp_folder = os.path.join(base_dir, "temp_download", folder_name)

            folder_success = False  # Flag for success of this folder's processing cycle
            try:
                # --- Download ---
                logger.info(f"Attempting to download required files for {folder_name} to {temp_folder}")
                download_successful = download_folder_threaded(
                    folder_url,
                    temp_folder,
                    required_files,
                    headers,
                    num_threads=num_download_threads
                )

                if download_successful:
                    logger.info(f"Download complete for {folder_name}. Starting data processing...")
                    # --- Process ---
                    processed_df = process_folder_data(temp_folder)

                    if processed_df is not None and not processed_df.empty:
                        logger.info(f"Processing complete for {folder_name}. Saving results locally...")
                        # --- Save Locally ---
                        save_successful = save_folder_data_locally(
                            processed_df,
                            folder_name,
                            LOCAL_OUTPUT_DIR,
                            version_manager
                        )

                        if save_successful:
                            logger.info(f"Successfully processed and saved data for {folder_name}.")
                            tracker.mark_folder_processed(folder_name, original_index)
                            overall_processed_count += 1
                            folder_success = True
                        else:
                            logger.error(f"Failed to save data locally for {folder_name}.")
                            tracker.mark_folder_failed(folder_name)
                            INCOMPLETE_FOLDERS.append(folder_name)

                        # --- Memory Management ---
                        del processed_df  # Explicitly delete large DataFrame
                        gc.collect()
                    else:
                        logger.error(
                            f"Processing failed or yielded no data for {folder_name} (incomplete file processing).")
                        tracker.mark_folder_failed(folder_name)
                        INCOMPLETE_FOLDERS.append(folder_name)
                else:
                    logger.error(f"Download failed for folder {folder_name}. Skipping processing.")
                    tracker.mark_folder_failed(folder_name)
                    INCOMPLETE_FOLDERS.append(folder_name)

            except Exception as e:
                logger.error(f"Unexpected error during processing cycle for folder {folder_name}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                tracker.mark_folder_failed(folder_name)
                INCOMPLETE_FOLDERS.append(folder_name)

            finally:
                # --- Cleanup Temporary Folder ---
                if os.path.exists(temp_folder):
                    logger.debug(f"Cleaning up temporary folder: {temp_folder}")
                    try:
                        shutil.rmtree(temp_folder)
                    except OSError as e:
                        logger.warning(f"Could not remove temporary folder {temp_folder}: {e}")
                # Short pause between folders
                time.sleep(1)

        # --- End of Loop ---
        logger.info("-" * 70)
        logger.info("Processing loop finished.")
        final_processed_count = len([f for f in tracker.status.get('processed_folders', []) if f in target_folders])
        final_failed_count = len([f for f in tracker.status.get('failed_folders', []) if f in target_folders])
        logger.info(f"Target folders successfully processed: {final_processed_count}")
        logger.info(f"Target folders currently marked as failed: {final_failed_count}")

        # --- ENHANCED REPORTING: Show incomplete processing details ---
        logger.info("=" * 70)
        logger.info("INCOMPLETE PROCESSING REPORT")
        logger.info("=" * 70)

        target_incomplete = [f for f in INCOMPLETE_FOLDERS if f in target_folders]
        if target_incomplete:
            logger.warning(f"⚠️  WARNING: {len(target_incomplete)} target folders had incomplete processing:")
            logger.warning(f"   These folders did not have all 4 required files successfully processed:")
            for i, folder in enumerate(target_incomplete, 1):
                logger.warning(f"   {i:2d}. {folder}")
            logger.info(f"   Required files for each folder: ['block.csv', 'cpu.csv', 'mem.csv', 'llite.csv']")
            logger.warning(f"   These folders may need manual review or reprocessing.")
        else:
            logger.info("✅ All target folders had complete file processing (all 4 required files).")

        success_rate = (final_processed_count / len(target_folders) * 100) if target_folders else 0
        logger.info(f"Target Folders Success Rate: {final_processed_count}/{len(target_folders)} ({success_rate:.1f}%)")
        logger.info("=" * 70)

        # Consider incrementing the version *once* at the end of a successful run if needed
        # version_manager.increment_version() # Uncomment if version should represent a completed run attempt

    except KeyboardInterrupt:
        logger.warning("Processing interrupted by user (Ctrl+C). Saving current state.")
        # Status is saved automatically by the tracker on changes
    except Exception as e:
        logger.error("An error occurred in the main processing controller")
        logger.error(f"Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

    finally:
        logger.info("=" * 70)
        logger.info("SCRIPT EXECUTION FINISHED")
        logger.info("=" * 70)
        # Final status summary
        if 'tracker' in locals():
            target_processed = [f for f in tracker.status.get('processed_folders', []) if f in ['2017-01', '2017-02']]
            target_failed = [f for f in tracker.status.get('failed_folders', []) if f in ['2017-01', '2017-02']]
            logger.info("Final Status (Target Folders Only):")
            logger.info(f"  Target Processed Folders: {len(target_processed)} - {target_processed}")
            logger.info(f"  Target Failed Folders: {len(target_failed)} - {target_failed}")
            logger.info(f"  Last Processed Index: {tracker.get_last_processed_index()}")
        if 'version_manager' in locals():
            logger.info(f"  Current Version Tag for Next Run: {version_manager.get_current_version_tag()}")
        logger.info(f"Log file saved to: {LOG_FILE}")


if __name__ == "__main__":
    main()
