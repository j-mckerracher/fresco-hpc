import time
import uuid
import signal
from datetime import datetime
import polars as pl
import re
import os
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
from pathlib import Path
import shutil
import psutil
import pyarrow as pa
import gc
from collections import defaultdict
from utils.ready_signal_creator import ReadySignalManager
from typing import Union
from tqdm import tqdm

# Create a log file with timestamp in the filename
log_dir = "<LOCAL_PATH_PLACEHOLDER>/projects/conte-to-fresco-etl/step-2/cache/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "conte_log.log")

# Configure logging with less verbose output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Reduce logging verbosity for some loggers
logging.getLogger('polars').setLevel(logging.WARNING)
logging.getLogger('pyarrow').setLevel(logging.WARNING)

# Globals
terminate_requested = False
memory_limit_exceeded = False
current_processing_file = None

# Define file paths
CACHE_DIR = Path(r"<LOCAL_PATH_PLACEHOLDER>/projects/conte-to-fresco-etl/step-2/cache")
JOB_ACCOUNTING_PATH = Path(CACHE_DIR / 'accounting')
PROC_METRIC_PATH = Path(CACHE_DIR / 'input/metrics')
OUTPUT_PATH = Path(CACHE_DIR / 'output_consumer')
COMPOSER_READY_DIR = Path(CACHE_DIR / 'composer_ready')

# Ensure directories exists
CACHE_DIR.mkdir(exist_ok=True)
OUTPUT_PATH.mkdir(exist_ok=True, parents=True)
COMPOSER_READY_DIR.mkdir(exist_ok=True, parents=True)

# Configuration
cpus = os.cpu_count()
logger.info(f"Workers: {cpus}")
MAX_WORKERS = int(os.environ.get("CONSUMER_MAX_WORKERS", cpus))
MIN_FREE_MEMORY_GB = float(os.environ.get("CONSUMER_MIN_FREE_MEMORY_GB", 5.0))
MIN_FREE_DISK_GB = float(os.environ.get("CONSUMER_MIN_FREE_DISK_GB", 10.0))
BASE_CHUNK_SIZE = int(os.environ.get("CONSUMER_BASE_CHUNK_SIZE", 100_000))
MAX_MEMORY_USAGE_GB = float(os.environ.get("CONSUMER_MAX_MEMORY_GB", 80.0))
MEMORY_CHECK_INTERVAL = 0.1
SMALL_FILE_THRESHOLD_MB = 20
MAX_RETRIES = 3
JOB_CHECK_INTERVAL = 10
FUTURE_TIMEOUT = 7200

# Create global instances
signal_manager = ReadySignalManager(ready_dir=Path(CACHE_DIR / 'ready'), logger=logger)
composer_signal_manager = ReadySignalManager(ready_dir=COMPOSER_READY_DIR, logger=logger)

# Define the expected PyArrow schema for output Parquet files
EXPECTED_PA_SCHEMA = pa.schema([
    ('time', pa.timestamp('ns', tz='UTC')),
    ('submit_time', pa.timestamp('ns', tz='UTC')),
    ('start_time', pa.timestamp('ns', tz='UTC')),
    ('end_time', pa.timestamp('ns', tz='UTC')),
    ('timelimit', pa.float64()),
    ('nhosts', pa.float64()),
    ('ncores', pa.float64()),
    ('account', pa.large_string()),
    ('queue', pa.large_string()),
    ('host', pa.large_string()),
    ('jid', pa.large_string()),
    ('unit', pa.large_string()),
    ('jobname', pa.large_string()),
    ('exitcode', pa.large_string()),
    ('host_list', pa.large_string()),
    ('username', pa.large_string()),
    ('value_cpuuser', pa.float64()),
    ('value_gpu', pa.float64()),
    ('value_memused', pa.float64()),
    ('value_memused_minus_diskcache', pa.float64()),
    ('value_nfs', pa.float64()),
    ('value_block', pa.float64())
])

# For Polars schema enforcement and column ordering
FINAL_COLUMNS = [field.name for field in EXPECTED_PA_SCHEMA]


def setup_signal_handlers():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 ** 3)


def get_available_memory():
    return psutil.virtual_memory().available / (1024 ** 3)


def get_free_disk_space():
    _, _, free = shutil.disk_usage(CACHE_DIR)
    return free / (1024 ** 3)


def calculate_chunk_size(current_size=BASE_CHUNK_SIZE):
    available_memory_gb = get_available_memory()
    if available_memory_gb < MIN_FREE_MEMORY_GB * 1.5:
        return max(10_000, current_size // 2)
    elif available_memory_gb > MIN_FREE_MEMORY_GB * 4:
        return min(200_000, int(current_size * 1.5))
    return current_size


def process_chunk_in_process(args):
    """Process a chunk in a separate process to avoid GIL and improve parallelism."""
    ts_file_path, start_row, chunk_size, jobs_df_dict, chunk_id = args

    # Reconstruct the jobs DataFrame from dictionary
    jobs_df = pl.DataFrame(jobs_df_dict)

    # Read the chunk
    ts_chunk = read_parquet_chunk(ts_file_path, start_row, chunk_size)
    if ts_chunk is None or ts_chunk.is_empty():
        return None

    # Process the chunk
    result = process_chunk(ts_chunk, jobs_df, chunk_id)

    # Clean up
    del ts_chunk
    gc.collect()

    return result


def truncate_long_strings(df: pl.DataFrame, max_length: int = 10000) -> pl.DataFrame:
    """Truncate string columns that exceed a certain length."""
    string_columns = [col for col in df.columns if df.schema[col] in (pl.String, pl.Utf8)]
    if not string_columns:
        return df

    truncate_expressions = []
    truncated_cols = []

    for col_name in string_columns:
        try:
            max_len = df.select(pl.col(col_name).str.len_bytes().max()).item()
            if max_len is not None and max_len > max_length:
                truncated_cols.append(col_name)
                truncate_expressions.append(
                    pl.col(col_name).str.slice(0, max_length).alias(col_name)
                )
        except Exception:
            truncate_expressions.append(
                pl.col(col_name).str.slice(0, max_length).alias(col_name)
            )

    if truncate_expressions:
        logger.info(f"Truncated {len(truncated_cols)} string columns exceeding {max_length} bytes")
        return df.with_columns(truncate_expressions)
    return df


def enforce_polars_schema(df: pl.DataFrame, target_schema: pa.Schema) -> pl.DataFrame:
    """Enforces a PyArrow schema onto a Polars DataFrame, creating missing columns."""
    expressions = []
    existing_columns = df.columns

    for field in target_schema:
        col_name = field.name
        pa_type = field.type

        if col_name in existing_columns:
            current_pl_type = df.schema[col_name]
            try:
                if pa.types.is_timestamp(pa_type):
                    time_unit = pa_type.unit if pa_type.unit else 'ns'
                    time_zone = pa_type.tz
                    target_pl_type = pl.Datetime(time_unit=time_unit, time_zone=time_zone)
                elif pa.types.is_float64(pa_type):
                    target_pl_type = pl.Float64
                elif pa.types.is_float32(pa_type):
                    target_pl_type = pl.Float32
                elif pa.types.is_int64(pa_type):
                    target_pl_type = pl.Int64
                elif pa.types.is_large_string(pa_type) or pa.types.is_string(pa_type):
                    target_pl_type = pl.Utf8
                else:
                    target_pl_type = pl.DataType.from_arrow(pa_type)

                if current_pl_type != target_pl_type:
                    expressions.append(pl.col(col_name).cast(target_pl_type))
                else:
                    expressions.append(pl.col(col_name))
            except Exception:
                expressions.append(pl.col(col_name))
        else:
            try:
                if pa.types.is_timestamp(pa_type):
                    time_unit = pa_type.unit if pa_type.unit else 'ns'
                    time_zone = pa_type.tz
                    pl_type = pl.Datetime(time_unit=time_unit, time_zone=time_zone)
                elif pa.types.is_float64(pa_type):
                    pl_type = pl.Float64
                elif pa.types.is_large_string(pa_type) or pa.types.is_string(pa_type):
                    pl_type = pl.Utf8
                else:
                    pl_type = pl.DataType.from_arrow(pa_type)
                expressions.append(pl.lit(None, dtype=pl_type).alias(col_name))
            except Exception:
                pass

    ordered_expressions = []
    if expressions:
        temp_df_for_selection = df.with_columns(expressions)
    else:
        temp_df_for_selection = df

    for field in target_schema:
        if field.name in temp_df_for_selection.columns:
            ordered_expressions.append(pl.col(field.name))

    if not ordered_expressions:
        polars_schema = {}
        for name, ptype in zip(target_schema.names, target_schema.types):
            try:
                if pa.types.is_timestamp(ptype):
                    time_unit = ptype.unit if ptype.unit else 'ns'
                    polars_schema[name] = pl.Datetime(time_unit=time_unit, time_zone=ptype.tz)
                elif pa.types.is_large_string(ptype) or pa.types.is_string(ptype):
                    polars_schema[name] = pl.Utf8
                else:
                    polars_schema[name] = pl.DataType.from_arrow(ptype)
            except Exception:
                polars_schema[name] = pl.Null
        return pl.DataFrame(schema=polars_schema)

    return temp_df_for_selection.select(ordered_expressions)


def write_daily_parquet(df_to_write: pl.DataFrame, year: str, month: str, day_str: str, output_dir: Path):
    """Write a DataFrame for a specific day to a Parquet file, enforcing schema - OPTIMIZED."""
    parquet_file = output_dir / f"perf_metrics_{year}-{month}-{day_str}.parquet"

    try:
        # Truncate extremely long string values before schema enforcement
        df_to_write = truncate_long_strings(df_to_write, max_length=1000)

        # Enforce the schema
        df_final = enforce_polars_schema(df_to_write, EXPECTED_PA_SCHEMA)
        temp_parquet_file = output_dir / f"temp_{uuid.uuid4().hex}_{parquet_file.name}"

        # OPTIMIZATION: Adjusted Parquet write settings for better performance
        pyarrow_write_options = {
            "compression": "snappy",
            "data_page_size": 128 * 1024,  # Increased from 64KB
            "row_group_size": 100_000,  # Increased from 50
            "data_page_version": "2.0",
            "use_dictionary": ["account", "queue", "jobname", "exitcode", "unit"],
            "write_statistics": True,
            "use_byte_stream_split": ["value_cpuuser", "value_gpu", "value_memused", "value_memused_minus_diskcache",
                                      "value_nfs", "value_block"],  # Better compression for float columns
        }

        # Write to temporary file first
        df_final.write_parquet(
            temp_parquet_file,
            use_pyarrow=True,
            pyarrow_options=pyarrow_write_options
        )

        # Move to final destination
        shutil.move(str(temp_parquet_file), str(parquet_file))
        logger.info(f"✓ Written {len(df_final):,} rows to {parquet_file.name}")

        try:
            os.chmod(parquet_file, 0o644)
        except Exception:
            pass

        return parquet_file

    except Exception as e:
        logger.error(f"✗ Failed to write {parquet_file.name}: {e}")
        return None


def convert_walltime_to_seconds(walltime_series: pl.Series) -> pl.Series:
    """Convert HH:MM:SS or seconds string/numeric to seconds (float)."""

    def converter(wt_val):
        if wt_val is None:
            return None
        if isinstance(wt_val, (int, float)):
            return float(wt_val)
        try:
            wt_str = str(wt_val)
            parts = wt_str.split(':')
            if len(parts) == 3:  # HH:MM:SS
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
            elif len(parts) == 2:  # MM:SS
                return float(parts[0]) * 60 + float(parts[1])
            elif len(parts) == 1:  # Seconds
                return float(parts[0])
            return None
        except ValueError:
            return None

    return walltime_series.map_elements(converter, return_dtype=pl.Float64)


def get_exit_status_description(exit_status_series: pl.Series) -> pl.Series:
    """Generates a human-readable exit status."""
    return pl.when(exit_status_series == 0) \
        .then(pl.lit("COMPLETED")) \
        .when(exit_status_series.is_not_null()) \
        .then(pl.concat_str([pl.lit("FAILED:"), exit_status_series.cast(pl.Utf8)])) \
        .otherwise(pl.lit("UNKNOWN"))


def parse_host_list_single(host_str):
    """Parse single host list value."""
    if host_str is None:
        return None
    try:
        import re
        raw_nodes = re.findall(r'([^\s/+]+)(?:/\d+)?', str(host_str))

        if not raw_nodes and '+' in str(host_str):
            raw_nodes = [part.split('/')[0].strip() for part in str(host_str).split('+') if part.strip()]
        elif not raw_nodes:
            node_candidate = str(host_str).split('/')[0].strip()
            if node_candidate:
                raw_nodes = [node_candidate]

        if raw_nodes:
            unique_nodes = sorted(list(set(node.strip() for node in raw_nodes if node.strip())))
            if unique_nodes:
                suffixed_nodes = [node + "_C" for node in unique_nodes]
                return "{" + ",".join(suffixed_nodes) + "}"
        return None
    except:
        return None


def parse_host_list_single(host_str):
    """Parse single host list value."""
    if host_str is None:
        return None
    try:
        import re
        raw_nodes = re.findall(r'([^\s/+]+)(?:/\d+)?', str(host_str))

        if not raw_nodes and '+' in str(host_str):
            raw_nodes = [part.split('/')[0].strip() for part in str(host_str).split('+') if part.strip()]
        elif not raw_nodes:
            node_candidate = str(host_str).split('/')[0].strip()
            if node_candidate:
                raw_nodes = [node_candidate]

        if raw_nodes:
            unique_nodes = sorted(list(set(node.strip() for node in raw_nodes if node.strip())))
            if unique_nodes:
                suffixed_nodes = [node + "_C" for node in unique_nodes]
                return "{" + ",".join(suffixed_nodes) + "}"
        return None
    except:
        return None


def transform_dataframe_suffixes(df: pl.DataFrame) -> pl.DataFrame:
    """Applies _C suffix to specified columns: jid, host, username."""
    cols_to_suffix = ["jid", "host", "username"]
    result_df = df

    for col_name in cols_to_suffix:
        if col_name in result_df.columns:
            if result_df.schema[col_name] == pl.Utf8 or result_df.schema[col_name] == pl.String:
                result_df = result_df.with_columns(
                    (pl.col(col_name) + "_C").alias(col_name)
                )
            else:
                result_df = result_df.with_columns(
                    (pl.col(col_name).cast(pl.Utf8) + "_C").alias(col_name)
                )

    return result_df


def process_chunk(ts_chunk: pl.DataFrame, jobs_df: pl.DataFrame, chunk_id: int) -> Union[pl.DataFrame, None]:
    """Processes a chunk of metric data against job accounting data."""

    if ts_chunk.is_empty() or jobs_df.is_empty():
        return None

    # Normalize 'Job Id' in ts_chunk to match accounting format
    if "Job Id" in ts_chunk.columns:
        # Add 'job' prefix if the Job Id is numeric only
        ts_chunk = ts_chunk.with_columns(
            pl.when(pl.col("Job Id").str.contains(r"^\d+$"))
            .then(pl.concat_str([pl.lit("job"), pl.col("Job Id")]))
            .otherwise(pl.col("Job Id").str.to_lowercase())
            .alias("Job Id")
        )
    ts_chunk = ts_chunk.with_columns(pl.col("Job Id").cast(pl.Utf8))
    jobs_df = jobs_df.with_columns(pl.col("jobID").cast(pl.Utf8))

    # Join metrics with job accounting data
    joined_df = ts_chunk.join(jobs_df, left_on="Job Id", right_on="jobID", how="inner")

    if joined_df.is_empty():
        return None

    # Ensure we have the required time columns
    time_cols_for_filter = ["Timestamp", "start_time", "end_time"]
    if not all(col in joined_df.columns for col in time_cols_for_filter):
        logger.error(f"Chunk {chunk_id}: Missing required time columns")
        return None

    # Parse and validate time columns
    for col_name in time_cols_for_filter:
        joined_df = parse_datetime_column(joined_df, col_name, tz_aware_target=True)
        if not isinstance(joined_df.schema[col_name], pl.Datetime) or joined_df.schema[col_name].time_zone != "UTC":
            logger.error(f"Chunk {chunk_id}: Invalid time column {col_name}")
            return None

    # Filter metrics to only include those within job execution timeframe
    filtered_by_time = joined_df.filter(
        (pl.col("Timestamp") >= pl.col("start_time")) & (pl.col("Timestamp") <= pl.col("end_time"))
    )

    if filtered_by_time.is_empty():
        return None

    # Create 1-minute time windows
    df_with_windows = filtered_by_time.with_columns(
        pl.col("Timestamp").dt.truncate("1m").alias("time")
    )

    # Get unique job-host-time combinations
    unique_combinations = df_with_windows.select(["Job Id", "Host", "time"]).unique()

    # Process each job-host-time combination separately
    all_results = []
    expected_events = ['cpuuser', 'gpu', 'memused', 'memused_minus_diskcache', 'nfs', 'block']

    for combo_idx, row in enumerate(unique_combinations.iter_rows(named=True)):
        job_id = row["Job Id"]
        host = row["Host"]
        time_window = row["time"]

        # Filter to just this specific job-host-time combination
        single_combo_df = df_with_windows.filter(
            (pl.col("Job Id") == job_id) &
            (pl.col("Host") == host) &
            (pl.col("time") == time_window)
        )

        if single_combo_df.is_empty():
            continue

        # Create a single row with aggregated metrics for this job-host-time
        result_row = {
            "jid": job_id,
            "host": host,
            "time": time_window
        }

        # Aggregate metrics for each event type
        for event in expected_events:
            event_data = single_combo_df.filter(pl.col("Event") == event)
            if not event_data.is_empty():
                avg_value = event_data.select(pl.col("Value").mean()).item()
                result_row[f"value_{event}"] = avg_value
            else:
                result_row[f"value_{event}"] = None

        # Get job information
        first_row = single_combo_df.head(1)
        job_info_source_cols = {
            "submit_time": "qtime",
            "start_time": "start_time",
            "end_time": "end_time",
            "timelimit": "Resource_List.walltime",
            "nhosts": "Resource_List.nodect",
            "ncores": "Resource_List.ncpus",
            "account": "account",
            "queue": "queue",
            "jobname": "jobname",
            "exitcode": "Exit_status",
            "username": "user",
            "host_list": "exec_host"
        }

        for final_alias, source_col in job_info_source_cols.items():
            if source_col in first_row.columns:
                value = first_row.select(pl.col(source_col)).item()
                result_row[final_alias] = value
            else:
                result_row[final_alias] = None

        all_results.append(result_row)

    if not all_results:
        return None

    # Convert results back to DataFrame
    aggregated_df = pl.DataFrame(all_results)

    # Rename gpu_usage to gpu if needed for consistency
    if "value_gpu_usage" in aggregated_df.columns and "value_gpu" not in aggregated_df.columns:
        aggregated_df = aggregated_df.rename({"value_gpu_usage": "value_gpu"})

    # REMOVED: Unit expansion logic that was creating duplicate rows
    # Instead, add a single unit column indicating this is a metrics row
    expanded_df = aggregated_df.with_columns(pl.lit("mixed").alias("unit"))

    if expanded_df.is_empty():
        return None

    # Apply transformations to specific columns
    transformations = []
    if "timelimit" in expanded_df.columns:
        transformations.append(convert_walltime_to_seconds(pl.col("timelimit")).alias("timelimit"))
    if "exitcode" in expanded_df.columns:
        transformations.append(
            get_exit_status_description(pl.col("exitcode").cast(pl.Int64)).alias("exitcode"))
    if "host_list" in expanded_df.columns:
        transformations.append(parse_host_list(pl.col("host_list")).alias("host_list"))
    if "nhosts" in expanded_df.columns:
        transformations.append(pl.col("nhosts").cast(pl.Float64).alias("nhosts"))
    if "ncores" in expanded_df.columns:
        transformations.append(pl.col("ncores").cast(pl.Float64).alias("ncores"))

    final_df = expanded_df
    if transformations:
        final_df = final_df.with_columns(transformations)

    # Apply _C suffixes if needed (currently commented out)
    # final_df = transform_dataframe_suffixes(final_df)

    # Add missing columns as null values to match expected schema
    for col_name in FINAL_COLUMNS:
        if col_name not in final_df.columns:
            field_idx = EXPECTED_PA_SCHEMA.get_field_index(col_name)
            if field_idx != -1:
                pa_type = EXPECTED_PA_SCHEMA.field(field_idx).type
                try:
                    if pa.types.is_timestamp(pa_type):
                        pl_type = pl.Datetime(time_unit=pa_type.unit if pa_type.unit else 'ns', time_zone=pa_type.tz)
                    elif pa.types.is_large_string(pa_type) or pa.types.is_string(pa_type):
                        pl_type = pl.Utf8
                    else:
                        pl_type = pl.DataType.from_arrow(pa_type)
                    final_df = final_df.with_columns(pl.lit(None, dtype=pl_type).alias(col_name))
                except Exception:
                    pass

    # Select columns in the correct order
    ordered_cols_present = [col for col in FINAL_COLUMNS if col in final_df.columns]
    output_df = final_df.select(ordered_cols_present)

    return output_df


def parse_host_list(exec_host_series: pl.Series) -> pl.Series:
    """Parse exec_host into a sorted, comma-separated string with _C suffix."""

    def extract_nodes(host_str: str):
        if host_str is None:
            return None
        try:
            raw_nodes = re.findall(r'([^\s/+]+)(?:/\d+)?', str(host_str))

            if not raw_nodes and '+' in str(host_str):
                raw_nodes = [part.split('/')[0].strip() for part in str(host_str).split('+') if part.strip()]
            elif not raw_nodes:
                if '{' not in str(host_str) and '}' not in str(host_str):
                    node_candidate = str(host_str).split('/')[0].strip()
                    if node_candidate:
                        raw_nodes = [node_candidate]

            if raw_nodes:
                unique_nodes_stripped = sorted(list(set(node.strip() for node in raw_nodes if node.strip())))
                if unique_nodes_stripped:
                    suffixed_nodes = [node + "_C" for node in unique_nodes_stripped]
                    return "{" + ",".join(suffixed_nodes) + "}"
            return None
        except Exception:
            return None

    return exec_host_series.map_elements(extract_nodes, return_dtype=pl.Utf8)


def parse_datetime_column(df: pl.DataFrame, column_name: str, tz_aware_target: bool = True) -> pl.DataFrame:
    if column_name not in df.columns:
        return df

    current_dtype_obj = df.schema[column_name]

    # If already a Polars Datetime
    if isinstance(current_dtype_obj, pl.Datetime):
        target_time_zone = "UTC" if tz_aware_target else None
        if current_dtype_obj.time_zone != target_time_zone:
            if target_time_zone is None:
                return df.with_columns(pl.col(column_name).dt.replace_time_zone(None))
            elif current_dtype_obj.time_zone is None:
                return df.with_columns(pl.col(column_name).dt.replace_time_zone(target_time_zone))
            else:
                return df.with_columns(pl.col(column_name).dt.convert_time_zone(target_time_zone))
        return df

    # If string or object, try parsing
    if current_dtype_obj == pl.Object or current_dtype_obj == pl.Utf8:
        parsed_successfully = False
        target_naive_dt_type = pl.Datetime(time_unit="us")

        formats_to_try = ["%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"]

        temp_col_name = f"_{column_name}_parsed_temp"

        for fmt in formats_to_try:
            try:
                df = df.with_columns(
                    pl.col(column_name).str.strptime(
                        dtype=target_naive_dt_type,
                        format=fmt,
                        strict=False,
                    ).alias(temp_col_name)
                )
                if not (df[temp_col_name].is_null().all() and not df[column_name].is_null().all() and df[
                    column_name].len() > 0):
                    df = df.drop(column_name).rename({temp_col_name: column_name})
                    parsed_successfully = True
                    break
                else:
                    df = df.drop(temp_col_name)
            except Exception:
                df = df.drop(temp_col_name) if temp_col_name in df.columns else df

        if parsed_successfully:
            if tz_aware_target:
                df = df.with_columns(pl.col(column_name).dt.replace_time_zone("UTC"))
            return df
        else:
            try:
                cast_target_dt_type = pl.Datetime(time_unit="us", time_zone="UTC" if tz_aware_target else None)
                df = df.with_columns(pl.col(column_name).cast(cast_target_dt_type))

                if isinstance(df.schema[column_name], pl.Datetime):
                    if tz_aware_target and df.schema[column_name].time_zone is None:
                        df = df.with_columns(pl.col(column_name).dt.replace_time_zone("UTC"))
                else:
                    raise ValueError(f"Cast failed for {column_name}")
                return df
            except Exception:
                null_dt_type = pl.Datetime(time_unit="us", time_zone="UTC" if tz_aware_target else None)
                return df.with_columns(pl.lit(None, dtype=null_dt_type).alias(column_name))

    return df


def read_jobs_df(job_file_path: Path):
    """Reads and prepares job accounting CSV data with optimizations."""
    logger.info(f"📖 Reading job accounting: {job_file_path.name}")
    if not job_file_path.exists():
        logger.error(f"✗ Job accounting file not found: {job_file_path}")
        return None

    try:
        # OPTIMIZATION: Only read columns we actually need
        needed_columns = [
            "jobID", "qtime", "start", "end", "start_time", "end_time",
            "Resource_List.walltime", "Resource_List.nodect", "Resource_List.ncpus",
            "account", "queue", "jobname", "Exit_status", "user", "exec_host"
        ]

        # First, read just the header to get column names
        with open(job_file_path, 'r') as f:
            header = f.readline().strip().split(',')

        # Find which columns we need are actually present
        columns_to_read = [col for col in needed_columns if col in header]

        # Add the record type column (usually the 3rd column)
        if len(header) > 2:
            columns_to_read.append(header[2])

        schema_overrides = {
            "jobID": pl.Utf8,
            "Resource_List.nodect": pl.Float64,
            "Resource_List.ncpus": pl.Float64,
            "Exit_status": pl.Int64,
            "Resource_List.walltime": pl.Utf8,
        }

        df = pl.read_csv(
            job_file_path,
            columns=columns_to_read,  # Only read needed columns
            schema_overrides=schema_overrides,
            infer_schema_length=10000,
            null_values=["NULL", "N/A", "", "NODE578"],
            low_memory=False,  # Trade memory for speed
            rechunk=True  # Ensure data is in contiguous memory
        )

        # Filter to only include 'E' (end) records
        if len(df.columns) > 2:
            record_type_col = header[2]
            if record_type_col in df.columns:
                df = df.filter(pl.col(record_type_col) == "E")

        # Normalize jobID
        if "jobID" in df.columns:
            df = df.with_columns(
                pl.col("jobID").str.replace(r"^jobID", "job", literal=False).alias("jobID")
            )

        # Handle duplicates
        if "jobID" in df.columns:
            total_rows_before = len(df)
            unique_job_ids_before = df.select("jobID").n_unique()

            if total_rows_before != unique_job_ids_before:
                duplicate_count = total_rows_before - unique_job_ids_before
                logger.warning(f"⚠ Found {duplicate_count} duplicate jobID records in {job_file_path.name}")

                if "end" in df.columns or "end_time" in df.columns:
                    end_col = "end" if "end" in df.columns else "end_time"

                    if df.schema[end_col] == pl.Utf8:
                        df = parse_datetime_column(df, end_col, tz_aware_target=True)

                    df = df.sort(["jobID", end_col], descending=[False, True])
                    df = df.unique(subset=["jobID"], keep="first")
                else:
                    df = df.unique(subset=["jobID"], keep="last")

                logger.info(f"✓ Deduplicated to {len(df)} unique job records")

        # Rename columns if needed
        rename_map = {
            "start": "start_time",
            "end": "end_time",
        }
        current_columns = df.columns
        effective_rename_map = {k: v for k, v in rename_map.items() if
                                k in current_columns and v not in current_columns}
        if effective_rename_map:
            df = df.rename(effective_rename_map)

        # Parse datetime columns
        for col_name in ["qtime", "start_time", "end_time"]:
            if col_name in df.columns:
                df = parse_datetime_column(df, col_name, tz_aware_target=True)

        logger.info(f"✓ Loaded {len(df):,} unique job records from {job_file_path.name}")
        return df

    except Exception as e:
        logger.error(f"✗ Error reading job accounting file {job_file_path.name}: {e}")
        return None


def process_ts_file_in_parallel(ts_file_path: Path, jobs_df: pl.DataFrame, year: str, month: str, day: str = None):
    """Process a FRESCO file in parallel chunks - OPTIMIZED VERSION."""
    global terminate_requested, memory_limit_exceeded, current_processing_file

    current_processing_file = ts_file_path.name
    job_identifier = f"{year}-{month}" + (f"-{day}" if day else "")

    daily_data_accumulator = defaultdict(list)

    try:
        # Get file metadata
        try:
            import pyarrow.parquet as pq
            pq_file_meta = pq.read_metadata(ts_file_path)
            total_rows = pq_file_meta.num_rows
        except Exception:
            logger.error(f"✗ Could not read metadata for {ts_file_path.name}")
            total_rows = 0

        if total_rows == 0:
            logger.warning(f"⚠ Empty or unreadable file: {ts_file_path.name}")
            current_processing_file = None
            return True

        # OPTIMIZATION 1: Increase base chunk size for better efficiency
        # With 90GB RAM and sorted data, we can process larger chunks
        optimized_chunk_size = max(100_000, BASE_CHUNK_SIZE * 4)  # Start with 100k rows minimum

        # OPTIMIZATION 2: Adjust chunk size based on available memory
        available_memory_gb = get_available_memory()
        if available_memory_gb > 30:  # If we have plenty of memory
            optimized_chunk_size = 500_000  # Process 500k rows at a time
        elif available_memory_gb > 15:
            optimized_chunk_size = 250_000

        num_chunks = (total_rows + optimized_chunk_size - 1) // optimized_chunk_size

        # OPTIMIZATION 3: Use ProcessPoolExecutor for true parallelism
        # This avoids Python's GIL and allows better CPU utilization
        num_workers = min(MAX_WORKERS, num_chunks)  # Don't create more workers than chunks

        logger.info(
            f"🔄 Processing {ts_file_path.name}: {total_rows:,} rows, {num_chunks} chunks of {optimized_chunk_size:,} rows, {num_workers} workers"
        )

        if jobs_df is None or jobs_df.is_empty():
            logger.error(f"✗ No job data available for {ts_file_path.name}")
            return False

        # Convert jobs_df to dictionary for pickling (ProcessPoolExecutor requirement)
        jobs_df_dict = jobs_df.to_dict(as_series=False)

        # OPTIMIZATION 4: Pre-calculate all chunk parameters
        chunk_args = []
        for i in range(num_chunks):
            if terminate_requested or memory_limit_exceeded:
                break
            start_row = i * optimized_chunk_size
            chunk_args.append((ts_file_path, start_row, optimized_chunk_size, jobs_df_dict, i + 1))

        # OPTIMIZATION 5: Use ProcessPoolExecutor with optimized settings
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Submit all chunks at once
            futures = [executor.submit(process_chunk_in_process, args) for args in chunk_args]

            # Process results with progress bar
            results_pbar = tqdm(as_completed(futures), total=len(futures), desc=f"Processing {ts_file_path.name}",
                                leave=False)

            for future in results_pbar:
                if terminate_requested:
                    # Cancel remaining futures
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    break

                try:
                    result_df = future.result(timeout=FUTURE_TIMEOUT)

                    if result_df is not None and not result_df.is_empty():
                        # OPTIMIZATION 6: Batch accumulation to reduce memory operations
                        # Group by day and accumulate
                        result_df_with_key = result_df.with_columns(
                            pl.col("time").dt.strftime("%Y-%m-%d").alias("_date_str_key")
                        )

                        for day_key_tuple, day_group_df in result_df_with_key.group_by("_date_str_key",
                                                                                       maintain_order=False):
                            day_key = day_key_tuple[0] if isinstance(day_key_tuple, tuple) else day_key_tuple
                            daily_data_accumulator[day_key].append(day_group_df.drop("_date_str_key"))

                        del result_df_with_key
                        del result_df

                except TimeoutError:
                    logger.error(f"✗ Chunk timed out")
                except Exception as e:
                    logger.error(f"✗ Error processing chunk: {e}")

            # Explicitly clean up
            executor.shutdown(wait=True)

        # OPTIMIZATION 7: More efficient daily file writing
        # Write daily files with better memory management
        days_processed_successfully = 0
        if daily_data_accumulator:
            daily_pbar = tqdm(daily_data_accumulator.items(), desc="Writing daily files", leave=False)

            for day_str_key, df_list in daily_pbar:
                if terminate_requested:
                    break

                if df_list:
                    # OPTIMIZATION 8: Use vertical_relaxed for better performance
                    # and rechunk to optimize memory layout
                    full_day_df = pl.concat(df_list, how="vertical_relaxed", rechunk=True)

                    # Clear the list immediately to free memory
                    daily_data_accumulator[day_str_key] = None
                    del df_list
                    gc.collect()

                    try:
                        y, m, d = day_str_key.split('-')
                        parquet_file = write_daily_parquet(full_day_df, y, m, d, OUTPUT_PATH)
                        if parquet_file:
                            days_processed_successfully += 1
                            composer_signal_manager.create_complete_signal(y, m, d)
                    except Exception as e:
                        logger.error(f"✗ Failed to write daily file for {day_str_key}: {e}")

                    del full_day_df
                    gc.collect()

        # Clear accumulator
        daily_data_accumulator.clear()
        gc.collect()

        # Create completion signals
        if day and days_processed_successfully > 0:
            composer_signal_manager.create_complete_signal(year, month, day)
        elif days_processed_successfully > 0:
            composer_signal_manager.create_complete_signal(year, month)

        logger.info(f"✓ Completed {ts_file_path.name}: {days_processed_successfully} daily files written")
        return True

    except Exception as e:
        logger.error(f"✗ Major error processing {ts_file_path.name}: {e}")
        return False
    finally:
        current_processing_file = None
        gc.collect()


def read_parquet_chunk(file_path: Path, start_row: int, chunk_size: int):
    """Read a chunk of parquet file with optimizations."""
    try:
        # OPTIMIZATION: Use columns parameter to read only needed columns
        # This significantly reduces memory usage and I/O
        needed_columns = ["Timestamp", "Job Id", "Host", "Event", "Value"]

        # Use scan_parquet with slice for chunked reading
        df = (
            pl.scan_parquet(
                file_path,
                rechunk=False,  # Don't rechunk, we'll handle it if needed
                low_memory=False,  # Trade memory for speed
            )
            .select(needed_columns)  # Select only needed columns
            .slice(start_row, chunk_size)
            .collect()
        )

        if "Timestamp" in df.columns:
            df = parse_datetime_column(df, "Timestamp", tz_aware_target=True)

        return df

    except Exception as e:
        logger.error(f"✗ Error reading chunk from {file_path.name}: {e}")
        return None


def process_chunk_optimized(ts_chunk: pl.DataFrame, jobs_df: pl.DataFrame, chunk_id: int) -> Union[pl.DataFrame, None]:
    """Highly optimized chunk processor using Polars native operations."""

    start_time = time.time()

    if ts_chunk.is_empty() or jobs_df.is_empty():
        return None

    # Log initial state
    logger.debug(f"Chunk {chunk_id}: Processing {len(ts_chunk):,} metric rows against {len(jobs_df):,} jobs")

    # Step 1: Normalize Job IDs efficiently
    ts_chunk = ts_chunk.with_columns(
        pl.when(pl.col("Job Id").str.contains(r"^\d+$"))
        .then(pl.concat_str([pl.lit("job"), pl.col("Job Id")]))
        .otherwise(pl.col("Job Id").str.to_lowercase())
        .cast(pl.Utf8)
        .alias("Job Id")
    )

    # Ensure jobID is string type in jobs_df
    jobs_df = jobs_df.with_columns(pl.col("jobID").cast(pl.Utf8))

    # Step 2: Efficient join with time filtering in one operation
    # First, let's do the join
    joined_df = ts_chunk.join(jobs_df, left_on="Job Id", right_on="jobID", how="inner")

    if joined_df.is_empty():
        logger.debug(f"Chunk {chunk_id}: No matching jobs found")
        return None

    logger.debug(f"Chunk {chunk_id}: {len(joined_df):,} rows after join")

    # Step 3: Parse timestamps if needed (should already be parsed from read_parquet_chunk)
    if "Timestamp" in joined_df.columns and joined_df.schema["Timestamp"] != pl.Datetime:
        joined_df = parse_datetime_column(joined_df, "Timestamp", tz_aware_target=True)

    # Step 4: Filter to job execution timeframe
    filtered_df = joined_df.filter(
        (pl.col("Timestamp") >= pl.col("start_time")) &
        (pl.col("Timestamp") <= pl.col("end_time"))
    )

    if filtered_df.is_empty():
        logger.debug(f"Chunk {chunk_id}: No metrics within job timeframes")
        return None

    logger.debug(f"Chunk {chunk_id}: {len(filtered_df):,} rows after time filtering")

    # Step 5: Create time windows and pivot metrics in one operation
    # This is the key optimization - do everything in one group_by operation
    result_df = (
        filtered_df
        .with_columns([
            pl.col("Timestamp").dt.truncate("1m").alias("time"),
            pl.col("Job Id").alias("jid"),
            pl.col("Host").alias("host")
        ])
        .group_by(["jid", "host", "time"])
        .agg([
            # Pivot metrics
            pl.when(pl.col("Event") == "cpuuser").then(pl.col("Value")).mean().alias("value_cpuuser"),
            pl.when(pl.col("Event") == "gpu").then(pl.col("Value")).mean().alias("value_gpu"),
            pl.when(pl.col("Event") == "memused").then(pl.col("Value")).mean().alias("value_memused"),
            pl.when(pl.col("Event") == "memused_minus_diskcache").then(pl.col("Value")).mean().alias(
                "value_memused_minus_diskcache"),
            pl.when(pl.col("Event") == "nfs").then(pl.col("Value")).mean().alias("value_nfs"),
            pl.when(pl.col("Event") == "block").then(pl.col("Value")).mean().alias("value_block"),

            # Job metadata (take first since they're all the same within a group)
            pl.col("qtime").first().alias("submit_time"),
            pl.col("start_time").first(),
            pl.col("end_time").first(),
            pl.col("Resource_List.walltime").first().alias("timelimit"),
            pl.col("Resource_List.nodect").first().cast(pl.Float64).alias("nhosts"),
            pl.col("Resource_List.ncpus").first().cast(pl.Float64).alias("ncores"),
            pl.col("account").first(),
            pl.col("queue").first(),
            pl.col("jobname").first(),
            pl.col("Exit_status").first().alias("exitcode"),
            pl.col("user").first().alias("username"),
            pl.col("exec_host").first().alias("host_list")
        ])
    )

    if result_df.is_empty():
        return None

    logger.debug(f"Chunk {chunk_id}: {len(result_df):,} aggregated rows")

    # Step 6: Apply transformations efficiently
    transformations = []

    if "timelimit" in result_df.columns:
        transformations.append(
            pl.col("timelimit")
            .map_elements(convert_walltime_to_seconds_single, return_dtype=pl.Float64)
            .alias("timelimit")
        )

    if "exitcode" in result_df.columns:
        transformations.append(
            pl.when(pl.col("exitcode") == 0)
            .then(pl.lit("COMPLETED"))
            .when(pl.col("exitcode").is_not_null())
            .then(pl.concat_str([pl.lit("FAILED:"), pl.col("exitcode").cast(pl.Utf8)]))
            .otherwise(pl.lit("UNKNOWN"))
            .alias("exitcode")
        )

    if "host_list" in result_df.columns:
        transformations.append(
            pl.col("host_list")
            .map_elements(parse_host_list_single, return_dtype=pl.Utf8)
            .alias("host_list")
        )

    if transformations:
        result_df = result_df.with_columns(transformations)

    # Step 7: FIXED - Add single unit column instead of creating 5 duplicate rows
    # OLD CODE that created 5x data:
    # units = ["CPU %", "GPU %", "GB", "GB/s", "MB/s"]
    # n_rows = len(result_df)
    # expanded_df = pl.concat([
    #     result_df.with_columns(pl.lit(unit).alias("unit"))
    #     for unit in units
    # ])

    # NEW CODE - just add a single unit column:
    expanded_df = result_df.with_columns(pl.lit("mixed").alias("unit"))

    # Step 8: Add missing columns to match schema
    for col_name in FINAL_COLUMNS:
        if col_name not in expanded_df.columns:
            # Determine the appropriate null type based on schema
            if col_name in ["submit_time", "start_time", "end_time", "time"]:
                expanded_df = expanded_df.with_columns(
                    pl.lit(None, dtype=pl.Datetime(time_unit="us", time_zone="UTC")).alias(col_name)
                )
            elif col_name in ["timelimit", "nhosts", "ncores", "value_cpuuser", "value_gpu",
                              "value_memused", "value_memused_minus_diskcache", "value_nfs", "value_block"]:
                expanded_df = expanded_df.with_columns(
                    pl.lit(None, dtype=pl.Float64).alias(col_name)
                )
            else:
                expanded_df = expanded_df.with_columns(
                    pl.lit(None, dtype=pl.Utf8).alias(col_name)
                )

    # Select columns in correct order
    final_df = expanded_df.select([col for col in FINAL_COLUMNS if col in expanded_df.columns])

    elapsed = time.time() - start_time
    logger.debug(f"Chunk {chunk_id}: Processed in {elapsed:.2f}s, output {len(final_df):,} rows")

    if elapsed > 10:
        logger.warning(f"Chunk {chunk_id}: Slow processing - took {elapsed:.2f}s")

    return final_df


# Helper functions for single value processing
def convert_walltime_to_seconds_single(wt_val):
    """Convert single walltime value to seconds."""
    if wt_val is None:
        return None
    if isinstance(wt_val, (int, float)):
        return float(wt_val)
    try:
        wt_str = str(wt_val)
        parts = wt_str.split(':')
        if len(parts) == 3:
            return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2:
            return float(parts[0]) * 60 + float(parts[1])
        elif len(parts) == 1:
            return float(parts[0])
        return None
    except:
        return None


def process_single_chunk_wrapper(ts_file_path, start_row, chunk_size, jobs_df, chunk_id):
    """Wrapper to read a chunk and then process it."""
    if terminate_requested or memory_limit_exceeded:
        return None

    ts_chunk = read_parquet_chunk(ts_file_path, start_row, chunk_size)
    if ts_chunk is None or ts_chunk.is_empty():
        return None

    processed_df = process_chunk(ts_chunk, jobs_df, chunk_id)
    del ts_chunk
    gc.collect()
    return processed_df


def process_single_chunk_wrapper_optimized(ts_file_path, start_row, chunk_size, jobs_df, chunk_id):
    """Optimized wrapper to read and process a chunk."""
    if terminate_requested or memory_limit_exceeded:
        return None

    try:
        # Read the chunk
        ts_chunk = read_parquet_chunk_with_timing(ts_file_path, start_row, chunk_size)
        if ts_chunk is None or ts_chunk.is_empty():
            return None

        # Process with optimized function
        processed_df = process_chunk_optimized(ts_chunk, jobs_df, chunk_id)

        # Clean up
        del ts_chunk
        gc.collect()

        return processed_df

    except Exception as e:
        logger.error(f"Error in chunk {chunk_id}: {e}")
        import traceback
        traceback.print_exc()
        return None


def discover_metric_files():
    """Find all FRESCO metric files and their corresponding accounting files."""
    logger.info("🔍 Discovering FRESCO metric files...")

    # Dictionary to group files/directories by year-month
    year_month_groups = {}

    # Check if the metrics directory exists
    if not PROC_METRIC_PATH.exists():
        logger.error(f"✗ Metrics directory does not exist: {PROC_METRIC_PATH}")
        return []
    
    # First, check if there are parquet files directly in the metrics directory
    direct_parquet_files = list(PROC_METRIC_PATH.glob("*.parquet"))
    if direct_parquet_files:
        logger.info(f"📁 Found {len(direct_parquet_files)} parquet files directly in metrics directory")
        # Show first few filenames as examples
        example_files = [f.name for f in direct_parquet_files[:5]]
        logger.info(f"📄 Example files: {', '.join(example_files)}{'...' if len(direct_parquet_files) > 5 else ''}")
    else:
        # Check what files ARE in the directory
        all_files = list(PROC_METRIC_PATH.glob("*"))
        logger.info(f"📂 No parquet files found directly. Directory contains {len(all_files)} items")
        if all_files:
            example_items = [f.name for f in all_files[:10]]
            logger.info(f"📋 Directory contents (first 10): {', '.join(example_items)}{'...' if len(all_files) > 10 else ''}")
    
    # Group direct files by year-month extracted from filename
    for parquet_file in direct_parquet_files:
            # Try to extract year-month from filename
            # Pattern: FRESCO_Conte_ts_YYYY-MM_v1_YYYYMMDD_HHMMSS.parquet
            file_match = re.search(r'FRESCO_Conte_ts_(\d{4})-(\d{2})_v', parquet_file.name)
            if file_match:
                year, month = file_match.groups()
                year_month_key = f"{year}-{month}"
                
                # Check for corresponding accounting file
                accounting_file = JOB_ACCOUNTING_PATH / f"{year}-{month}.csv"
                if not accounting_file.exists():
                    logger.warning(f"⚠ No accounting file found for {year}-{month}: {accounting_file}")
                    continue
                
                if year_month_key not in year_month_groups:
                    year_month_groups[year_month_key] = {
                        "year": year,
                        "month": month,
                        "files": [],
                        "directories": [],
                        "accounting_file": accounting_file
                    }
                
                year_month_groups[year_month_key]["files"].append(parquet_file)
                logger.debug(f"📄 Added {parquet_file.name} to {year_month_key}")
            else:
                logger.warning(f"⚠ Could not extract year-month from filename: {parquet_file.name}")

    # Also look for subdirectories in the metrics path
    # These directories contain the actual daily parquet files
    for metric_dir in PROC_METRIC_PATH.glob("*"):
        if not metric_dir.is_dir():
            continue

        # Extract year-month from directory name
        # Pattern: sorted_FRESCO_Conte_ts_YYYY-MM_v1_YYYYMMDD_HHMMSS or similar
        # Also handle chunked versions: sorted_FRESCO_Conte_ts_YYYY-MM_v1_YYYYMMDD_HHMMSS_chunk_000
        dir_match = re.search(r'sorted_FRESCO_Conte_ts_(\d{4})-(\d{2})_v', metric_dir.name)
        if not dir_match:
            # Try old pattern: YYYY-MM
            dir_match = re.match(r'(\d{4})-(\d{2})', metric_dir.name)
        
        if dir_match:
            year, month = dir_match.groups()
            year_month_key = f"{year}-{month}"
            
            # Check for corresponding accounting file
            accounting_file = JOB_ACCOUNTING_PATH / f"{year}-{month}.csv"
            if not accounting_file.exists():
                logger.warning(f"⚠ No accounting file found for {year}-{month}: {accounting_file}")
                continue

            # Look for daily parquet files in this directory
            daily_files = list(metric_dir.glob("*.parquet"))
            if daily_files:
                logger.info(f"📁 Found {len(daily_files)} daily files in {metric_dir.name}")
                
                # Group directories by year-month
                if year_month_key not in year_month_groups:
                    year_month_groups[year_month_key] = {
                        "year": year,
                        "month": month,
                        "files": [],
                        "directories": [],
                        "accounting_file": accounting_file
                    }
                
                year_month_groups[year_month_key]["directories"].append(metric_dir)
            else:
                logger.warning(f"⚠ No parquet files found in {metric_dir.name}")

    # Convert grouped files/directories to metric_files list
    metric_files = []
    for year_month_key, group_info in year_month_groups.items():
        files = group_info["files"]
        directories = group_info["directories"]
        total_files = len(files) + sum(len(list(d.glob("*.parquet"))) for d in directories)
        
        logger.info(f"📊 {year_month_key}: {len(files)} direct files + {len(directories)} directories = {total_files} total files")
        
        metric_files.append({
            "year": group_info["year"],
            "month": group_info["month"],
            "day": None,
            "metric_files": files,  # Direct parquet files
            "metric_directories": directories,  # Directories containing parquet files
            "accounting_file": group_info["accounting_file"]
        })

    logger.info(f"📊 Found {len(metric_files)} year-month groups to process")
    return metric_files


def signal_handler(sig, frame):
    global terminate_requested
    logger.warning(f"🛑 Signal {sig} received. Requesting graceful termination...")
    terminate_requested = True


def main_direct_processing():
    """Main function for direct processing of all files without waiting for signals."""
    global terminate_requested

    logger.info("🚀 Starting direct processing mode...")

    # Define months to skip
    SKIP_MONTHS = {}

    # Discover all metric files
    metric_files = discover_metric_files()

    if not metric_files:
        logger.warning("⚠ No metric files found for processing. Exiting.")
        return

    # Filter out files from months we want to skip
    original_count = len(metric_files)
    metric_files = [
        job_details for job_details in metric_files
        if f"{job_details['year']}-{job_details['month']}" not in SKIP_MONTHS
    ]
    skipped_count = original_count - len(metric_files)

    if skipped_count > 0:
        logger.info(f"⏭ Skipping {skipped_count} files from months: {', '.join(sorted(SKIP_MONTHS))}")

    if not metric_files:
        logger.warning("⚠ No metric files remaining after filtering. Exiting.")
        return

    # Process each metric file with progress bar
    logger.info(f"📈 Processing {len(metric_files)} metric files...")

    main_pbar = tqdm(metric_files, desc="Overall Progress",
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')

    for idx, job_details in enumerate(main_pbar, 1):
        if terminate_requested:
            logger.info("🛑 Termination requested. Stopping processing.")
            break

        year = job_details["year"]
        month = job_details["month"]
        day = job_details["day"]
        metric_file = job_details["metric_file"]
        accounting_file = job_details["accounting_file"]

        job_id = f"{year}-{month}" if not day else f"{year}-{month}-{day}"

        # Update progress bar description
        main_pbar.set_description(f"Processing {job_id}")

        # Check if output file already exists
        if day:
            output_file = OUTPUT_PATH / f"perf_metrics_{year}-{month}-{day}.parquet"
            if output_file.exists():
                logger.info(f"⏭ Skipping {job_id}: output already exists")
                continue
        else:
            existing_daily_files = list(OUTPUT_PATH.glob(f"perf_metrics_{year}-{month}-*.parquet"))
            if existing_daily_files:
                logger.info(f"⏭ Skipping {year}-{month}: {len(existing_daily_files)} daily files exist")
                continue

        # Read job accounting data
        jobs_df = read_jobs_df(accounting_file)
        if jobs_df is None or jobs_df.is_empty():
            logger.error(f"✗ Failed to read job accounting for {job_id}")
            composer_signal_manager.create_failed_signal(year, month, day, "Failed to read accounting data")
            continue

        # Process the metric file
        success = process_ts_file_in_parallel(metric_file, jobs_df, year, month, day)

        # Cleanup
        del jobs_df
        gc.collect()

        if success:
            logger.info(f"✅ Successfully processed {job_id}")
        else:
            logger.error(f"❌ Failed to process {job_id}")
            composer_signal_manager.create_failed_signal(year, month, day, "Processing failed")

    main_pbar.close()
    logger.info("🎉 Completed direct processing of all metric files.")


def read_parquet_chunk_with_timing(file_path: Path, start_row: int, chunk_size: int):
    """Read a chunk of parquet file with timing diagnostics."""
    start_time = time.time()
    try:
        # OPTIMIZATION: Use columns parameter to read only needed columns
        needed_columns = ["Timestamp", "Job Id", "Host", "Event", "Value"]

        logger.debug(f"Reading chunk: start_row={start_row}, chunk_size={chunk_size}")

        # Try a more direct approach first
        df = pl.read_parquet(
            file_path,
            columns=needed_columns,
            use_pyarrow=False,  # Use native Polars reader
            rechunk=False,
            low_memory=False
        ).slice(start_row, chunk_size)

        read_time = time.time() - start_time
        logger.debug(f"Chunk read in {read_time:.2f}s: {len(df)} rows")

        if "Timestamp" in df.columns:
            parse_start = time.time()
            df = parse_datetime_column(df, "Timestamp", tz_aware_target=True)
            logger.debug(f"Timestamp parsing took {time.time() - parse_start:.2f}s")

        total_time = time.time() - start_time
        if total_time > 5:
            logger.warning(f"Slow chunk read: {total_time:.2f}s for {chunk_size} rows")

        return df

    except Exception as e:
        logger.error(f"✗ Error reading chunk from {file_path.name}: {e}")
        return None


def process_chunk_simple(args):
    """Simplified chunk processor for debugging."""
    ts_file_path, start_row, chunk_size, jobs_df_dict, chunk_id = args

    start_time = time.time()
    logger.info(f"Starting chunk {chunk_id}")

    # Read the chunk
    ts_chunk = read_parquet_chunk_with_timing(ts_file_path, start_row, chunk_size)
    if ts_chunk is None or ts_chunk.is_empty():
        logger.warning(f"Chunk {chunk_id} is empty")
        return None

    logger.info(f"Chunk {chunk_id} read complete: {len(ts_chunk)} rows in {time.time() - start_time:.2f}s")

    # For now, just return some basic info to test the pipeline
    return pl.DataFrame({
        "chunk_id": [chunk_id],
        "rows_processed": [len(ts_chunk)],
        "time": [pl.datetime_range(
            datetime.now(),
            datetime.now(),
            interval="1m",
            time_zone="UTC"
        )[0]]
    })


def process_ts_file_sequential_test(ts_file_path: Path, jobs_df: pl.DataFrame, year: str, month: str, day: str = None):
    """Test processing with sequential execution for debugging."""
    global terminate_requested, memory_limit_exceeded, current_processing_file

    current_processing_file = ts_file_path.name

    try:
        # Get file metadata
        import pyarrow.parquet as pq
        pq_file_meta = pq.read_metadata(ts_file_path)
        total_rows = pq_file_meta.num_rows

        logger.info(f"File has {total_rows:,} rows")

        # Test with smaller chunks first
        test_chunk_size = 10_000
        num_test_chunks = min(5, (total_rows + test_chunk_size - 1) // test_chunk_size)

        logger.info(f"Testing with {num_test_chunks} chunks of {test_chunk_size} rows")

        # Process a few chunks sequentially to test
        for i in range(num_test_chunks):
            start_row = i * test_chunk_size
            logger.info(f"Processing test chunk {i + 1}/{num_test_chunks}")

            chunk_start = time.time()
            ts_chunk = read_parquet_chunk_with_timing(ts_file_path, start_row, test_chunk_size)

            if ts_chunk is not None:
                logger.info(f"Test chunk {i + 1} completed in {time.time() - chunk_start:.2f}s")
                logger.info(f"Chunk shape: {ts_chunk.shape}")
                logger.info(f"Columns: {ts_chunk.columns}")

                # Show sample data
                if i == 0:
                    logger.info(f"Sample data:\n{ts_chunk.head(5)}")
            else:
                logger.error(f"Failed to read test chunk {i + 1}")

        return True

    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


# Alternative: Use ThreadPoolExecutor with shared memory
def process_ts_file_with_threads(ts_file_path: Path, jobs_df: pl.DataFrame, year: str, month: str, day: str = None):
    """Process using threads instead of processes to avoid serialization overhead."""
    global terminate_requested, memory_limit_exceeded, current_processing_file

    current_processing_file = ts_file_path.name
    daily_data_accumulator = defaultdict(list)

    try:
        # Get file metadata
        import pyarrow.parquet as pq
        pq_file_meta = pq.read_metadata(ts_file_path)
        total_rows = pq_file_meta.num_rows

        if total_rows == 0:
            logger.warning(f"⚠ Empty file: {ts_file_path.name}")
            return True

        # Use larger chunks
        chunk_size = max(250_000, BASE_CHUNK_SIZE * 2)
        num_chunks = (total_rows + chunk_size - 1) // chunk_size

        # Limit workers for thread pool
        num_workers = min(8, MAX_WORKERS // 4, num_chunks)  # Use fewer threads

        logger.info(
            f"🔄 Processing {ts_file_path.name}: {total_rows:,} rows, {num_chunks} chunks of {chunk_size:,} rows, {num_workers} threads"
        )

        # Process chunks with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = []

            # Submit all chunks
            for i in range(num_chunks):
                if terminate_requested or memory_limit_exceeded:
                    break
                start_row = i * chunk_size
                future = executor.submit(
                    process_single_chunk_wrapper,
                    ts_file_path, start_row, chunk_size, jobs_df, i + 1
                )
                futures.append(future)

            # Process results
            completed = 0
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"Processing {ts_file_path.name}"):
                if terminate_requested:
                    break

                try:
                    result_df = future.result(timeout=300)  # 5 minute timeout
                    completed += 1

                    if result_df is not None and not result_df.is_empty():
                        # Process results as before...
                        result_df_with_key = result_df.with_columns(
                            pl.col("time").dt.strftime("%Y-%m-%d").alias("_date_str_key")
                        )

                        for day_key_tuple, day_group_df in result_df_with_key.group_by("_date_str_key",
                                                                                       maintain_order=False):
                            day_key = day_key_tuple[0] if isinstance(day_key_tuple, tuple) else day_key_tuple
                            daily_data_accumulator[day_key].append(day_group_df.drop("_date_str_key"))

                        del result_df_with_key
                        del result_df

                    if completed % 10 == 0:
                        logger.info(f"Progress: {completed}/{num_chunks} chunks completed")
                        gc.collect()

                except Exception as e:
                    logger.error(f"✗ Error processing chunk: {e}")

        # Write daily files as before...
        days_processed_successfully = 0
        if daily_data_accumulator:
            for day_str_key, df_list in tqdm(daily_data_accumulator.items(), desc="Writing daily files"):
                if terminate_requested:
                    break
                if df_list:
                    full_day_df = pl.concat(df_list, how="vertical_relaxed", rechunk=True)
                    daily_data_accumulator[day_str_key] = None
                    del df_list
                    gc.collect()

                    try:
                        y, m, d = day_str_key.split('-')
                        parquet_file = write_daily_parquet(full_day_df, y, m, d, OUTPUT_PATH)
                        if parquet_file:
                            days_processed_successfully += 1
                            composer_signal_manager.create_complete_signal(y, m, d)
                    except Exception as e:
                        logger.error(f"✗ Failed to write daily file for {day_str_key}: {e}")

                    del full_day_df
                    gc.collect()

        logger.info(f"✓ Completed {ts_file_path.name}: {days_processed_successfully} daily files written")
        return True

    except Exception as e:
        logger.error(f"✗ Major error processing {ts_file_path.name}: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        current_processing_file = None
        gc.collect()


def process_ts_file_fast(items_to_process, jobs_df: pl.DataFrame, year: str, month: str, day: str = None):
    """Fast, simplified processing using optimized chunk processing."""
    global terminate_requested, memory_limit_exceeded, current_processing_file

    daily_data_accumulator = defaultdict(list)

    try:
        # Handle a list of mixed files and directories
        if isinstance(items_to_process, list):
            # Separate files and directories
            direct_files = [item for item in items_to_process if item.is_file()]
            directories = [item for item in items_to_process if item.is_dir()]
            
            # Count total files for logging
            total_direct_files = len(direct_files)
            total_dirs = len(directories)
            total_files_in_dirs = sum(len(list(d.glob("*.parquet"))) for d in directories)
            total_files = total_direct_files + total_files_in_dirs
            
            logger.info(f"📁 Processing {total_direct_files} direct files + {total_dirs} directories = {total_files} total files for {year}-{month}")
            
            # Process direct files first
            for file_idx, parquet_file in enumerate(direct_files, 1):
                if terminate_requested or memory_limit_exceeded:
                    break
                
                current_processing_file = parquet_file.name
                logger.info(f"📄 Processing direct file {file_idx}/{total_direct_files}: {parquet_file.name}")
                
                success = process_single_parquet_file(parquet_file, jobs_df, year, month, daily_data_accumulator)
                if not success:
                    logger.warning(f"⚠ Failed to process {parquet_file.name}")
            
            # Process directories
            for dir_idx, metric_dir in enumerate(directories, 1):
                if terminate_requested or memory_limit_exceeded:
                    break
                
                current_processing_file = metric_dir.name
                parquet_files = list(metric_dir.glob("*.parquet"))
                
                logger.info(f"📂 Processing directory {dir_idx}/{total_dirs}: {metric_dir.name} ({len(parquet_files)} files)")
                
                # Process each parquet file in the directory
                for parquet_file in parquet_files:
                    if terminate_requested or memory_limit_exceeded:
                        break
                    
                    logger.debug(f"📄 Processing file: {parquet_file.name}")
                    success = process_single_parquet_file(parquet_file, jobs_df, year, month, daily_data_accumulator)
                    if not success:
                        logger.warning(f"⚠ Failed to process {parquet_file.name}")
            
            # Write accumulated daily files for all items
            days_processed_successfully = write_accumulated_daily_files(daily_data_accumulator, year, month)
            
            logger.info(f"✓ Completed {total_direct_files} files + {total_dirs} directories for {year}-{month}: {days_processed_successfully} daily files written")
            return True
            
        # Handle single directory or file (backward compatibility)
        elif isinstance(items_to_process, Path):
            current_processing_file = items_to_process.name
            
            if items_to_process.is_dir():
                # Process all parquet files in the directory
                parquet_files = list(items_to_process.glob("*.parquet"))
                if not parquet_files:
                    logger.warning(f"⚠ No parquet files found in directory: {items_to_process.name}")
                    return True
                
                logger.info(f"📁 Processing {len(parquet_files)} files in {items_to_process.name}")
                
                # Process each parquet file in the directory
                for parquet_file in parquet_files:
                    if terminate_requested or memory_limit_exceeded:
                        break
                    
                    logger.debug(f"📄 Processing file: {parquet_file.name}")
                    success = process_single_parquet_file(parquet_file, jobs_df, year, month, daily_data_accumulator)
                    if not success:
                        logger.warning(f"⚠ Failed to process {parquet_file.name}")
                
                # Write accumulated daily files
                days_processed_successfully = write_accumulated_daily_files(daily_data_accumulator, year, month)
                
                logger.info(f"✓ Completed directory {items_to_process.name}: {days_processed_successfully} daily files written")
                return True
            else:
                # Process single file (original logic)
                success = process_single_parquet_file(items_to_process, jobs_df, year, month, daily_data_accumulator)
                if success:
                    days_processed_successfully = write_accumulated_daily_files(daily_data_accumulator, year, month)
                    logger.info(f"✓ Completed file {items_to_process.name}: {days_processed_successfully} daily files written")
                return success
        else:
            logger.error(f"✗ Unexpected input type: {type(items_to_process)}")
            return False

    except Exception as e:
        logger.error(f"✗ Major error processing: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        current_processing_file = None
        gc.collect()


def process_single_parquet_file(parquet_file: Path, jobs_df: pl.DataFrame, year: str, month: str, daily_data_accumulator: dict):
    """Process a single parquet file and accumulate results."""
    try:
        # Get file metadata
        import pyarrow.parquet as pq
        pq_file_meta = pq.read_metadata(parquet_file)
        total_rows = pq_file_meta.num_rows

        if total_rows == 0:
            logger.warning(f"⚠ Empty file: {parquet_file.name}")
            return True

        # Use reasonable chunk size based on available memory
        available_memory_gb = get_available_memory()
        if available_memory_gb > 50:
            chunk_size = 100_000  # 100k rows per chunk
        elif available_memory_gb > 20:
            chunk_size = 50_000  # 50k rows per chunk
        else:
            chunk_size = 25_000  # 25k rows per chunk

        num_chunks = (total_rows + chunk_size - 1) // chunk_size

        # Use ThreadPoolExecutor with reasonable number of workers
        num_workers = min(8, os.cpu_count() // 2)  # Don't overwhelm the system

        logger.info(
            f"🔄 Processing {parquet_file.name}: {total_rows:,} rows, {num_chunks} chunks of {chunk_size:,} rows, {num_workers} workers"
        )

        if jobs_df is None or jobs_df.is_empty():
            logger.error(f"✗ No job data available for {parquet_file.name}")
            return False

        # Process chunks
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit chunks in batches to avoid overwhelming memory
            batch_size = num_workers * 2
            processed_chunks = 0

            with tqdm(total=num_chunks, desc=f"Processing {parquet_file.name}", unit="chunks") as pbar:
                for batch_start in range(0, num_chunks, batch_size):
                    if terminate_requested or memory_limit_exceeded:
                        break

                    # Submit a batch of chunks
                    batch_end = min(batch_start + batch_size, num_chunks)
                    futures = []

                    for i in range(batch_start, batch_end):
                        start_row = i * chunk_size
                        future = executor.submit(
                            process_single_chunk_wrapper_optimized,
                            parquet_file, start_row, chunk_size, jobs_df, i + 1
                        )
                        futures.append(future)

                    # Process batch results
                    for future in as_completed(futures):
                        if terminate_requested:
                            break

                        try:
                            result_df = future.result(timeout=120)  # 2 minute timeout per chunk

                            if result_df is not None and not result_df.is_empty():
                                # Group by day and accumulate
                                result_df_with_key = result_df.with_columns(
                                    pl.col("time").dt.strftime("%Y-%m-%d").alias("_date_str_key")
                                )

                                for day_key_tuple, day_group_df in result_df_with_key.group_by("_date_str_key",
                                                                                               maintain_order=False):
                                    day_key = day_key_tuple[0] if isinstance(day_key_tuple, tuple) else day_key_tuple
                                    daily_data_accumulator[day_key].append(day_group_df.drop("_date_str_key"))

                                del result_df_with_key
                                del result_df

                            processed_chunks += 1
                            pbar.update(1)

                            # Log progress every 10%
                            if processed_chunks % max(1, num_chunks // 10) == 0:
                                logger.info(
                                    f"Progress: {processed_chunks}/{num_chunks} chunks ({processed_chunks * 100 // num_chunks}%)")
                                gc.collect()

                        except TimeoutError:
                            logger.error(f"✗ Chunk timed out")
                            pbar.update(1)
                        except Exception as e:
                            logger.error(f"✗ Error processing chunk: {e}")
                            pbar.update(1)

                    # Clear futures to free memory
                    futures.clear()
                    gc.collect()

        if terminate_requested or memory_limit_exceeded:
            logger.warning(f"⚠ Processing interrupted for {parquet_file.name}")
            return False

        return True

    except Exception as e:
        logger.error(f"✗ Major error processing {parquet_file.name}: {e}")
        import traceback
        traceback.print_exc()
        return False


def write_accumulated_daily_files(daily_data_accumulator: dict, year: str, month: str):
    """Write accumulated daily data to parquet files."""
    days_processed_successfully = 0
    
    if daily_data_accumulator:
        logger.info(f"Writing {len(daily_data_accumulator)} daily files...")

        for day_str_key, df_list in tqdm(daily_data_accumulator.items(), desc="Writing daily files"):
            if terminate_requested:
                break

            if df_list:
                # Concatenate all dataframes for this day
                full_day_df = pl.concat(df_list, how="vertical_relaxed", rechunk=True)

                # Clear the list immediately
                daily_data_accumulator[day_str_key] = None
                del df_list
                gc.collect()

                try:
                    y, m, d = day_str_key.split('-')
                    parquet_file = write_daily_parquet(full_day_df, y, m, d, OUTPUT_PATH)
                    if parquet_file:
                        days_processed_successfully += 1
                        composer_signal_manager.create_complete_signal(y, m, d)
                except Exception as e:
                    logger.error(f"✗ Failed to write daily file for {day_str_key}: {e}")

                del full_day_df
                gc.collect()

    # Clear accumulator
    daily_data_accumulator.clear()
    gc.collect()

    # Create completion signals
    if days_processed_successfully > 0:
        composer_signal_manager.create_complete_signal(year, month)

    return days_processed_successfully


def main_direct_processing_diagnostic():
    """Main function with diagnostics to identify performance issues."""
    global terminate_requested

    logger.info("🚀 Starting direct processing mode with diagnostics...")

    # Set Polars to use all available threads
    pl.Config.set_tbl_rows(20)

    # Discover all metric files
    metric_files = discover_metric_files()

    if not metric_files:
        logger.warning("⚠ No metric files found for processing. Exiting.")
        return

    # Process each metric file
    logger.info(f"📈 Processing {len(metric_files)} metric files...")

    # Test with just the first file
    if True:  # Set to False to process all files
        logger.info("🔬 Running in diagnostic mode - processing only first file")
        metric_files = metric_files[:1]

    for idx, job_details in enumerate(metric_files, 1):
        if terminate_requested:
            logger.info("🛑 Termination requested. Stopping processing.")
            break

        year = job_details["year"]
        month = job_details["month"]
        day = job_details["day"]
        metric_file = job_details["metric_file"]
        accounting_file = job_details["accounting_file"]

        job_id = f"{year}-{month}" if not day else f"{year}-{month}-{day}"

        logger.info(f"\n{'=' * 60}")
        logger.info(f"Processing {idx}/{len(metric_files)}: {job_id}")
        logger.info(f"Metric file: {metric_file.name}")
        logger.info(f"File size: {metric_file.stat().st_size / (1024 ** 3):.2f} GB")

        # Check if output already exists
        if day:
            output_file = OUTPUT_PATH / f"perf_metrics_{year}-{month}-{day}.parquet"
            if output_file.exists():
                logger.info(f"⏭ Skipping {job_id}: output already exists")
                continue

        # Read job accounting data with timing
        job_read_start = time.time()
        jobs_df = read_jobs_df(accounting_file)
        job_read_time = time.time() - job_read_start
        logger.info(f"Job accounting read took {job_read_time:.2f}s")

        if jobs_df is None or jobs_df.is_empty():
            logger.error(f"✗ Failed to read job accounting for {job_id}")
            continue

        logger.info(f"Jobs DataFrame shape: {jobs_df.shape}")
        logger.info(f"Memory usage: {get_memory_usage():.2f} GB")
        logger.info(f"Available memory: {get_available_memory():.2f} GB")

        # First, run a sequential test to see if basic reading works
        logger.info("\n🧪 Running sequential read test...")
        test_success = process_ts_file_sequential_test(metric_file, jobs_df, year, month, day)

        if not test_success:
            logger.error("Sequential test failed! Skipping this file.")
            continue

        # Try different processing methods
        process_start = time.time()

        # Option 1: Try thread-based processing (usually faster for I/O bound tasks)
        logger.info("\n🔄 Attempting thread-based processing...")
        success = process_ts_file_with_threads(metric_file, jobs_df, year, month, day)

        # Option 2: If threads fail, try the original process-based approach
        # Uncomment to test:
        # if not success:
        #     logger.info("\n🔄 Attempting process-based processing...")
        #     success = process_ts_file_in_parallel(metric_file, jobs_df, year, month, day)

        process_time = time.time() - process_start

        # Cleanup
        del jobs_df
        gc.collect()

        if success:
            logger.info(f"✅ Successfully processed {job_id} in {process_time:.2f}s")
        else:
            logger.error(f"❌ Failed to process {job_id}")

        logger.info(f"Final memory usage: {get_memory_usage():.2f} GB")
        logger.info(f"{'=' * 60}\n")

    logger.info("🎉 Diagnostic processing complete.")


def main_direct_processing_fast():
    """Main function using the optimized fast processor."""
    global terminate_requested

    logger.info("🚀 Starting fast processing mode...")

    # Set Polars to use all available threads for operations
    pl.Config.set_tbl_rows(20)

    # Discover all metric files
    metric_files = discover_metric_files()

    if not metric_files:
        logger.warning("⚠ No metric files found for processing. Exiting.")
        return

    logger.info(f"📈 Processing {len(metric_files)} metric files...")

    main_pbar = tqdm(metric_files, desc="Overall Progress",
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')

    for idx, job_details in enumerate(main_pbar, 1):
        if terminate_requested:
            logger.info("🛑 Termination requested. Stopping processing.")
            break

        year = job_details["year"]
        month = job_details["month"]
        day = job_details["day"]
        metric_files = job_details.get("metric_files", [])
        metric_directories = job_details.get("metric_directories", [])
        accounting_file = job_details["accounting_file"]

        job_id = f"{year}-{month}" if not day else f"{year}-{month}-{day}"

        # Update progress bar description
        main_pbar.set_description(f"Processing {job_id}")

        # Check if output already exists
        if day:
            output_file = OUTPUT_PATH / f"perf_metrics_{year}-{month}-{day}.parquet"
            if output_file.exists():
                logger.info(f"⏭ Skipping {job_id}: output already exists")
                continue
        else:
            # For monthly files, check if we have any daily outputs
            existing_daily_files = list(OUTPUT_PATH.glob(f"perf_metrics_{year}-{month}-*.parquet"))
            if existing_daily_files:
                logger.info(f"⏭ Skipping {year}-{month}: {len(existing_daily_files)} daily files exist")
                continue

        # Read job accounting data
        total_files = len(metric_files)
        total_dirs = len(metric_directories)
        if total_dirs > 0:
            dir_names = [d.name for d in metric_directories]
            logger.info(f"Processing {job_id} from {total_files} direct files + {total_dirs} directories: {', '.join(dir_names[:3])}{'...' if len(dir_names) > 3 else ''}")
        else:
            logger.info(f"Processing {job_id} from {total_files} direct parquet files")
        
        jobs_df = read_jobs_df(accounting_file)
        if jobs_df is None or jobs_df.is_empty():
            logger.error(f"✗ Failed to read job accounting for {job_id}")
            composer_signal_manager.create_failed_signal(year, month, day, "Failed to read accounting data")
            continue

        # Process the metric files and directories
        process_start = time.time()
        
        # Create combined list for processing
        items_to_process = []
        if metric_files:
            items_to_process.extend(metric_files)  # Direct files
        if metric_directories:
            items_to_process.extend(metric_directories)  # Directories
        
        success = process_ts_file_fast(items_to_process, jobs_df, year, month, day)
        process_time = time.time() - process_start

        # Cleanup
        del jobs_df
        gc.collect()

        if success:
            logger.info(f"✅ Successfully processed {job_id} in {process_time / 60:.1f} minutes")
        else:
            logger.error(f"❌ Failed to process {job_id}")
            composer_signal_manager.create_failed_signal(year, month, day, "Processing failed")

    main_pbar.close()
    logger.info("🎉 Completed processing all metric files.")


# Add this to your script to use the diagnostic version
if __name__ == "__main__":
    setup_signal_handlers()
    logger.info(f"🎯 Direct Processing Consumer Script Started. PID: {os.getpid()}")
    logger.info(f"📦 Polars: {pl.__version__}, PyArrow: {pa.__version__}")
    logger.info(f"📂 Output: {OUTPUT_PATH}")
    logger.info(f"📊 Input FRESCO: {PROC_METRIC_PATH}")
    logger.info(f"📋 Job Accounting: {JOB_ACCOUNTING_PATH}")
    logger.info(f"⚙️  Workers: {MAX_WORKERS}, Chunk size: {BASE_CHUNK_SIZE:,}")

    try:
        # main_direct_processing()
        # main_direct_processing_diagnostic()
        main_direct_processing_fast()
    except KeyboardInterrupt:
        logger.info("⌨️  Processing interrupted by KeyboardInterrupt.")
    finally:
        logger.info("🔚 Direct processing consumer script shutting down.")
        for handler in logging.getLogger().handlers:
            try:
                handler.flush()
                handler.close()
            except Exception:
                pass
