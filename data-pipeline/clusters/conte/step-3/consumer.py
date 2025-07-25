import polars as pl
from pathlib import Path
import logging
import concurrent.futures
import os
import time
import signal
import sys
from typing import Union

# --- Configuration ---
ROOT_DIR = Path("./input")  # Use current directory for testing
OUTPUT_DIR = Path("./output")
# Adjust based on your system's cores, leave some for the OS
# os.cpu_count() can give the total number of logical cores
# Start with slightly less than os.cpu_count() and adjust based on performance
NUM_WORKERS = 2

# Global flag for graceful shutdown
shutdown_requested = False

# Create logs directory if it doesn't exist
Path("logs").mkdir(exist_ok=True)

# Configure logging for better tracking (include process ID for parallel runs)
log_file = "logs/conte-final-cleanup.log"
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file),
                        logging.StreamHandler()
                    ]
                    )


def signal_handler(signum, frame):
    """Handle SIGINT (Ctrl+C) gracefully"""
    global shutdown_requested
    if not shutdown_requested:
        shutdown_requested = True
        print("\n\n--- Graceful shutdown requested (Ctrl+C detected) ---")
        print("Waiting for current tasks to complete...")
        print("Press Ctrl+C again to force quit (not recommended)")
        logging.info("Graceful shutdown requested via SIGINT")
    else:
        print("\n\nForce quit requested. Exiting immediately...")
        logging.warning("Force quit requested via second SIGINT")
        sys.exit(1)


# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)


def transform_dataframe(df: pl.DataFrame) -> tuple[pl.DataFrame, bool]:
    """
    Applies the required transformations to the DataFrame columns.
    Returns the transformed DataFrame and a boolean indicating if relevant columns were found.
    """
    transformations = []
    relevant_columns_found = False

    # Define the expected column order to match the goal format
    expected_column_order = [
        "time", "submit_time", "start_time", "end_time", "timelimit", "nhosts", "ncores",
        "account", "queue", "host", "jid", "unit", "jobname", "exitcode", "host_list",
        "username", "value_cpuuser", "value_gpu", "value_memused",
        "value_memused_minus_diskcache", "value_nfs", "value_block"
    ]

    # Columns that should get "_C" suffix (based on goal format analysis)
    append_c_cols = ["account", "queue", "host", "jid", "jobname", "host_list", "username"]

    # 1. Add missing 'unit' column if it doesn't exist
    if "unit" not in df.columns:
        relevant_columns_found = True
        # Add unit column with empty string as default
        transformations.append(pl.lit("").alias("unit"))
        logging.debug("Added missing 'unit' column.")

    # 2. Transform columns that need "_C" suffix
    for col_name in append_c_cols:
        if col_name in df.columns:
            relevant_columns_found = True
            # Ensure column is treated as string if it's not already
            if df[col_name].dtype != pl.Utf8:
                if col_name == "jid":
                    # Special handling for jid column - only capitalize lowercase "job" to "JOB", don't add extra "_C"
                    transformations.append(
                        pl.col(col_name).cast(pl.Utf8)
                        .str.replace_all("JOBjob", "JOB")  # Fix the duplication issue
                        .alias(col_name)
                    )
                elif col_name == "host_list":
                    # Transform host_list: add _C to hostnames and filter out non-hostname entries
                    logging.debug("Applying host_list transformation (may take longer for large strings)")
                    transformations.append(
                        pl.col(col_name).cast(pl.Utf8)
                        # First remove any existing _C suffixes to prevent duplication
                        .str.replace_all(r"([A-Z]+\d+)_C", r"${1}")
                        # Then add _C to all hostnames (letters followed by numbers)
                        .str.replace_all(r"([A-Z]+\d+)", r"${1}_C")
                        # Remove non-hostname entries (negative numbers with optional _C)
                        .str.replace_all(r"[{,]\s*-\d+(_C)?\s*,?", ",")  # Remove in middle
                        .str.replace_all(r"^-\d+(_C)?\s*,?\s*", "")      # Remove at start
                        .str.replace_all(r",\s*-\d+(_C)?\s*$", "")       # Remove at end
                        .str.replace_all(r",,+", ",")  # Clean up multiple commas
                        .str.replace_all(r"^,|,$", "")  # Remove leading/trailing commas
                        .alias(col_name)
                    )
                else:
                    # Only add "_C" if the column doesn't already end with "_C"
                    transformations.append(
                        pl.when(pl.col(col_name).cast(pl.Utf8).str.ends_with("_C"))
                        .then(pl.col(col_name).cast(pl.Utf8))  # Keep as-is if already ends with _C
                        .otherwise(pl.col(col_name).cast(pl.Utf8) + "_C")  # Add _C if not
                        .alias(col_name)
                    )
            else:
                if col_name == "jid":
                    # Special handling for jid column - only capitalize lowercase "job" to "JOB", don't add extra "_C"
                    transformations.append(
                        pl.col(col_name)
                        .str.replace_all("JOBjob", "JOB")  # Fix the duplication issue
                        .alias(col_name)
                    )
                elif col_name == "host_list":
                    # Transform host_list: add _C to hostnames and filter out non-hostname entries
                    logging.debug("Applying host_list transformation (may take longer for large strings)")
                    transformations.append(
                        pl.col(col_name)
                        # First remove any existing _C suffixes to prevent duplication
                        .str.replace_all(r"([A-Z]+\d+)_C", r"${1}")
                        # Then add _C to all hostnames (letters followed by numbers)
                        .str.replace_all(r"([A-Z]+\d+)", r"${1}_C")
                        # Remove non-hostname entries (negative numbers with optional _C)
                        .str.replace_all(r"[{,]\s*-\d+(_C)?\s*,?", ",")  # Remove in middle
                        .str.replace_all(r"^-\d+(_C)?\s*,?\s*", "")      # Remove at start
                        .str.replace_all(r",\s*-\d+(_C)?\s*$", "")       # Remove at end
                        .str.replace_all(r",,+", ",")  # Clean up multiple commas
                        .str.replace_all(r"^,|,$", "")  # Remove leading/trailing commas
                        .alias(col_name)
                    )
                else:
                    # Only add "_C" if the column doesn't already end with "_C"
                    transformations.append(
                        pl.when(pl.col(col_name).str.ends_with("_C"))
                        .then(pl.col(col_name))  # Keep as-is if already ends with _C
                        .otherwise(pl.col(col_name) + "_C")  # Add _C if not
                        .alias(col_name)
                    )
            logging.debug(f"Added '{col_name}' transformation with _C suffix.")

    # 3. Clean exitcode column by removing non-letter characters
    if "exitcode" in df.columns:
        relevant_columns_found = True
        # Remove non-letter characters from exitcode column (e.g., "FAILED:-11" becomes "FAILED")
        transformations.append(
            pl.col("exitcode").cast(pl.Utf8)
            .str.replace_all(r"[^A-Za-z]", "")
            .alias("exitcode")
        )
        logging.debug("Added 'exitcode' transformation to remove non-letter characters.")

    # 4. Keep all other columns as-is
    other_cols = [col for col in df.columns if col not in append_c_cols and col != "exitcode"]
    for col_name in other_cols:
        transformations.append(pl.col(col_name))

    # Apply all transformations if any were added
    if transformations:
        try:
            df_transformed = df.with_columns(transformations)

            # 4. Reorder columns to match expected format (only include columns that exist)
            available_columns = [col for col in expected_column_order if col in df_transformed.columns]
            # Add any columns that might not be in the expected order at the end
            remaining_columns = [col for col in df_transformed.columns if col not in available_columns]
            final_column_order = available_columns + remaining_columns

            if final_column_order != list(df_transformed.columns):
                df_transformed = df_transformed.select(final_column_order)
                logging.debug("Reordered columns to match expected format.")

            return df_transformed, relevant_columns_found
        except Exception as e:
            logging.error(f"Error applying transformations: {e}")
            return df, relevant_columns_found
    else:
        logging.info("No transformations needed.")

    return df, relevant_columns_found


def get_output_path(input_file_path: Path, input_root: Path, output_root: Path) -> Path:
    """
    Generate the output file path by maintaining the relative directory structure.

    Args:
        input_file_path: Path to the input file
        input_root: Root input directory
        output_root: Root output directory

    Returns:
        Path to the output file
    """
    # Get the relative path from the input root
    relative_path = input_file_path.relative_to(input_root)
    # Create the corresponding path in the output directory
    output_path = output_root / relative_path
    return output_path


# --- Function to Process a Single File (for parallel execution) ---
def process_single_file(file_path: Path) -> tuple[str, Union[str, None]]:
    """
    Reads, transforms, and writes a single parquet file to the output directory.
    Returns a tuple: (status, error_message | None).
    Status can be 'processed', 'skipped', 'error'.
    """
    thread_start_time = time.monotonic()
    logging.debug(f"Starting processing: {file_path.name}")
    try:
        # Read the Parquet file using Polars' Rust implementation (usually default)
        # Consider low_memory=True ONLY if hitting memory limits, it can be slower.
        df = pl.read_parquet(file_path)

        # Apply transformations
        # No need to clone, we'll decide whether to write based on relevant_columns_found
        df_modified, relevant_columns_found = transform_dataframe(df)

        # Only write if relevant columns were found (meaning transformations were attempted)
        if relevant_columns_found:
            # Generate output file path maintaining directory structure
            output_file_path = get_output_path(file_path, ROOT_DIR, OUTPUT_DIR)

            # Ensure the output directory exists
            output_file_path.parent.mkdir(parents=True, exist_ok=True)

            # Write the processed data to the output file
            df_modified.write_parquet(output_file_path, compression='zstd', compression_level=3)

            duration = time.monotonic() - thread_start_time
            logging.debug(
                f"Successfully processed and wrote: {file_path.name} -> {output_file_path.name} in {duration:.2f}s")
            return 'processed', None
        else:
            # Even if no transformations were needed, copy the file to maintain consistency
            output_file_path = get_output_path(file_path, ROOT_DIR, OUTPUT_DIR)

            # Ensure the output directory exists
            output_file_path.parent.mkdir(parents=True, exist_ok=True)

            # Write the original data to the output file
            df.write_parquet(output_file_path, compression='zstd', compression_level=3)

            duration = time.monotonic() - thread_start_time
            logging.debug(
                f"Copied file (no transformations needed): {file_path.name} -> {output_file_path.name} in {duration:.2f}s")
            return 'skipped', None

    # FIX: Replaced ArrowError with ComputeError for compatibility with modern Polars versions.
    # ComputeError is the general exception for failures during query execution, including I/O errors.
    except pl.exceptions.ComputeError as e:
        logging.error(f"Polars computation/IO error processing {file_path.name}: {e}")
        return 'error', str(e)
    except pl.exceptions.SchemaError as e:
        logging.error(f"Polars schema error processing {file_path.name}: {e}")
        return 'error', str(e)
    except FileNotFoundError:
        logging.error(f"File not found during processing: {file_path.name}")
        return 'error', "File not found"
    except PermissionError:
        logging.error(f"Permission denied for file: {file_path.name}")
        return 'error', "Permission denied"
    except Exception as e:
        # Catch other potential errors
        logging.error(f"An unexpected error occurred processing {file_path.name}: {e}",
                      exc_info=False)  # exc_info=False reduces log noise
        return 'error', str(e)


# --- Main Processing Logic ---
def main():
    """
    Finds parquet files and processes them in parallel.
    """
    global shutdown_requested  # <-- FIX: Declare intent to use the global variable
    start_time = time.monotonic()

    if not ROOT_DIR.is_dir():
        logging.error(f"Root directory not found or is not a directory: {ROOT_DIR}")
        return

    # Create output directory if it doesn't exist
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.info(f"Output directory ensured: {OUTPUT_DIR}")

    logging.info(f"Starting processing in directory: {ROOT_DIR} using {NUM_WORKERS} workers.")
    logging.info(f"Output will be written to: {OUTPUT_DIR}")

    # Use rglob to recursively find all .parquet files - this itself is usually fast
    try:
        parquet_files = list(ROOT_DIR.rglob("*.parquet"))
    except Exception as e:
        logging.error(f"Error finding files in {ROOT_DIR}: {e}")
        return

    if not parquet_files:
        logging.warning(f"No .parquet files found in {ROOT_DIR} or its subdirectories.")
        return

    total_files = len(parquet_files)
    logging.info(f"Found {total_files} .parquet files to process.")

    processed_count = 0
    skipped_count = 0
    error_count = 0
    errors_list = []  # Keep track of specific errors if needed

    # Use ProcessPoolExecutor for parallel processing
    # The 'with' statement ensures the pool is properly shut down
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            # Submit all tasks and get future objects
            # Using submit instead of map for better control over shutdown
            futures = [executor.submit(process_single_file, file_path) for file_path in parquet_files]

            # Process results as they complete
            try:
                for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
                    # Check for shutdown
                    if shutdown_requested:
                        logging.info("Shutdown requested. Cancelling remaining tasks...")
                        # Cancel pending futures
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break

                    try:
                        # Short timeout allows this loop to remain responsive to the shutdown flag
                        result = future.result(timeout=1)
                        status, error_message = result
                        if status == 'processed':
                            processed_count += 1
                        elif status == 'skipped':
                            skipped_count += 1
                        elif status == 'error':
                            error_count += 1
                            # Optionally store file path with error:
                            # errors_list.append((parquet_files[i-1], error_message))

                        # Log progress periodically
                        if i % 100 == 0 or i == total_files:  # Log every 100 files or at the end
                            logging.info(f"Progress: {i}/{total_files} files checked...")

                    except concurrent.futures.TimeoutError:
                        # This is expected. It just means no future finished within the timeout.
                        # Continue to the next loop iteration to check the shutdown flag again.
                        continue
                    except Exception as e:
                        error_count += 1
                        logging.error(f"Error processing future: {e}")

            except KeyboardInterrupt:
                # This block is a safeguard. The signal handler should catch SIGINT first,
                # but this makes the code more robust.
                logging.info("KeyboardInterrupt caught in processing loop")
                shutdown_requested = True

    except Exception as e:
        # This will now correctly catch other errors without triggering the UnboundLocalError
        logging.error(f"An error occurred in the main processing pool: {e}")
        return

    end_time = time.monotonic()
    total_duration = end_time - start_time

    logging.info("--- Processing Summary ---")
    logging.info(f"Total files found: {total_files}")
    logging.info(f"Files successfully processed and written to output: {processed_count}")
    logging.info(f"Files copied to output (no transformations needed): {skipped_count}")
    logging.info(f"Files with errors: {error_count}")
    logging.info(f"Total execution time: {total_duration:.2f} seconds")
    logging.info(f"Output directory: {OUTPUT_DIR}")

    if shutdown_requested:
        logging.info("Processing interrupted by user request (Ctrl+C)")
        print("Processing interrupted gracefully. Completed tasks have been saved.")
    else:
        logging.info("Processing finished successfully.")
        print("Processing completed successfully!")
        print(f"All processed files are available in: {OUTPUT_DIR}")


# --- Script Execution ---
if __name__ == "__main__":
    # Make sure the root directory path is correct
    if not ROOT_DIR.exists():
        print(f"ERROR: The specified root directory does not exist: {ROOT_DIR}")
        print("Please update the ROOT_DIR variable in the script.")
    else:
        main()