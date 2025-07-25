import os
import pandas as pd
import argparse
from pathlib import Path
import logging
import multiprocessing as mp
from functools import partial
import time
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pa_csv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import json
import re

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def get_processed_folders(status_file_path='processing_status.json'):
    """
    Read the processing_status.json file and return the list of processed folders.
    
    Args:
        status_file_path (str): Path to the processing status JSON file
        
    Returns:
        set: Set of processed folder names (YYYY-MM format)
    """
    try:
        if not Path(status_file_path).exists():
            logging.warning(f"Processing status file {status_file_path} not found. No folders will be processed.")
            return set()
            
        with open(status_file_path, 'r') as f:
            status_data = json.load(f)
            
        processed_folders = status_data.get('processed_folders', [])
        logging.info(f"Found {len(processed_folders)} processed folders in status file")
        
        return set(processed_folders)
        
    except Exception as e:
        logging.error(f"Error reading processing status file {status_file_path}: {str(e)}")
        return set()

def extract_month_from_filename(filename):
    """
    Extract YYYY-MM month pattern from CSV filename.
    
    Args:
        filename (str): CSV filename
        
    Returns:
        str or None: YYYY-MM format string if found, None otherwise
    """
    # Look for YYYY-MM pattern in filename
    match = re.search(r'(\d{4}-\d{2})', filename)
    return match.group(1) if match else None

def filter_csv_files_by_processed_months(csv_files, processed_folders):
    """
    Filter CSV files to only include those from processed months.
    
    Args:
        csv_files (list): List of Path objects for CSV files
        processed_folders (set): Set of processed folder names (YYYY-MM format)
        
    Returns:
        list: Filtered list of CSV files from processed months
    """
    filtered_files = []
    
    for csv_file in csv_files:
        month = extract_month_from_filename(csv_file.name)
        if month and month in processed_folders:
            filtered_files.append(csv_file)
            logging.debug(f"Including {csv_file.name} (month: {month})")
        elif month:
            logging.debug(f"Skipping {csv_file.name} (month: {month} not in processed folders)")
        else:
            logging.debug(f"Skipping {csv_file.name} (no month pattern found)")
    
    logging.info(f"Filtered {len(csv_files)} CSV files down to {len(filtered_files)} from processed months")
    return filtered_files

class CSVConversionHandler(FileSystemEventHandler):
    """Handles CSV file creation events and converts them to parquet."""
    
    def __init__(self, output_dir=None, delete_csv=True, chunk_size=100000, use_pyarrow=True, wait_delay=180, status_file_path='processing_status.json'):
        self.output_dir = output_dir
        self.delete_csv = delete_csv
        self.chunk_size = chunk_size
        self.use_pyarrow = use_pyarrow
        self.wait_delay = wait_delay
        self.status_file_path = status_file_path
        logging.info(f"CSV conversion handler initialized")
        logging.info(f"Output directory: {output_dir or 'same as input'}")
        logging.info(f"Delete CSV after conversion: {delete_csv}")
        logging.info(f"Chunk size: {chunk_size:,}")
        logging.info(f"Using {'PyArrow' if use_pyarrow else 'Pandas'} for conversion")
        logging.info(f"Will process ALL CSV files (no date filtering)")
    
    def on_created(self, event):
        """Called when a file is created."""
        if event.is_directory:
            return
        
        file_path = Path(event.src_path)
        
        # Only process CSV files
        if file_path.suffix.lower() != '.csv':
            return
        
        logging.info(f"New CSV file detected: {file_path.name}")
        
        # Wait 3 minutes to allow the file writing process to complete
        logging.info(f"Waiting {self.wait_delay} seconds before processing {file_path.name}...")
        time.sleep(self.wait_delay)
        
        # Convert the file
        parquet_file = convert_csv_to_parquet_chunked(
            str(file_path), 
            self.output_dir, 
            self.chunk_size, 
            self.use_pyarrow
        )
        
        if parquet_file and self.delete_csv:
            try:
                file_path.unlink()
                logging.info(f"Deleted original CSV file: {file_path}")
            except Exception as e:
                logging.error(f"Error deleting {file_path}: {str(e)}")

def process_existing_csv_files(directory_path, handler):
    """Process any existing CSV files in the directory at startup."""
    directory = Path(directory_path)
    logging.info(f"Checking for existing CSV files in {directory}")
    
    existing_csvs = [f for f in directory.iterdir() if f.is_file() and f.suffix.lower() == '.csv']
    
    if not existing_csvs:
        logging.info("No existing CSV files found to process.")
        return
    
    logging.info(f"Found {len(existing_csvs)} existing CSV files to process.")
    for csv_file in existing_csvs:
        # Create a mock event to reuse the existing handler logic
        mock_event = type('MockEvent', (), {'src_path': str(csv_file), 'is_directory': False})
        handler.on_created(mock_event)

def convert_csv_to_parquet_chunked(csv_file_path, output_dir=None, chunk_size=100000, use_pyarrow=True):
    """
    Convert a large CSV file to Parquet format using chunked processing.
    
    Args:
        csv_file_path (str): Path to the CSV file
        output_dir (str): Output directory for parquet file (optional)
        chunk_size (int): Number of rows to process at a time
        use_pyarrow (bool): Use PyArrow for faster processing
    
    Returns:
        str: Path to the created parquet file, or None if conversion failed
    """
    try:
        start_time = time.time()
        csv_path = Path(csv_file_path)
        
        # Determine output path
        if output_dir:
            output_path = Path(output_dir) / f"{csv_path.stem}.parquet"
        else:
            output_path = csv_path.with_suffix('.parquet')
        
        # Create output directory if it doesn't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if use_pyarrow:
            # Try PyArrow first, fall back to Pandas if it fails
            try:
                # PyArrow method - fastest for large files
                parse_options = pa_csv.ParseOptions(delimiter=',')
                read_options = pa_csv.ReadOptions(use_threads=True, block_size=chunk_size * 1024)
                convert_options = pa_csv.ConvertOptions(auto_dict_encode=True)
                
                table = pa_csv.read_csv(
                    csv_file_path,
                    parse_options=parse_options,
                    read_options=read_options,
                    convert_options=convert_options
                )
                
                pq.write_table(table, output_path, compression='snappy')
            except Exception as pyarrow_error:
                logging.warning(f"PyArrow failed for {csv_file_path}: {pyarrow_error}")
                logging.info(f"Falling back to Pandas for {csv_file_path}")
                
                # Fallback to Pandas with error handling
                try:
                    df = pd.read_csv(csv_file_path, on_bad_lines='skip', low_memory=False)
                    df.to_parquet(output_path, index=False, compression='snappy')
                except Exception as pandas_error:
                    logging.error(f"Both PyArrow and Pandas failed for {csv_file_path}: {pandas_error}")
                    raise pandas_error
        else:
            # Pandas chunked method - more memory efficient
            first_chunk = True
            
            for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size, low_memory=False):
                # Optimize data types to reduce memory usage
                chunk = optimize_dtypes(chunk)
                
                if first_chunk:
                    # Write first chunk with schema
                    chunk.to_parquet(output_path, index=False, compression='snappy')
                    first_chunk = False
                else:
                    # Append subsequent chunks
                    chunk.to_parquet(output_path, index=False, compression='snappy', append=True)
        
        end_time = time.time()
        file_size_mb = csv_path.stat().st_size / (1024 * 1024)
        
        logging.info(f"Successfully converted {csv_file_path} ({file_size_mb:.1f}MB) to {output_path} in {end_time - start_time:.2f}s")
        return str(output_path)
        
    except Exception as e:
        logging.error(f"Error converting {csv_file_path}: {str(e)}")
        return None

def optimize_dtypes(df):
    """
    Optimize DataFrame data types to reduce memory usage.
    
    Args:
        df (pd.DataFrame): DataFrame to optimize
    
    Returns:
        pd.DataFrame: Optimized DataFrame
    """
    for col in df.columns:
        col_type = df[col].dtype
        
        if col_type != 'object':
            c_min = df[col].min()
            c_max = df[col].max()
            
            if str(col_type)[:3] == 'int':
                if c_min > pd.iinfo(pd.Int8Dtype()).min and c_max < pd.iinfo(pd.Int8Dtype()).max:
                    df[col] = df[col].astype(pd.Int8Dtype())
                elif c_min > pd.iinfo(pd.Int16Dtype()).min and c_max < pd.iinfo(pd.Int16Dtype()).max:
                    df[col] = df[col].astype(pd.Int16Dtype())
                elif c_min > pd.iinfo(pd.Int32Dtype()).min and c_max < pd.iinfo(pd.Int32Dtype()).max:
                    df[col] = df[col].astype(pd.Int32Dtype())
                    
            elif str(col_type)[:5] == 'float':
                if c_min > pd.finfo(pd.Float32Dtype()).min and c_max < pd.finfo(pd.Float32Dtype()).max:
                    df[col] = df[col].astype(pd.Float32Dtype())
        else:
            # Convert string columns to category if they have low cardinality
            num_unique_values = len(df[col].unique())
            num_total_values = len(df[col])
            if num_unique_values / num_total_values < 0.5:
                df[col] = df[col].astype('category')
    
    return df

def convert_single_file_worker(csv_file_path, output_dir, chunk_size, use_pyarrow, delete_csv):
    """
    Worker function for multiprocessing conversion.
    
    Args:
        csv_file_path (str): Path to CSV file
        output_dir (str): Output directory
        chunk_size (int): Chunk size for processing
        use_pyarrow (bool): Use PyArrow for conversion
        delete_csv (bool): Delete CSV after conversion
    
    Returns:
        tuple: (csv_file_path, parquet_file_path, success)
    """
    parquet_file = convert_csv_to_parquet_chunked(
        csv_file_path, output_dir, chunk_size, use_pyarrow
    )
    
    if parquet_file and delete_csv:
        try:
            Path(csv_file_path).unlink()
            logging.info(f"Deleted original CSV file: {csv_file_path}")
        except Exception as e:
            logging.error(f"Error deleting {csv_file_path}: {str(e)}")
    
    return (csv_file_path, parquet_file, parquet_file is not None)

def convert_directory_csv_to_parquet(directory_path, output_dir=None, delete_csv=True, 
                                   chunk_size=100000, use_pyarrow=True, max_workers=None, status_file_path='processing_status.json'):
    """
    Convert all CSV files in a directory to Parquet format using multiprocessing.
    
    Args:
        directory_path (str): Path to directory containing CSV files
        output_dir (str): Output directory for parquet files (optional)
        delete_csv (bool): Whether to delete CSV files after conversion
        chunk_size (int): Number of rows to process at a time
        use_pyarrow (bool): Use PyArrow for faster processing
        max_workers (int): Maximum number of worker processes (default: CPU count)
        status_file_path (str): Path to processing_status.json file
    """
    directory = Path(directory_path)
    
    if not directory.exists():
        logging.error(f"Directory {directory_path} does not exist")
        return
    
    if not directory.is_dir():
        logging.error(f"{directory_path} is not a directory")
        return
    
    # Find all CSV files in the directory
    csv_files = list(directory.glob("*.csv"))
    
    if not csv_files:
        logging.info(f"No CSV files found in {directory_path}")
        return
    
    if max_workers is None:
        max_workers = min(mp.cpu_count(), len(csv_files))
    
    logging.info(f"Found {len(csv_files)} CSV files to convert")
    logging.info(f"Using {max_workers} worker processes")
    logging.info(f"Chunk size: {chunk_size:,} rows")
    logging.info(f"Using {'PyArrow' if use_pyarrow else 'Pandas'} for conversion")
    
    start_time = time.time()
    
    # Create worker function with fixed arguments
    worker_func = partial(
        convert_single_file_worker,
        output_dir=output_dir,
        chunk_size=chunk_size,
        use_pyarrow=use_pyarrow,
        delete_csv=delete_csv
    )
    
    # Process files in parallel
    with mp.Pool(processes=max_workers) as pool:
        results = pool.map(worker_func, [str(f) for f in csv_files])
    
    # Analyze results
    converted_files = []
    failed_conversions = []
    
    for csv_path, parquet_path, success in results:
        if success:
            converted_files.append((csv_path, parquet_path))
        else:
            failed_conversions.append(csv_path)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Summary
    logging.info(f"Conversion complete in {total_time:.2f}s!")
    logging.info(f"Successfully converted: {len(converted_files)} files")
    logging.info(f"Average time per file: {total_time/len(csv_files):.2f}s")
    
    if failed_conversions:
        logging.info(f"Failed conversions: {len(failed_conversions)} files")
        for failed_file in failed_conversions:
            logging.info(f"  - {failed_file}")

def monitor_directory_continuously(directory_path, output_dir=None, delete_csv=True, 
                                 chunk_size=100000, use_pyarrow=True, wait_delay=180, status_file_path='processing_status.json'):
    """
    Continuously monitor a directory for CSV files and convert them to Parquet.
    
    Args:
        directory_path (str): Path to directory to monitor
        output_dir (str): Output directory for parquet files (optional)
        delete_csv (bool): Whether to delete CSV files after conversion
        chunk_size (int): Number of rows to process at a time
        use_pyarrow (bool): Use PyArrow for faster processing
        wait_delay (int): Seconds to wait before processing a new file
        status_file_path (str): Path to processing_status.json file
    """
    directory = Path(directory_path)
    
    if not directory.exists():
        logging.error(f"Directory {directory_path} does not exist")
        return
    
    if not directory.is_dir():
        logging.error(f"{directory_path} is not a directory")
        return
    
    # Create output directory if specified
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
    
    # Create event handler
    event_handler = CSVConversionHandler(
        output_dir=output_dir,
        delete_csv=delete_csv,
        chunk_size=chunk_size,
        use_pyarrow=use_pyarrow,
        wait_delay=wait_delay,
        status_file_path=status_file_path
    )
    
    # Process any existing CSV files
    process_existing_csv_files(directory_path, event_handler)
    
    # Set up file system observer
    observer = Observer()
    observer.schedule(event_handler, path=str(directory), recursive=False)
    
    logging.info(f"Starting continuous monitoring of {directory}")
    logging.info("Press Ctrl+C to stop...")
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping monitor due to KeyboardInterrupt...")
        observer.stop()
    except Exception as e:
        logging.exception("An unexpected error occurred in the main loop:")
        observer.stop()
    finally:
        observer.join()
        logging.info("Monitor stopped.")

def main():
    """Main function to handle command line arguments and execute conversion."""
    parser = argparse.ArgumentParser(description="Convert CSV files to Parquet format (optimized for large files)")
    parser.add_argument("directory", nargs='?', default="./output", 
                       help="Directory containing CSV files to convert (default: ./output)")
    parser.add_argument("-o", "--output", help="Output directory for parquet files (default: same as input)")
    parser.add_argument("--keep-csv", action="store_true", help="Keep original CSV files (don't delete them)")
    parser.add_argument("--chunk-size", type=int, default=100000, help="Chunk size for processing large files (default: 100,000)")
    parser.add_argument("--workers", type=int, help="Number of worker processes (default: CPU count)")
    parser.add_argument("--use-pandas", action="store_true", help="Use Pandas instead of PyArrow (slower but more compatible)")
    parser.add_argument("--monitor", action="store_true", help="Continuously monitor directory for new CSV files")
    parser.add_argument("--wait-delay", type=int, default=180, help="Seconds to wait before processing new files (default: 180)")
    parser.add_argument("--status-file", default="processing_status.json", help="Path to processing status JSON file (default: processing_status.json)")
    
    args = parser.parse_args()
    
    setup_logging()
    
    # Ensure the output directory exists
    output_dir = Path(args.directory)
    if not output_dir.exists():
        logging.info(f"Creating output directory: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
    
    if args.monitor:
        # Continuous monitoring mode
        logging.info(f"Starting continuous monitoring of transformer output directory: {args.directory}")
        monitor_directory_continuously(
            directory_path=args.directory,
            output_dir=args.output,
            delete_csv=not args.keep_csv,
            chunk_size=args.chunk_size,
            use_pyarrow=not args.use_pandas,
            wait_delay=args.wait_delay,
            status_file_path=args.status_file
        )
    else:
        # Batch conversion mode (original functionality)
        logging.info(f"Converting CSV files in transformer output directory: {args.directory}")
        convert_directory_csv_to_parquet(
            directory_path=args.directory,
            output_dir=args.output,
            delete_csv=not args.keep_csv,
            chunk_size=args.chunk_size,
            use_pyarrow=not args.use_pandas,
            max_workers=args.workers,
            status_file_path=args.status_file
        )

if __name__ == "__main__":
    main()