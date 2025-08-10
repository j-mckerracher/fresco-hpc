#!/usr/bin/env python3
"""
HPC ETL Pipeline - Main Entry Point

A modular, configuration-driven pipeline for processing HPC cluster data.
Supports multiple data sources (HTTP, local filesystem, Globus) and 
configurable transformations.

Usage:
    # Process from HTTP source (configured in YAML)
    python run_pipeline.py --config config/datasets/conte_hpc.yaml
    
    # Process a single file (Globus mode)
    python run_pipeline.py --config config/datasets/conte_hpc.yaml --file /path/to/file.csv
    
    # Process a local folder
    python run_pipeline.py --config config/datasets/conte_hpc.yaml --folder /path/to/folder
    
    # Watch for new files (file watcher mode)
    python run_pipeline.py --config config/datasets/conte_hpc.yaml --watch --source-dir /path/to/watch

Examples:
    # Basic HTTP processing
    python run_pipeline.py --config config/datasets/conte_hpc.yaml
    
    # Process single Globus file
    python run_pipeline.py --config config/datasets/conte_hpc.yaml --file /globus/received/data.csv
    
    # Watch directory for new files
    python run_pipeline.py --config config/datasets/conte_hpc.yaml --watch --source-dir /monitoring/input
"""

import sys
import logging
import argparse
from pathlib import Path
from typing import Optional

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.core.pipeline import Pipeline
from src.core.exceptions import HpcEtlException
from src.watchers.file_watcher import FileWatcher


def setup_logging(log_level: str = 'INFO', log_file: Optional[str] = None):
    """Setup logging configuration."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=handlers
    )


def validate_args(args):
    """Validate command line arguments."""
    if not Path(args.config).exists():
        raise ValueError(f"Configuration file does not exist: {args.config}")
    
    if args.file and not Path(args.file).exists():
        raise ValueError(f"Input file does not exist: {args.file}")
    
    if args.folder and not Path(args.folder).exists():
        raise ValueError(f"Input folder does not exist: {args.folder}")
    
    # Check for conflicting arguments
    mode_args = [args.file, args.folder, args.watch]
    if sum(bool(arg) for arg in mode_args) > 1:
        raise ValueError("Cannot specify multiple modes: --file, --folder, --watch")


def run_single_file_mode(pipeline: Pipeline, file_path: str) -> int:
    """Run pipeline in single file mode (typically for Globus)."""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Processing single file: {file_path}")
        success = pipeline.process_file(file_path)
        
        if success:
            logger.info("Single file processing completed successfully")
            return 0
        else:
            logger.error("Single file processing failed")
            return 1
            
    except Exception as e:
        logger.error(f"Error in single file mode: {e}")
        return 1


def run_folder_mode(pipeline: Pipeline, folder_path: str) -> int:
    """Run pipeline in folder processing mode."""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Processing folder: {folder_path}")
        stats = pipeline.process_folder(folder_path)
        
        logger.info(f"Folder processing completed. Stats: {stats}")
        
        if stats['processed'] > 0:
            return 0
        else:
            logger.error("No files were processed successfully")
            return 1
            
    except Exception as e:
        logger.error(f"Error in folder mode: {e}")
        return 1


def run_extraction_mode(pipeline: Pipeline, source: Optional[str] = None) -> int:
    """Run pipeline in full extraction mode."""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Running full pipeline extraction")
        stats = pipeline.run(source)
        
        logger.info(f"Pipeline execution completed. Stats: {stats}")
        
        if stats['processed'] > 0:
            return 0
        else:
            logger.error("No files were processed successfully")
            return 1
            
    except Exception as e:
        logger.error(f"Error in extraction mode: {e}")
        return 1


def run_watch_mode(pipeline: Pipeline, source_dir: str) -> int:
    """Run pipeline in file watching mode."""
    logger = logging.getLogger(__name__)
    
    try:
        watch_config = {
            'source_dir': source_dir,
            'wait_delay_seconds': 10,
            'max_move_attempts': 3,
            'retry_delay_seconds': 3,
            'recursive': False
        }
        
        watcher = FileWatcher(pipeline, watch_config)
        
        logger.info(f"Starting file watcher on directory: {source_dir}")
        logger.info("Press Ctrl+C to stop watching")
        
        watcher.start()  # This blocks until interrupted
        return 0
        
    except KeyboardInterrupt:
        logger.info("File watcher stopped by user")
        return 0
    except Exception as e:
        logger.error(f"Error in watch mode: {e}")
        return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='HPC ETL Pipeline - Process HPC cluster monitoring data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Required arguments
    parser.add_argument(
        '--config', 
        required=True,
        help='Path to the configuration file (YAML format)'
    )
    
    # Processing mode arguments (mutually exclusive)
    parser.add_argument(
        '--file',
        help='Process a single file (useful for Globus transfers)'
    )
    
    parser.add_argument(
        '--folder',
        help='Process all files in a folder'
    )
    
    parser.add_argument(
        '--watch',
        action='store_true',
        help='Watch directory for new files and process them automatically'
    )
    
    # Optional arguments
    parser.add_argument(
        '--source',
        help='Specific source to process (e.g., folder name for HTTP extractor)'
    )
    
    parser.add_argument(
        '--source-dir',
        default='./input',
        help='Directory to watch for new files (used with --watch)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level'
    )
    
    parser.add_argument(
        '--log-file',
        help='Log file path (optional, logs to stdout by default)'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='HPC ETL Pipeline 1.0.0'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level, args.log_file)
    logger = logging.getLogger(__name__)
    
    try:
        # Validate arguments
        validate_args(args)
        
        # Initialize pipeline
        logger.info(f"Initializing pipeline with config: {args.config}")
        pipeline = Pipeline(args.config)
        
        # Determine and run processing mode
        if args.file:
            return run_single_file_mode(pipeline, args.file)
        elif args.folder:
            return run_folder_mode(pipeline, args.folder)
        elif args.watch:
            return run_watch_mode(pipeline, args.source_dir)
        else:
            # Default mode: full extraction
            return run_extraction_mode(pipeline, args.source)
            
    except Exception as e:
        if logger:
            logger.error(f"Pipeline execution failed: {e}")
        else:
            print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())