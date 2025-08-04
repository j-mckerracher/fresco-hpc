
import argparse

from hpc_etl_pipeline.src.core.pipeline import Pipeline
from hpc_etl_pipeline.src.watchers.file_watcher import FileWatcher

def main():
    """Main entry point for the HPC ETL pipeline."""
    parser = argparse.ArgumentParser(description="HPC ETL Pipeline")
    parser.add_argument("--config", required=True, help="Path to the dataset configuration file.")
    parser.add_argument("--file", help="Path to a single file to process.")
    parser.add_argument("--folder", help="Path to a folder to process.")
    parser.add_argument("--watch", action="store_true", help="Watch a directory for new files.")
    args = parser.parse_args()

    pipeline = Pipeline(args.config)

    if args.file:
        # The process_file method will be implemented later
        # pipeline.process_file(args.file)
        pass
    elif args.folder:
        # The process_folder method will be implemented later
        # pipeline.process_folder(args.folder)
        pass
    elif args.watch:
        watch_dir = pipeline.config.get("source", {}).get("watch_directory", ".")
        watcher = FileWatcher(pipeline, watch_dir)
        watcher.start()
    else:
        pipeline.run()

if __name__ == "__main__":
    main()
