import polars as pl
import os
from pathlib import Path
from datetime import datetime


def process_year_month_folder(base_path, year_month_folder):
    """
    Process all parquet files in a year-month folder and split into hourly chunks.

    Args:
        base_path: Base directory path
        year_month_folder: Year-month folder name (e.g., "2017-08")
    """
    folder_path = Path(base_path) / year_month_folder

    if not folder_path.exists():
        print(f"Folder {folder_path} does not exist, skipping...")
        return

    # Find all parquet files in the folder
    parquet_files = list(folder_path.glob("*.parquet"))

    if not parquet_files:
        print(f"No parquet files found in {folder_path}, skipping...")
        return

    print(f"Processing {year_month_folder} with {len(parquet_files)} files...")

    # Read and combine all parquet files for this year-month
    dataframes = []
    for file_path in parquet_files:
        try:
            df = pl.read_parquet(file_path)
            dataframes.append(df)
            print(f"  Loaded {file_path.name}: {df.shape[0]} rows")
        except Exception as e:
            print(f"  Error reading {file_path}: {e}")

    if not dataframes:
        print(f"No valid parquet files to process in {year_month_folder}")
        return

    # Combine all dataframes
    combined_df = pl.concat(dataframes, how="vertical")
    print(f"  Combined dataset: {combined_df.shape[0]} rows, {combined_df.shape[1]} columns")

    # Ensure the time column is datetime type
    if "time" not in combined_df.columns:
        print(f"  Warning: 'time' column not found in {year_month_folder}")
        return

    # Convert time column to datetime if it's not already
    combined_df = combined_df.with_columns(
        pl.col("time").str.to_datetime() if combined_df["time"].dtype == pl.Utf8
        else pl.col("time")
    )

    # Extract year, month, day, and hour from time column
    combined_df = combined_df.with_columns([
        pl.col("time").dt.year().alias("year"),
        pl.col("time").dt.month().alias("month"),
        pl.col("time").dt.day().alias("day"),
        pl.col("time").dt.hour().alias("hour")
    ])

    # Get unique combinations of year, month, day, hour
    unique_combinations = combined_df.select(["year", "month", "day", "hour"]).unique().sort(
        ["year", "month", "day", "hour"])

    hours_processed = 0
    total_rows_written = 0

    for row in unique_combinations.iter_rows(named=True):
        year = row["year"]
        month = row["month"]
        day = row["day"]
        hour = row["hour"]

        # Filter data for this specific year/month/day/hour combination
        group_df = combined_df.filter(
            (pl.col("year") == year) &
            (pl.col("month") == month) &
            (pl.col("day") == day) &
            (pl.col("hour") == hour)
        ).drop(["year", "month", "day", "hour"])

        if group_df.shape[0] > 0:
            # Create nested output directory structure: output/year/month/day/
            output_dir = Path("<LOCAL_PATH_PLACEHOLDER>/projects/stampede-step-4/output") / str(
                year) / f"{month:02d}" / f"{day:02d}"
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create filename with _S suffix and zero-padded hour
            filename = f"{hour:02d}_S.parquet"
            output_path = output_dir / filename

            # Save the hourly chunk
            group_df.write_parquet(output_path)

            hours_processed += 1
            total_rows_written += group_df.shape[0]
            print(f"    Saved {year}/{month:02d}/{day:02d}/{filename}: {group_df.shape[0]} rows")

    print(f"  Completed {year_month_folder}: {hours_processed} hourly files, {total_rows_written} total rows")

    # Verify no data was lost
    if total_rows_written != combined_df.shape[0]:
        print(f"  WARNING: Data loss detected! Input: {combined_df.shape[0]}, Output: {total_rows_written}")
    else:
        print(f"  ✓ All data preserved: {total_rows_written} rows")


def main():
    """
    Main function to process all year-month folders.
    """
    base_path = "<LOCAL_PATH_PLACEHOLDER>/projects/stampede-step-4/input"

    if not os.path.exists(base_path):
        print(f"Base path {base_path} does not exist!")
        return

    # Get all subdirectories (year-month folders)
    year_month_folders = [d for d in os.listdir(base_path)
                          if os.path.isdir(os.path.join(base_path, d)) and d not in ["chunks", "output"]]

    if not year_month_folders:
        print("No year-month folders found!")
        return

    print(f"Found {len(year_month_folders)} year-month folders to process:")
    for folder in sorted(year_month_folders):
        print(f"  {folder}")

    print("\nStarting processing...")

    # Process each year-month folder
    for year_month_folder in sorted(year_month_folders):
        try:
            process_year_month_folder(base_path, year_month_folder)
        except Exception as e:
            print(f"Error processing {year_month_folder}: {e}")

    print("\nProcessing complete!")


if __name__ == "__main__":
    main()