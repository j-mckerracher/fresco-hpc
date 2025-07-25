import os
import pandas as pd
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

# Define source and destination directories
source_dir = r"<LOCAL_PATH_PLACEHOLDER>\Documents\Conte\conte-transformed-by-step-1"
dest_base_dir = r"<LOCAL_PATH_PLACEHOLDER>\Documents\Conte\conte-transformed-by-step-1-daily"

# Ensure the destination base directory exists
os.makedirs(dest_base_dir, exist_ok=True)

# Dictionary to collect all data by date before saving
date_data = {}

# Get all parquet files in the source directory
parquet_files = [f for f in os.listdir(source_dir) if f.endswith('.parquet')]

# First, collect all data from all files, grouped by date
for file_name in parquet_files:
    file_path = os.path.join(source_dir, file_name)
    print(f"Reading {file_name}...")

    # Read the parquet file
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        print(f"Error reading {file_name}: {e}")
        continue

    # Ensure Timestamp column is datetime type
    if 'Timestamp' in df.columns:
        try:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        except Exception as e:
            print(f"  Error converting Timestamp column to datetime: {e}")
            continue
    else:
        print(f"  'Timestamp' column not found in {file_name}, skipping...")
        continue

    # Extract date as a string in format YYYY-MM-DD
    df['date_str'] = df['Timestamp'].dt.strftime('%Y-%m-%d')
    df['year'] = df['Timestamp'].dt.year
    df['month'] = df['Timestamp'].dt.month

    # Group by date string
    date_groups = df.groupby(['date_str', 'year', 'month'])

    # Collect data by date
    for (date_str, year, month), day_df in date_groups:
        # Create a key for the date
        date_key = (date_str, year, month)

        # Store in dictionary, concatenating if date already exists
        if date_key in date_data:
            print(f"  Found overlapping data for {date_str}, combining...")
            # Concatenate and drop duplicates to preserve all data
            date_data[date_key] = pd.concat([date_data[date_key], day_df], ignore_index=True)
            # Drop duplicates if any (based on all columns or a unique key if available)
            if 'Job Id' in day_df.columns and 'Event' in day_df.columns and 'Timestamp' in day_df.columns:
                date_data[date_key] = date_data[date_key].drop_duplicates(subset=['Job Id', 'Event', 'Timestamp'])
            else:
                date_data[date_key] = date_data[date_key].drop_duplicates()
        else:
            date_data[date_key] = day_df

    print(f"  Finished processing {file_name}")

# Now save all collected data by date
print("\nSaving all daily files...")
for (date_str, year, month), day_df in date_data.items():
    # Format year and month with leading zeros
    year_str = str(year)
    month_str = f"{month:02d}"

    # Create the monthly folder if it doesn't exist
    month_dir = os.path.join(dest_base_dir, f"{year_str}-{month_str}")
    os.makedirs(month_dir, exist_ok=True)

    # Output path for the daily file
    output_path = os.path.join(month_dir, f"FRESCO_Conte_ts_{date_str}_v1.parquet")

    # Remove the temporary columns before saving
    save_df = day_df.drop(columns=['date_str', 'year', 'month'])

    # Save as parquet
    save_df.to_parquet(output_path, index=False)

    print(f"  Saved {output_path}")

print("Processing complete!")