#!/usr/bin/env python3
"""
Script to move combined daily files to final directory.
Moves only files matching YYYY-MM-DD.parquet pattern (combined files).
"""
import os
import shutil
import argparse
from pathlib import Path
import re



def is_combined_file(filename):
    """Check if filename matches YYYY-MM-DD.parquet pattern"""
    pattern = r"^\d{4}-\d{2}-\d{2}\.parquet$"
    return bool(re.match(pattern, filename))


def move_combined_files(source_dir, target_dir, dry_run=False):
    """Move combined daily files to target directory"""
    source_path = Path(source_dir)
    target_path = Path(target_dir)
    
    if not source_path.exists():
        print(f"Error: Source directory {source_path} does not exist")
        return
    
    if not source_path.is_dir():
        print(f"Error: {source_path} is not a directory")
        return
    
    # Find combined files
    combined_files = []
    for file_path in source_path.iterdir():
        if file_path.is_file() and is_combined_file(file_path.name):
            combined_files.append(file_path)
    
    if not combined_files:
        print("No combined files found matching YYYY-MM-DD.parquet pattern")
        return
    
    combined_files.sort()  # Sort by filename (date)
    print(f"Found {len(combined_files)} combined files to move")
    
    if dry_run:
        print("\n--- DRY RUN MODE ---")
        print(f"Would create directory: {target_path}")
        print("Would move the following files:")
        for file_path in combined_files:
            target_file = target_path / file_path.name
            print(f"  {file_path} -> {target_file}")
        return
    
    # Create target directory
    print(f"Creating target directory: {target_path}")
    target_path.mkdir(parents=True, exist_ok=True)
    
    # Move files
    moved_count = 0
    failed_count = 0
    
    print("\nMoving files...")
    for file_path in combined_files:
        target_file = target_path / file_path.name
        
        try:
            if target_file.exists():
                print(f"Warning: Target file {target_file} already exists, skipping {file_path.name}")
                failed_count += 1
                continue
            
            shutil.move(str(file_path), str(target_file))
            print(f"Moved: {file_path.name}")
            moved_count += 1
            
        except Exception as e:
            print(f"Error moving {file_path.name}: {e}")
            failed_count += 1
    
    print(f"\nSummary:")
    print(f"  Files successfully moved: {moved_count}")
    print(f"  Files failed/skipped: {failed_count}")
    print(f"  Target directory: {target_path}")

def main():
    parser = argparse.ArgumentParser(
        description="Move combined daily files (YYYY-MM-DD.parquet) to final directory"
    )
    parser.add_argument(
        "source_dir",
        help="Source directory containing combined files"
    )
    parser.add_argument(
        "target_dir", 
        help="Target directory to move files to"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be moved without actually moving files"
    )
    
    args = parser.parse_args()
    
    move_combined_files(args.source_dir, args.target_dir, dry_run=args.dry_run)

if __name__ == "__main__":
    main()