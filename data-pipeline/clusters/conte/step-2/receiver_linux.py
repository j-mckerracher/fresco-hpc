#!/usr/bin/env python3
"""
Linux version of receiver.py for testing/troubleshooting on the server
This script shows what files are ready for transfer
"""

import os
import time
import logging
from pathlib import Path
import re
from typing import List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Path configuration for Linux server
SERVER_SOURCE_DIR = Path("<LOCAL_PATH_PLACEHOLDER>/projects/conte-to-fresco-etl/step-2/cache/output_consumer")
SIGNAL_DIR = Path("<LOCAL_PATH_PLACEHOLDER>/projects/conte-to-fresco-etl/step-2/cache/composer_ready")

def find_completed_files() -> List[Tuple[str, Path]]:
    """Find files with .complete signals in the signal directory that haven't been transferred yet"""
    completed_files = []
    stale_transfers = []

    # Log all signal files for debugging
    all_signals = list(SIGNAL_DIR.glob("*.complete"))
    logger.info(f"Found {len(all_signals)} .complete signal files")

    # Show first few examples
    if all_signals:
        examples = [f.name for f in all_signals[:10]]
        logger.info(f"Examples: {', '.join(examples)}{'...' if len(all_signals) > 10 else ''}")

    # Check for both daily signals (YYYY-MM-DD.complete) and monthly signals (YYYY-MM.complete)
    daily_pattern = r'(\d{4}-\d{2}-\d{2})\.complete'
    monthly_pattern = r'(\d{4}-\d{2})\.complete'

    # Process all complete signals
    for signal_file in all_signals:
        # Try daily pattern first
        daily_match = re.match(daily_pattern, signal_file.name)
        if daily_match:
            date_str = daily_match.group(1)

            # Check if this date already has a .transferred signal
            transferred_signal = SIGNAL_DIR / f"{date_str}.transferred"
            source_file = SERVER_SOURCE_DIR / f"perf_metrics_{date_str}.parquet"
            
            if transferred_signal.exists():
                # Check if the .complete signal is newer than .transferred signal
                complete_mtime = signal_file.stat().st_mtime
                transferred_mtime = transferred_signal.stat().st_mtime
                
                if complete_mtime > transferred_mtime and source_file.exists():
                    stale_transfers.append((date_str, source_file, signal_file, transferred_signal))
                    logger.warning(f"🔄 Stale transfer: {date_str} - complete signal is newer than transferred signal")
                else:
                    logger.debug(f"Signal {signal_file.name} already has a corresponding .transferred signal. Skipping.")
                continue

            if source_file.exists():
                completed_files.append((date_str, source_file))
                logger.info(f"✓ Ready for transfer: {date_str} -> {source_file.name}")
            else:
                logger.warning(f"⚠ Daily signal {signal_file.name} exists but source file {source_file} not found")
            continue

        # Try monthly pattern if daily doesn't match
        monthly_match = re.match(monthly_pattern, signal_file.name)
        if monthly_match:
            month_str = monthly_match.group(1)

            # Check if this month already has a .transferred signal
            transferred_signal = SIGNAL_DIR / f"{month_str}.transferred"
            if transferred_signal.exists():
                logger.debug(f"Signal {signal_file.name} already has a corresponding .transferred signal. Skipping.")
                continue

            # Look for all files in that month (could be multiple days)
            pattern = f"perf_metrics_{month_str}-*.parquet"
            month_files = list(SERVER_SOURCE_DIR.glob(pattern))

            logger.info(f"Found monthly signal {signal_file.name}, looking for files matching: {pattern}")
            logger.info(f"Found {len(month_files)} matching daily files")

            for source_file in month_files:
                # Check if this specific file already has a transferred signal
                file_match = re.search(r'perf_metrics_(\d{4}-\d{2}-\d{2})\.parquet', source_file.name)
                if file_match:
                    date_str = file_match.group(1)

                    # Skip if this specific day already has a transferred signal
                    day_transferred_signal = SIGNAL_DIR / f"{date_str}.transferred"
                    if day_transferred_signal.exists():
                        logger.debug(f"Daily file {source_file.name} already has a .transferred signal. Skipping.")
                        continue

                    completed_files.append((date_str, source_file))
                    logger.info(f"✓ Ready for transfer: {date_str} -> {source_file.name}")

    logger.info(f"📊 Summary: {len(completed_files)} files ready for transfer")
    if stale_transfers:
        logger.warning(f"⚠️ Found {len(stale_transfers)} stale transfers (complete signal newer than transferred)")
        for date_str, source_file, complete_signal, transferred_signal in stale_transfers[:10]:
            complete_time = time.strftime("%H:%M:%S", time.localtime(complete_signal.stat().st_mtime))
            transferred_time = time.strftime("%H:%M:%S", time.localtime(transferred_signal.stat().st_mtime))
            logger.warning(f"  {date_str}: complete={complete_time}, transferred={transferred_time}")
    
    return completed_files, stale_transfers

def check_network_paths():
    """Check if the Windows receiver script paths would be accessible"""
    logger.info("\n🔍 Checking Windows receiver script network path accessibility...")
    
    # These are the paths the Windows receiver script expects
    windows_paths = [
        "U:\\projects\\conte-to-fresco-etl\\step-2\\cache\\output_consumer",
        "U:\\projects\\conte-to-fresco-etl\\step-2\\cache\\composer_ready"
    ]
    
    for path in windows_paths:
        logger.info(f"Windows receiver expects path: {path}")
    
    logger.info("\nNote: These paths should be accessible from your Windows machine via network drive mapping.")

def main():
    """Main function to check transfer status"""
    logger.info("🚀 Linux Receiver Diagnostic Tool")
    logger.info(f"📂 Server source directory: {SERVER_SOURCE_DIR}")
    logger.info(f"📋 Signal directory: {SIGNAL_DIR}")
    
    # Check if directories exist
    if not SERVER_SOURCE_DIR.exists():
        logger.error(f"❌ Server source directory not found: {SERVER_SOURCE_DIR}")
        return
    
    if not SIGNAL_DIR.exists():
        logger.error(f"❌ Signal directory not found: {SIGNAL_DIR}")
        return
    
    logger.info("✅ Directories found")
    
    # Find completed files
    completed_files, stale_transfers = find_completed_files()
    
    if completed_files:
        logger.info(f"\n📋 Files ready for transfer ({len(completed_files)}):")
        for i, (date_str, source_file) in enumerate(completed_files[:20], 1):
            size_mb = source_file.stat().st_size / (1024 * 1024)
            logger.info(f"  {i:2d}. {date_str} - {source_file.name} ({size_mb:.1f} MB)")
        
        if len(completed_files) > 20:
            logger.info(f"  ... and {len(completed_files) - 20} more files")
    else:
        logger.info("ℹ️  No files currently ready for transfer")
    
    # Check network path info
    check_network_paths()
    
    # Show recent signal activity
    logger.info("\n📈 Recent signal file activity:")
    recent_signals = sorted(SIGNAL_DIR.glob("*.complete"), key=lambda x: x.stat().st_mtime, reverse=True)[:10]
    for signal_file in recent_signals:
        mtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(signal_file.stat().st_mtime))
        logger.info(f"  {signal_file.name} - {mtime}")

if __name__ == "__main__":
    main()