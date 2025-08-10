"""HPC-specific data transformers."""

from typing import Dict, Any, Optional
import polars as pl
import logging

from .base_transformer import BaseTransformer
from ..core.exceptions import TransformationError

logger = logging.getLogger(__name__)


class BlockIOTransformer(BaseTransformer):
    """Transforms block I/O data to calculate disk throughput rates."""
    
    SECTOR_SIZE_BYTES = 512
    BYTES_TO_GB = 1 / (1024 ** 3)
    MIN_TIME_DELTA = 0.1
    
    def transform(self, data: pl.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """Transform block I/O data to throughput rates."""
        if not self.validate_input(data):
            return pl.DataFrame()
        
        try:
            # Required columns for block processing
            required_columns = ['rd_sectors', 'wr_sectors', 'jobID', 'node', 'device', 'timestamp']
            missing_cols = [col for col in required_columns if col not in data.columns]
            if missing_cols:
                raise TransformationError(f"Missing required columns: {missing_cols}")
            
            # Convert numeric columns and handle errors
            df = data.with_columns([
                pl.col('rd_sectors').cast(pl.Float64, strict=False),
                pl.col('wr_sectors').cast(pl.Float64, strict=False),
                pl.col('jobID').fill_null('unknown').cast(pl.Utf8),
                pl.col('node').fill_null('unknown').cast(pl.Utf8),
                pl.col('device').fill_null('unknown').cast(pl.Utf8)
            ])
            
            # Drop rows with invalid numeric data
            df = df.filter(
                pl.col('rd_sectors').is_not_null() & 
                pl.col('wr_sectors').is_not_null()
            )
            
            if df.is_empty():
                logger.info("No valid data rows after filtering invalid values")
                return pl.DataFrame()
            
            # Clean job ID and parse timestamp
            df = df.with_columns([
                pl.col('jobID').str.replace_all('jobID', 'JOB', literal=False),
                pl.col('timestamp').str.strptime(pl.Datetime, format='%m/%d/%Y %H:%M:%S', strict=False).alias('Timestamp_original')
            ])
            
            # Filter out rows with invalid timestamps
            df = df.filter(pl.col('Timestamp_original').is_not_null())
            if df.is_empty():
                logger.info("No valid data rows after timestamp filtering")
                return pl.DataFrame()
            
            # Sort by job, node, device, timestamp
            df = df.sort(['jobID', 'node', 'device', 'Timestamp_original'])
            
            # Calculate I/O rates per device
            df = df.with_columns([
                (pl.col('rd_sectors') + pl.col('wr_sectors')).alias('total_sectors')
            ])
            
            # Calculate time and sector deltas within each device group
            df = df.with_columns([
                pl.col('Timestamp_original').diff().over(['jobID', 'node', 'device']).dt.total_seconds().alias('time_delta_seconds'),
                pl.col('total_sectors').diff().over(['jobID', 'node', 'device']).alias('sector_delta')
            ])
            
            # Calculate device-level I/O rates
            df = df.with_columns([
                pl.when(
                    (pl.col('time_delta_seconds').is_not_null()) &
                    (pl.col('time_delta_seconds') >= self.MIN_TIME_DELTA) &
                    (pl.col('sector_delta').is_not_null()) &
                    (pl.col('sector_delta') >= 0)
                ).then(
                    (pl.col('sector_delta') * self.SECTOR_SIZE_BYTES * self.BYTES_TO_GB) / pl.col('time_delta_seconds')
                ).otherwise(0.0).alias('Value_device_rate')
            ])
            
            # Aggregate device rates to node level
            node_aggregated = df.group_by(['jobID', 'node', 'Timestamp_original']).agg([
                pl.col('Value_device_rate').sum().alias('Value')
            ])
            
            # Create final output with standardized schema
            result = node_aggregated.with_columns([
                pl.col('jobID').alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.lit('block').alias('Event'),
                pl.lit('GB/s').alias('Units'),
                pl.col('Timestamp_original').alias('Timestamp')
            ]).select(['Job Id', 'Host', 'Event', 'Value', 'Units', 'Timestamp'])
            
            logger.info(f"Processed block I/O data: {len(result)} output rows")
            return result
            
        except Exception as e:
            raise TransformationError(f"Failed to transform block I/O data: {e}")


class CPUTransformer(BaseTransformer):
    """Transforms CPU data to calculate user CPU percentage."""
    
    CPU_JIFFY_COLUMNS = ['user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq']
    
    def transform(self, data: pl.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """Transform CPU data to user percentage."""
        if not self.validate_input(data):
            return pl.DataFrame()
        
        try:
            # Required columns for CPU processing
            required_columns = self.CPU_JIFFY_COLUMNS + ['jobID', 'node', 'device', 'timestamp']
            missing_cols = [col for col in required_columns if col not in data.columns]
            if missing_cols:
                raise TransformationError(f"Missing required columns: {missing_cols}")
            
            # Convert all jiffy columns to numeric
            jiffy_casts = [pl.col(col).cast(pl.Float64, strict=False) for col in self.CPU_JIFFY_COLUMNS]
            
            df = data.with_columns([
                *jiffy_casts,
                pl.col('jobID').fill_null('unknown').cast(pl.Utf8),
                pl.col('node').fill_null('unknown').cast(pl.Utf8),
                pl.col('device').fill_null('unknown').cast(pl.Utf8)
            ])
            
            # Drop rows with any invalid jiffy values
            jiffy_filter = pl.fold(
                acc=pl.lit(True),
                function=lambda acc, x: acc & x.is_not_null(),
                exprs=[pl.col(col) for col in self.CPU_JIFFY_COLUMNS]
            )
            df = df.filter(jiffy_filter)
            
            if df.is_empty():
                logger.info("No valid data rows after filtering invalid CPU values")
                return pl.DataFrame()
            
            # Clean job ID and parse timestamp
            df = df.with_columns([
                pl.col('jobID').str.replace_all('jobID', 'JOB', literal=False),
                pl.col('timestamp').str.strptime(pl.Datetime, format='%m/%d/%Y %H:%M:%S', strict=False).alias('Timestamp_original')
            ])
            
            # Filter out rows with invalid timestamps
            df = df.filter(pl.col('Timestamp_original').is_not_null())
            if df.is_empty():
                logger.info("No valid data rows after timestamp filtering")
                return pl.DataFrame()
            
            # Sort by job, node, device (CPU core), timestamp
            df = df.sort(['jobID', 'node', 'device', 'Timestamp_original'])
            
            # Calculate total jiffies
            df = df.with_columns([
                pl.sum_horizontal([pl.col(col) for col in self.CPU_JIFFY_COLUMNS]).alias('total_jiffies')
            ])
            
            # Calculate deltas for each jiffy column per CPU core
            delta_exprs = []
            for col in self.CPU_JIFFY_COLUMNS:
                delta_exprs.append(
                    pl.col(col).diff().over(['jobID', 'node', 'device']).alias(f'{col}_delta')
                )
            delta_exprs.append(
                pl.col('total_jiffies').diff().over(['jobID', 'node', 'device']).alias('total_jiffies_delta')
            )
            
            df = df.with_columns(delta_exprs)
            
            # Filter valid deltas (positive values, not first measurement)
            valid_filter = (
                pl.col('total_jiffies_delta').is_not_null() &
                (pl.col('total_jiffies_delta') > 0) &
                pl.col('user_delta').is_not_null() &
                pl.col('nice_delta').is_not_null() &
                (pl.col('user_delta') >= 0) &
                (pl.col('nice_delta') >= 0)
            )
            df = df.filter(valid_filter)
            
            if df.is_empty():
                logger.info("No valid CPU delta data after filtering")
                return pl.DataFrame()
            
            # Aggregate deltas from CPU cores to node level
            node_aggregated = df.group_by(['jobID', 'node', 'Timestamp_original']).agg([
                pl.col('user_delta').sum().alias('user_delta_sum'),
                pl.col('nice_delta').sum().alias('nice_delta_sum'),
                pl.col('total_jiffies_delta').sum().alias('total_jiffies_delta_sum')
            ])
            
            # Calculate CPU user percentage at node level
            node_aggregated = node_aggregated.with_columns([
                pl.when(pl.col('total_jiffies_delta_sum') > 0)
                .then(((pl.col('user_delta_sum') + pl.col('nice_delta_sum')) / pl.col('total_jiffies_delta_sum')) * 100.0)
                .otherwise(0.0)
                .clip(0.0, 100.0)
                .alias('Value')
            ])
            
            # Create final output with standardized schema
            result = node_aggregated.with_columns([
                pl.col('jobID').alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.lit('cpuuser').alias('Event'),
                pl.lit('CPU %').alias('Units'),
                pl.col('Timestamp_original').alias('Timestamp')
            ]).select(['Job Id', 'Host', 'Event', 'Value', 'Units', 'Timestamp'])
            
            logger.info(f"Processed CPU data: {len(result)} output rows")
            return result
            
        except Exception as e:
            raise TransformationError(f"Failed to transform CPU data: {e}")


class MemoryTransformer(BaseTransformer):
    """Transforms memory data to calculate memory usage metrics."""
    
    BYTES_TO_GB = 1 / (1024 ** 3)
    
    def transform(self, data: pl.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """Transform memory data to usage metrics."""
        if not self.validate_input(data):
            return pl.DataFrame()
        
        try:
            # Required columns for memory processing
            required_columns = ['MemTotal', 'MemFree', 'FilePages', 'jobID', 'node', 'timestamp']
            missing_cols = [col for col in required_columns if col not in data.columns]
            if missing_cols:
                raise TransformationError(f"Missing required columns: {missing_cols}")
            
            # Convert memory columns to numeric
            df = data.with_columns([
                pl.col('MemTotal').cast(pl.Float64, strict=False),
                pl.col('MemFree').cast(pl.Float64, strict=False),
                pl.col('FilePages').cast(pl.Float64, strict=False),
                pl.col('jobID').fill_null('unknown').cast(pl.Utf8),
                pl.col('node').fill_null('unknown').cast(pl.Utf8)
            ])
            
            # Drop rows with invalid memory values
            df = df.filter(
                pl.col('MemTotal').is_not_null() & 
                pl.col('MemFree').is_not_null() &
                pl.col('FilePages').is_not_null()
            )
            
            if df.is_empty():
                logger.info("No valid data rows after filtering invalid memory values")
                return pl.DataFrame()
            
            # Clean job ID and parse timestamp
            df = df.with_columns([
                pl.col('jobID').str.replace_all('jobID', 'JOB', literal=False),
                pl.col('timestamp').str.strptime(pl.Datetime, format='%m/%d/%Y %H:%M:%S', strict=False).alias('Timestamp_original')
            ])
            
            # Filter out rows with invalid timestamps
            df = df.filter(pl.col('Timestamp_original').is_not_null())
            if df.is_empty():
                logger.info("No valid data rows after timestamp filtering")
                return pl.DataFrame()
            
            # Validate and bound memory values
            df = df.with_columns([
                # Ensure all values are non-negative
                pl.col('MemTotal').clip(0.0, None),
                pl.col('MemFree').clip(0.0, None),
                pl.col('FilePages').clip(0.0, None)
            ])
            
            # Ensure MemFree <= MemTotal
            df = df.with_columns([
                pl.min_horizontal([pl.col('MemFree'), pl.col('MemTotal')]).alias('MemFree')
            ])
            
            # Calculate memory used
            df = df.with_columns([
                (pl.col('MemTotal') - pl.col('MemFree')).alias('memory_used')
            ])
            
            # Ensure FilePages <= MemTotal and <= memory_used
            df = df.with_columns([
                pl.min_horizontal([
                    pl.col('FilePages'), 
                    pl.col('MemTotal'), 
                    pl.col('memory_used')
                ]).alias('FilePages')
            ])
            
            # Calculate both memory metrics
            df = df.with_columns([
                (pl.col('memory_used') * self.BYTES_TO_GB).alias('memused_value'),
                ((pl.col('memory_used') - pl.col('FilePages')) * self.BYTES_TO_GB).alias('memused_minus_diskcache_value')
            ])
            
            # Create two output rows per input row (one for each metric)
            memused_rows = df.select([
                pl.col('jobID').alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.lit('memused').alias('Event'),
                pl.col('memused_value').alias('Value'),
                pl.lit('GB').alias('Units'),
                pl.col('Timestamp_original').alias('Timestamp')
            ])
            
            memused_minus_cache_rows = df.select([
                pl.col('jobID').alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.lit('memused_minus_diskcache').alias('Event'),
                pl.col('memused_minus_diskcache_value').alias('Value'),
                pl.lit('GB').alias('Units'),
                pl.col('Timestamp_original').alias('Timestamp')
            ])
            
            # Combine both result sets
            result = pl.concat([memused_rows, memused_minus_cache_rows])
            
            logger.info(f"Processed memory data: {len(result)} output rows")
            return result
            
        except Exception as e:
            raise TransformationError(f"Failed to transform memory data: {e}")


class NFSTransformer(BaseTransformer):
    """Transforms NFS data to calculate transfer rates."""
    
    BYTES_TO_MB = 1 / (1024 * 1024)
    MIN_TIME_DELTA = 0.1
    
    def transform(self, data: pl.DataFrame, metadata: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        """Transform NFS data to transfer rates."""
        if not self.validate_input(data):
            return pl.DataFrame()
        
        try:
            # Required columns for NFS processing
            required_columns = ['read_bytes', 'write_bytes', 'jobID', 'node', 'timestamp']
            missing_cols = [col for col in required_columns if col not in data.columns]
            if missing_cols:
                raise TransformationError(f"Missing required columns: {missing_cols}")
            
            # Convert numeric columns
            df = data.with_columns([
                pl.col('read_bytes').cast(pl.Float64, strict=False),
                pl.col('write_bytes').cast(pl.Float64, strict=False),
                pl.col('jobID').fill_null('unknown').cast(pl.Utf8),
                pl.col('node').fill_null('unknown').cast(pl.Utf8)
            ])
            
            # Drop rows with invalid values
            df = df.filter(
                pl.col('read_bytes').is_not_null() & 
                pl.col('write_bytes').is_not_null()
            )
            
            if df.is_empty():
                logger.info("No valid data rows after filtering invalid NFS values")
                return pl.DataFrame()
            
            # Clean job ID and parse timestamp
            df = df.with_columns([
                pl.col('jobID').str.replace_all('jobID', 'JOB', literal=False),
                pl.col('timestamp').str.strptime(pl.Datetime, format='%m/%d/%Y %H:%M:%S', strict=False).alias('Timestamp_original')
            ])
            
            # Filter out rows with invalid timestamps
            df = df.filter(pl.col('Timestamp_original').is_not_null())
            if df.is_empty():
                logger.info("No valid data rows after timestamp filtering")
                return pl.DataFrame()
            
            # Sort by job, node, timestamp
            df = df.sort(['jobID', 'node', 'Timestamp_original'])
            
            # Calculate total bytes and deltas
            df = df.with_columns([
                (pl.col('read_bytes') + pl.col('write_bytes')).alias('total_bytes')
            ])
            
            # Calculate time and byte deltas within each node group
            df = df.with_columns([
                pl.col('Timestamp_original').diff().over(['jobID', 'node']).dt.total_seconds().alias('time_delta_seconds'),
                pl.col('total_bytes').diff().over(['jobID', 'node']).alias('byte_delta')
            ])
            
            # Calculate NFS transfer rates
            df = df.with_columns([
                pl.when(
                    (pl.col('time_delta_seconds').is_not_null()) &
                    (pl.col('time_delta_seconds') >= self.MIN_TIME_DELTA) &
                    (pl.col('byte_delta').is_not_null()) &
                    (pl.col('byte_delta') >= 0)
                ).then(
                    (pl.col('byte_delta') * self.BYTES_TO_MB) / pl.col('time_delta_seconds')
                ).otherwise(0.0).alias('Value')
            ])
            
            # Filter out first measurements (no deltas available)
            df = df.filter(pl.col('time_delta_seconds').is_not_null())
            
            # Create final output with standardized schema
            result = df.with_columns([
                pl.col('jobID').alias('Job Id'),
                pl.col('node').alias('Host'),
                pl.lit('nfs').alias('Event'),
                pl.lit('MB/s').alias('Units'),
                pl.col('Timestamp_original').alias('Timestamp')
            ]).select(['Job Id', 'Host', 'Event', 'Value', 'Units', 'Timestamp'])
            
            logger.info(f"Processed NFS data: {len(result)} output rows")
            return result
            
        except Exception as e:
            raise TransformationError(f"Failed to transform NFS data: {e}")