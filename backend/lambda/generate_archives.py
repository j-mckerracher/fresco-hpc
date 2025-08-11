#!/usr/bin/env python3

import os
import json
import tempfile
import zipfile
import hashlib
import subprocess
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class S3Object:
    def __init__(self, key: str, last_modified: datetime, size: int):
        self.key = key
        self.last_modified = last_modified
        self.size = size

class DiskMonitor:
    def __init__(self, max_usage_gb: float = 28.0):
        self.max_usage_gb = max_usage_gb
        
    def get_home_usage_gb(self) -> float:
        """Get current home directory usage in GB using du -sh ~"""
        try:
            result = subprocess.run(['du', '-sb', os.path.expanduser('~')], 
                                  capture_output=True, text=True, check=True)
            bytes_used = int(result.stdout.split()[0])
            gb_used = bytes_used / (1024**3)
            return gb_used
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to check disk usage: {e}")
            raise
    
    def check_usage_limit(self) -> bool:
        """Check if current usage exceeds the limit"""
        current_usage = self.get_home_usage_gb()
        logger.info(f"Current home directory usage: {current_usage:.2f}GB (limit: {self.max_usage_gb}GB)")
        
        if current_usage > self.max_usage_gb:
            logger.error(f"Disk usage {current_usage:.2f}GB exceeds limit {self.max_usage_gb}GB")
            return False
        return True

class ArchiveGenerator:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.disk_monitor = DiskMonitor()
        
        # Environment variables
        self.src_bucket = os.environ.get('SOURCE_BUCKET', 'fresco-data')
        self.src_prefix = os.environ.get('SOURCE_PREFIX', '')
        self.archive_bucket = os.environ.get('ARCHIVE_BUCKET', 'fresco-archives')
        
    def list_objects(self, bucket: str, prefix: str) -> List[S3Object]:
        """List objects in S3 bucket using boto3"""
        objects = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects.append(S3Object(
                        key=obj['Key'],
                        last_modified=obj['LastModified'],
                        size=obj['Size']
                    ))
        
        logger.info(f"Found {len(objects)} objects in s3://{bucket}/{prefix}")
        return objects
    
    def group_by_month(self, objects: List[S3Object]) -> Dict[str, List[S3Object]]:
        """Group objects by month (YYYY-MM)"""
        groups = {}
        for obj in objects:
            month_key = obj.last_modified.strftime('%Y-%m')
            if month_key not in groups:
                groups[month_key] = []
            groups[month_key].append(obj)
        return groups
    
    def group_by_quarter(self, objects: List[S3Object]) -> Dict[str, List[S3Object]]:
        """Group objects by quarter (YYYY-QN)"""
        groups = {}
        for obj in objects:
            quarter = (obj.last_modified.month - 1) // 3 + 1
            quarter_key = f"{obj.last_modified.year}-Q{quarter}"
            if quarter_key not in groups:
                groups[quarter_key] = []
            groups[quarter_key].append(obj)
        return groups
    
    def estimate_download_size(self, objects: List[S3Object]) -> float:
        """Estimate total download size in GB"""
        total_bytes = sum(obj.size for obj in objects)
        return total_bytes / (1024**3)
    
    def create_archive(self, archive_type: str, name: str, objects: List[S3Object], 
                      src_bucket: str, archive_bucket: str) -> Dict:
        """Create archive from S3 objects with disk usage monitoring"""
        
        # Pre-flight checks
        estimated_size = self.estimate_download_size(objects)
        logger.info(f"Creating {archive_type} archive '{name}' with {len(objects)} objects (~{estimated_size:.2f}GB)")
        
        if not self.disk_monitor.check_usage_limit():
            raise RuntimeError("Disk usage limit exceeded before archive creation")
        
        # Create temporary directory
        with tempfile.TemporaryDirectory(prefix='archive-') as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Download objects from S3
            for obj in objects:
                local_filename = Path(obj.key).name
                local_path = tmp_path / local_filename
                
                try:
                    logger.debug(f"Downloading s3://{src_bucket}/{obj.key}")
                    self.s3_client.download_file(src_bucket, obj.key, str(local_path))
                except ClientError as e:
                    logger.error(f"Failed to download {obj.key}: {e}")
                    raise
                
                # Check disk usage after each download
                if not self.disk_monitor.check_usage_limit():
                    raise RuntimeError(f"Disk usage limit exceeded during download of {obj.key}")
            
            # Create ZIP archive
            archive_name = f"{name}.zip"
            archive_path = tmp_path / archive_name
            
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in tmp_path.glob('*'):
                    if file_path != archive_path:
                        zipf.write(file_path, file_path.name)
            
            # Calculate checksum
            sha256_hash = hashlib.sha256()
            with open(archive_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            checksum = sha256_hash.hexdigest()
            
            # Get archive size
            archive_size = archive_path.stat().st_size
            
            # Upload archive to S3
            s3_key = f"archives/{archive_type}/{archive_name}"
            try:
                logger.info(f"Uploading archive to s3://{archive_bucket}/{s3_key}")
                self.s3_client.upload_file(str(archive_path), archive_bucket, s3_key)
            except ClientError as e:
                logger.error(f"Failed to upload archive: {e}")
                raise
            
            # Calculate date range
            dates = [obj.last_modified for obj in objects]
            start_date = min(dates).isoformat()
            end_date = max(dates).isoformat()
            
            return {
                'path': s3_key,
                'size': archive_size,
                'checksum': checksum,
                'start': start_date,
                'end': end_date,
                'object_count': len(objects)
            }
    
    def run(self):
        """Main execution function"""
        logger.info("Starting archive generation process")
        
        # Initial disk usage check
        if not self.disk_monitor.check_usage_limit():
            raise RuntimeError("Initial disk usage check failed")
        
        try:
            # List all objects
            objects = self.list_objects(self.src_bucket, self.src_prefix)
            if not objects:
                logger.warning("No objects found to archive")
                return
            
            manifest = []
            
            # Process monthly archives
            monthly_groups = self.group_by_month(objects)
            logger.info(f"Creating {len(monthly_groups)} monthly archives")
            
            for name, group in monthly_groups.items():
                try:
                    archive_info = self.create_archive('monthly', name, group, 
                                                     self.src_bucket, self.archive_bucket)
                    manifest.append(archive_info)
                    logger.info(f"Completed monthly archive: {name}")
                except Exception as e:
                    logger.error(f"Failed to create monthly archive {name}: {e}")
                    raise
            
            # Process quarterly archives
            quarterly_groups = self.group_by_quarter(objects)
            logger.info(f"Creating {len(quarterly_groups)} quarterly archives")
            
            for name, group in quarterly_groups.items():
                try:
                    archive_info = self.create_archive('quarterly', name, group, 
                                                     self.src_bucket, self.archive_bucket)
                    manifest.append(archive_info)
                    logger.info(f"Completed quarterly archive: {name}")
                except Exception as e:
                    logger.error(f"Failed to create quarterly archive {name}: {e}")
                    raise
            
            # Upload manifest
            manifest_json = json.dumps(manifest, indent=2)
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                f.write(manifest_json)
                manifest_path = f.name
            
            try:
                logger.info("Uploading archive manifest")
                self.s3_client.upload_file(manifest_path, self.archive_bucket, 'archives/index.json')
                logger.info(f"Archive generation completed successfully. Created {len(manifest)} archives.")
            finally:
                os.unlink(manifest_path)
                
        except Exception as e:
            logger.error(f"Archive generation failed: {e}")
            raise

def main():
    """Main entry point"""
    try:
        generator = ArchiveGenerator()
        generator.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)

if __name__ == '__main__':
    main()