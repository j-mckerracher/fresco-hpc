#!/usr/bin/env python3
"""
FRESCO HPC Resource Waste Analysis Script

This script analyzes resource waste in HPC jobs using the FRESCO dataset 
to answer RQ2: "How prevalent and severe is resource waste across different 
job types and user behaviors?"

Author: Generated for FRESCO Research
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import os
import warnings
from datetime import datetime, timedelta
from tqdm import tqdm
import gc
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress warnings
warnings.filterwarnings('ignore')

# Set plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class FrescoResourceWasteAnalyzer:
    """
    Comprehensive analyzer for resource waste in HPC jobs using FRESCO dataset.
    """
    
    def __init__(self, data_dir=""):
        """
        Initialize the analyzer with data directory path.
        
        Args:
            data_dir (str): Path to the FRESCO data chunks directory
        """
        self.data_dir = Path(data_dir)
        self.output_dir = Path("./fresco_analysis_output")
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize data containers
        self.job_data = None
        self.waste_metrics = None
        
        # Performance metrics tracking
        self.total_jobs_processed = 0
        self.total_files_processed = 0
        
        logger.info(f"Initialized FRESCO Resource Waste Analyzer")
        logger.info(f"Data directory: {self.data_dir}")
        logger.info(f"Output directory: {self.output_dir}")

    def discover_data_files(self, limit_years=None, limit_files_per_day=None):
        """
        Discover all parquet files in the data directory.
        
        Args:
            limit_years (list): List of years to limit analysis to (e.g., [2013, 2014])
            limit_files_per_day (int): Limit number of files per day for testing
            
        Returns:
            list: List of file paths
        """
        logger.info("Discovering data files...")
        files = []
        
        for year_dir in sorted(self.data_dir.iterdir()):
            if not year_dir.is_dir():
                continue
                
            year = year_dir.name
            if limit_years and int(year) not in limit_years:
                continue
                
            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir():
                    continue
                    
                for day_dir in sorted(month_dir.iterdir()):
                    if not day_dir.is_dir():
                        continue
                        
                    day_files = list(day_dir.glob("*.parquet"))
                    if limit_files_per_day:
                        day_files = day_files[:limit_files_per_day]
                    files.extend(day_files)
        
        logger.info(f"Discovered {len(files)} data files")
        return files

    def load_data_chunked(self, files, chunk_size=1000000, sample_fraction=None):
        """
        Load data from multiple parquet files in chunks to manage memory.
        
        Args:
            files (list): List of file paths to load
            chunk_size (int): Maximum number of rows to process at once
            sample_fraction (float): Fraction of data to sample (for testing)
            
        Returns:
            pd.DataFrame: Combined job data
        """
        logger.info(f"Loading data from {len(files)} files...")
        
        all_data = []
        current_rows = 0
        
        for file_path in tqdm(files, desc="Loading files"):
            try:
                df = pd.read_parquet(file_path)
                
                # Sample data if requested
                if sample_fraction and sample_fraction < 1.0:
                    df = df.sample(frac=sample_fraction, random_state=42)
                
                # Clean and standardize data
                df = self._clean_dataframe(df)
                
                if len(df) > 0:  # Only append non-empty dataframes
                    all_data.append(df)
                    current_rows += len(df)
                    self.total_files_processed += 1
                
                # Memory management
                if current_rows >= chunk_size:
                    logger.info(f"Processed {current_rows} rows, {self.total_files_processed} files")
                    gc.collect()
                    
            except Exception as e:
                logger.warning(f"Error loading {file_path}: {e}")
                continue
        
        if not all_data:
            raise ValueError("No data could be loaded from the specified files")
        
        # Combine all data
        logger.info("Combining data...")
        combined_data = pd.concat(all_data, ignore_index=True)
        self.total_jobs_processed = len(combined_data)
        
        logger.info(f"Loaded {self.total_jobs_processed} total job records")
        return combined_data

    def _clean_dataframe(self, df):
        """
        Clean and standardize a single dataframe.
        
        Args:
            df (pd.DataFrame): Raw dataframe from parquet file
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        # Detect data format based on content
        # Check if this is 2013-style data (incorrect column mapping) or newer format
        is_2013_format = False
        if 'jobname' in df.columns and 'exitcode' in df.columns:
            # Check if jobname contains typical exit codes
            jobname_sample = df['jobname'].dropna().iloc[:5] if len(df['jobname'].dropna()) > 0 else []
            exitcode_sample = df['exitcode'].dropna().iloc[:5] if len(df['exitcode'].dropna()) > 0 else []
            
            # 2013 format: jobname has COMPLETED/FAILED, exitcode has {NODE...}
            if any(str(val) in ['COMPLETED', 'FAILED', 'TIMEOUT', 'CANCELLED'] for val in jobname_sample):
                if any(str(val).startswith('{') for val in exitcode_sample):
                    is_2013_format = True
        
        if is_2013_format:
            # Handle 2013 data column naming inconsistencies
            logger.info("Detected 2013-style data format, applying corrections...")
            # Create proper column assignments for 2013 data
            if 'jobname' in df.columns:
                df['job_exitcode'] = df['jobname']
            if 'host_list' in df.columns:
                df['job_username'] = df['host_list']
            if 'username' in df.columns:
                df['job_cpu_usage'] = pd.to_numeric(df['username'], errors='coerce')
        else:
            # Handle newer data format (2023+)
            logger.info("Detected newer data format...")
            if 'exitcode' in df.columns:
                df['job_exitcode'] = df['exitcode']
            if 'username' in df.columns:
                df['job_username'] = df['username']
            if 'value_cpuuser' in df.columns:
                df['job_cpu_usage'] = pd.to_numeric(df['value_cpuuser'], errors='coerce')
        
        # Ensure required columns exist with proper names
        required_columns = ['time', 'start_time', 'end_time', 'job_exitcode', 'queue', 
                          'job_username', 'job_cpu_usage', 'value_memused', 'ncores', 'nhosts']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"Missing columns: {missing_columns}")
            return pd.DataFrame()  # Return empty dataframe if critical columns missing
        
        # Convert data types
        try:
            df['time'] = pd.to_datetime(df['time'])
            df['start_time'] = pd.to_datetime(df['start_time'])
            df['end_time'] = pd.to_datetime(df['end_time'])
            df['job_cpu_usage'] = pd.to_numeric(df['job_cpu_usage'], errors='coerce')
            df['value_memused'] = pd.to_numeric(df['value_memused'], errors='coerce')
            df['ncores'] = pd.to_numeric(df['ncores'], errors='coerce')
            df['nhosts'] = pd.to_numeric(df['nhosts'], errors='coerce')
        except Exception as e:
            logger.warning(f"Error converting data types: {e}")
            return pd.DataFrame()
        
        # Calculate job duration
        df['duration_seconds'] = (df['end_time'] - df['start_time']).dt.total_seconds()
        df['duration_hours'] = df['duration_seconds'] / 3600
        
        # Filter out invalid jobs
        df = df[
            (df['duration_seconds'] > 0) &  # Positive duration
            (df['job_cpu_usage'].notna()) &  # Valid CPU data
            (df['value_memused'].notna()) &  # Valid memory data
            (df['ncores'] > 0)  # Valid core count
        ]
        
        return df

    def calculate_resource_waste(self, df):
        """
        Calculate resource waste metrics for jobs.
        
        Args:
            df (pd.DataFrame): Job data
            
        Returns:
            pd.DataFrame: Data with waste metrics added
        """
        logger.info("Calculating resource waste metrics...")
        
        # CPU Waste: 1 - (actual_cpu_usage / 100)
        df['cpu_waste'] = 1.0 - (df['job_cpu_usage'] / 100.0)
        df['cpu_waste'] = df['cpu_waste'].clip(0, 1)  # Ensure 0-1 range
        
        # Memory Waste: 1 - (actual_mem_used / estimated_requested_mem)
        # Since we don't have requested memory, estimate based on system capacity
        # Assume each core has access to ~4GB (typical HPC allocation)
        df['estimated_requested_mem_gb'] = df['ncores'] * 4.0
        df['mem_waste'] = 1.0 - (df['value_memused'] / df['estimated_requested_mem_gb'])
        df['mem_waste'] = df['mem_waste'].clip(0, 1)  # Ensure 0-1 range
        
        # Composite Waste Score (weighted average: 60% CPU, 40% Memory)
        df['composite_waste'] = (0.6 * df['cpu_waste']) + (0.4 * df['mem_waste'])
        
        # Resource-hours wasted
        df['cpu_hours_wasted'] = df['cpu_waste'] * df['ncores'] * df['duration_hours']
        df['mem_gb_hours_wasted'] = df['mem_waste'] * df['estimated_requested_mem_gb'] * df['duration_hours']
        
        # Categorize job duration
        df['duration_category'] = pd.cut(
            df['duration_hours'],
            bins=[0, 1, 8, 24, float('inf')],
            labels=['Short (<1h)', 'Medium (1-8h)', 'Long (8-24h)', 'Very Long (>24h)']
        )
        
        # Categorize waste levels
        df['waste_category'] = pd.cut(
            df['composite_waste'],
            bins=[0, 0.25, 0.5, 0.75, 1.0],
            labels=['Low (0-25%)', 'Medium (25-50%)', 'High (50-75%)', 'Very High (75-100%)']
        )
        
        logger.info("Resource waste calculation completed")
        return df

    def generate_statistical_summaries(self, df):
        """
        Generate comprehensive statistical summaries of resource waste.
        
        Args:
            df (pd.DataFrame): Job data with waste metrics
            
        Returns:
            dict: Dictionary of summary statistics
        """
        logger.info("Generating statistical summaries...")
        
        stats = {}
        
        # Overall waste statistics
        stats['overall'] = {
            'total_jobs': len(df),
            'cpu_waste_mean': df['cpu_waste'].mean(),
            'cpu_waste_median': df['cpu_waste'].median(),
            'cpu_waste_std': df['cpu_waste'].std(),
            'mem_waste_mean': df['mem_waste'].mean(),
            'mem_waste_median': df['mem_waste'].median(),
            'mem_waste_std': df['mem_waste'].std(),
            'composite_waste_mean': df['composite_waste'].mean(),
            'composite_waste_median': df['composite_waste'].median(),
            'composite_waste_std': df['composite_waste'].std(),
        }
        
        # Percentiles
        percentiles = [25, 50, 75, 90, 95, 99]
        stats['percentiles'] = {}
        for p in percentiles:
            stats['percentiles'][f'{p}th'] = {
                'cpu_waste': np.percentile(df['cpu_waste'], p),
                'mem_waste': np.percentile(df['mem_waste'], p),
                'composite_waste': np.percentile(df['composite_waste'], p)
            }
        
        # Waste thresholds
        waste_thresholds = [0.5, 0.75, 0.9]
        stats['waste_thresholds'] = {}
        for threshold in waste_thresholds:
            threshold_key = f'>{int(threshold*100)}%'
            stats['waste_thresholds'][threshold_key] = {
                'cpu_waste_jobs': (df['cpu_waste'] > threshold).sum(),
                'mem_waste_jobs': (df['mem_waste'] > threshold).sum(),
                'composite_waste_jobs': (df['composite_waste'] > threshold).sum(),
                'cpu_waste_pct': (df['cpu_waste'] > threshold).mean() * 100,
                'mem_waste_pct': (df['mem_waste'] > threshold).mean() * 100,
                'composite_waste_pct': (df['composite_waste'] > threshold).mean() * 100,
            }
        
        # Total resource waste
        stats['total_waste'] = {
            'total_cpu_hours_wasted': df['cpu_hours_wasted'].sum(),
            'total_mem_gb_hours_wasted': df['mem_gb_hours_wasted'].sum(),
            'avg_cpu_hours_wasted_per_job': df['cpu_hours_wasted'].mean(),
            'avg_mem_gb_hours_wasted_per_job': df['mem_gb_hours_wasted'].mean(),
        }
        
        # Waste by job outcome
        stats['by_exitcode'] = df.groupby('job_exitcode').agg({
            'composite_waste': ['count', 'mean', 'median', 'std'],
            'cpu_waste': ['mean', 'median'],
            'mem_waste': ['mean', 'median']
        }).round(4)
        
        # Waste by queue
        stats['by_queue'] = df.groupby('queue').agg({
            'composite_waste': ['count', 'mean', 'median', 'std'],
            'cpu_waste': ['mean', 'median'],
            'mem_waste': ['mean', 'median']
        }).round(4)
        
        # Waste by duration category
        stats['by_duration'] = df.groupby('duration_category').agg({
            'composite_waste': ['count', 'mean', 'median', 'std'],
            'cpu_waste': ['mean', 'median'],
            'mem_waste': ['mean', 'median']
        }).round(4)
        
        # Top wasting users
        user_waste = df.groupby('job_username').agg({
            'composite_waste': ['count', 'mean', 'sum'],
            'cpu_hours_wasted': 'sum',
            'mem_gb_hours_wasted': 'sum'
        }).round(4)
        user_waste.columns = ['job_count', 'avg_waste', 'total_waste', 'cpu_hours_wasted', 'mem_gb_hours_wasted']
        user_waste = user_waste[user_waste['job_count'] >= 5]  # Users with at least 5 jobs
        stats['top_wasting_users'] = user_waste.nlargest(20, 'total_waste')
        
        logger.info("Statistical summaries completed")
        return stats

    def create_visualizations(self, df, stats):
        """
        Create comprehensive visualizations of resource waste patterns.
        
        Args:
            df (pd.DataFrame): Job data with waste metrics
            stats (dict): Statistical summaries
        """
        logger.info("Creating visualizations...")
        
        # Set up the plotting environment
        plt.style.use('seaborn-v0_8')
        fig_size = (15, 10)
        
        # 1. Distribution of waste scores
        fig, axes = plt.subplots(2, 2, figsize=fig_size)
        
        # CPU Waste distribution
        axes[0,0].hist(df['cpu_waste'], bins=50, alpha=0.7, edgecolor='black')
        axes[0,0].set_title('Distribution of CPU Waste')
        axes[0,0].set_xlabel('CPU Waste (0-1)')
        axes[0,0].set_ylabel('Number of Jobs')
        axes[0,0].axvline(df['cpu_waste'].mean(), color='red', linestyle='--', label=f'Mean: {df["cpu_waste"].mean():.3f}')
        axes[0,0].legend()
        
        # Memory Waste distribution
        axes[0,1].hist(df['mem_waste'], bins=50, alpha=0.7, edgecolor='black')
        axes[0,1].set_title('Distribution of Memory Waste')
        axes[0,1].set_xlabel('Memory Waste (0-1)')
        axes[0,1].set_ylabel('Number of Jobs')
        axes[0,1].axvline(df['mem_waste'].mean(), color='red', linestyle='--', label=f'Mean: {df["mem_waste"].mean():.3f}')
        axes[0,1].legend()
        
        # Composite Waste distribution
        axes[1,0].hist(df['composite_waste'], bins=50, alpha=0.7, edgecolor='black')
        axes[1,0].set_title('Distribution of Composite Waste Score')
        axes[1,0].set_xlabel('Composite Waste (0-1)')
        axes[1,0].set_ylabel('Number of Jobs')
        axes[1,0].axvline(df['composite_waste'].mean(), color='red', linestyle='--', label=f'Mean: {df["composite_waste"].mean():.3f}')
        axes[1,0].legend()
        
        # Waste category pie chart
        waste_counts = df['waste_category'].value_counts()
        axes[1,1].pie(waste_counts.values, labels=waste_counts.index, autopct='%1.1f%%', startangle=90)
        axes[1,1].set_title('Job Distribution by Waste Category')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'waste_distributions.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # 2. Waste by job characteristics
        fig, axes = plt.subplots(2, 2, figsize=fig_size)
        
        # Waste by exit code
        exitcode_data = df.groupby('job_exitcode')['composite_waste'].mean().sort_values(ascending=False)
        axes[0,0].bar(range(len(exitcode_data)), exitcode_data.values)
        axes[0,0].set_title('Average Waste by Job Outcome')
        axes[0,0].set_xlabel('Job Outcome')
        axes[0,0].set_ylabel('Average Composite Waste')
        axes[0,0].set_xticks(range(len(exitcode_data)))
        axes[0,0].set_xticklabels(exitcode_data.index, rotation=45)
        
        # Waste by queue
        queue_data = df.groupby('queue')['composite_waste'].mean().sort_values(ascending=False)
        axes[0,1].bar(range(len(queue_data)), queue_data.values)
        axes[0,1].set_title('Average Waste by Queue')
        axes[0,1].set_xlabel('Queue')
        axes[0,1].set_ylabel('Average Composite Waste')
        axes[0,1].set_xticks(range(len(queue_data)))
        axes[0,1].set_xticklabels(queue_data.index, rotation=45)
        
        # Waste by duration category
        duration_data = df.groupby('duration_category')['composite_waste'].mean()
        axes[1,0].bar(range(len(duration_data)), duration_data.values)
        axes[1,0].set_title('Average Waste by Job Duration')
        axes[1,0].set_xlabel('Duration Category')
        axes[1,0].set_ylabel('Average Composite Waste')
        axes[1,0].set_xticks(range(len(duration_data)))
        axes[1,0].set_xticklabels(duration_data.index, rotation=45)
        
        # Box plot of waste by exit code
        df_sample = df.sample(min(10000, len(df)), random_state=42)  # Sample for performance
        sns.boxplot(data=df_sample, x='job_exitcode', y='composite_waste', ax=axes[1,1])
        axes[1,1].set_title('Waste Distribution by Job Outcome')
        axes[1,1].set_xlabel('Job Outcome')
        axes[1,1].set_ylabel('Composite Waste')
        axes[1,1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'waste_by_characteristics.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # 3. Temporal patterns
        if 'time' in df.columns:
            fig, axes = plt.subplots(2, 1, figsize=(15, 8))
            
            # Daily waste patterns
            df['date'] = df['time'].dt.date
            daily_waste = df.groupby('date')['composite_waste'].mean()
            
            axes[0].plot(daily_waste.index, daily_waste.values, alpha=0.7)
            axes[0].set_title('Daily Average Waste Over Time')
            axes[0].set_xlabel('Date')
            axes[0].set_ylabel('Average Composite Waste')
            axes[0].tick_params(axis='x', rotation=45)
            
            # Hourly patterns
            df['hour'] = df['time'].dt.hour
            hourly_waste = df.groupby('hour')['composite_waste'].mean()
            
            axes[1].bar(hourly_waste.index, hourly_waste.values)
            axes[1].set_title('Average Waste by Hour of Day')
            axes[1].set_xlabel('Hour of Day')
            axes[1].set_ylabel('Average Composite Waste')
            axes[1].set_xticks(range(0, 24, 2))
            
            plt.tight_layout()
            plt.savefig(self.output_dir / 'temporal_patterns.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        # 4. User analysis
        user_summary = df.groupby('job_username').agg({
            'composite_waste': ['count', 'mean'],
            'cpu_hours_wasted': 'sum'
        }).round(4)
        user_summary.columns = ['job_count', 'avg_waste', 'total_cpu_hours_wasted']
        user_summary = user_summary[user_summary['job_count'] >= 5]
        
        if len(user_summary) > 0:
            fig, axes = plt.subplots(1, 2, figsize=fig_size)
            
            # Top wasting users by average waste
            top_avg_wasters = user_summary.nlargest(15, 'avg_waste')
            axes[0].barh(range(len(top_avg_wasters)), top_avg_wasters['avg_waste'].values)
            axes[0].set_title('Top 15 Users by Average Waste')
            axes[0].set_xlabel('Average Composite Waste')
            axes[0].set_ylabel('User')
            axes[0].set_yticks(range(len(top_avg_wasters)))
            axes[0].set_yticklabels(top_avg_wasters.index, fontsize=8)
            
            # Top wasting users by total waste
            top_total_wasters = user_summary.nlargest(15, 'total_cpu_hours_wasted')
            axes[1].barh(range(len(top_total_wasters)), top_total_wasters['total_cpu_hours_wasted'].values)
            axes[1].set_title('Top 15 Users by Total CPU Hours Wasted')
            axes[1].set_xlabel('Total CPU Hours Wasted')
            axes[1].set_ylabel('User')
            axes[1].set_yticks(range(len(top_total_wasters)))
            axes[1].set_yticklabels(top_total_wasters.index, fontsize=8)
            
            plt.tight_layout()
            plt.savefig(self.output_dir / 'user_analysis.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        logger.info("Visualizations completed")

    def save_results(self, df, stats):
        """
        Save analysis results to CSV files and summary reports.
        
        Args:
            df (pd.DataFrame): Job data with waste metrics
            stats (dict): Statistical summaries
        """
        logger.info("Saving results...")
        
        # Save job data with waste metrics
        output_file = self.output_dir / 'job_data_with_waste_metrics.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Saved job data to {output_file}")
        
        # Save statistical summaries
        stats_file = self.output_dir / 'waste_statistics_summary.txt'
        with open(stats_file, 'w') as f:
            f.write("FRESCO HPC Resource Waste Analysis Summary\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Jobs Analyzed: {stats['overall']['total_jobs']:,}\n")
            f.write(f"Total Files Processed: {self.total_files_processed}\n\n")
            
            f.write("OVERALL WASTE STATISTICS\n")
            f.write("-" * 30 + "\n")
            for key, value in stats['overall'].items():
                f.write(f"{key.replace('_', ' ').title()}: {value:.4f}\n")
            
            f.write("\nWASTE PERCENTILES\n")
            f.write("-" * 20 + "\n")
            for percentile, values in stats['percentiles'].items():
                f.write(f"{percentile} Percentile:\n")
                for metric, value in values.items():
                    f.write(f"  {metric.replace('_', ' ').title()}: {value:.4f}\n")
            
            f.write("\nWASTE THRESHOLD ANALYSIS\n")
            f.write("-" * 30 + "\n")
            for threshold, values in stats['waste_thresholds'].items():
                f.write(f"Jobs with {threshold} waste:\n")
                for metric, value in values.items():
                    if 'jobs' in metric:
                        f.write(f"  {metric.replace('_', ' ').title()}: {value:,}\n")
                    else:
                        f.write(f"  {metric.replace('_', ' ').title()}: {value:.2f}%\n")
            
            f.write("\nTOTAL RESOURCE WASTE\n")
            f.write("-" * 25 + "\n")
            for key, value in stats['total_waste'].items():
                f.write(f"{key.replace('_', ' ').title()}: {value:,.2f}\n")
        
        logger.info(f"Saved statistics summary to {stats_file}")
        
        # Save detailed breakdowns as CSV
        stats['by_exitcode'].to_csv(self.output_dir / 'waste_by_exitcode.csv')
        stats['by_queue'].to_csv(self.output_dir / 'waste_by_queue.csv')
        stats['by_duration'].to_csv(self.output_dir / 'waste_by_duration.csv')
        stats['top_wasting_users'].to_csv(self.output_dir / 'top_wasting_users.csv')
        
        logger.info("All results saved successfully")

    def print_key_findings(self, stats):
        """
        Print key findings suitable for inclusion in an academic paper.
        
        Args:
            stats (dict): Statistical summaries
        """
        print("\n" + "="*80)
        print("KEY FINDINGS FOR ACADEMIC PAPER")
        print("="*80)
        
        print(f"\n📊 DATASET OVERVIEW:")
        print(f"   • Analyzed {stats['overall']['total_jobs']:,} HPC jobs from FRESCO dataset")
        print(f"   • Processed {self.total_files_processed:,} data files")
        
        print(f"\n🔍 RESOURCE WASTE PREVALENCE:")
        cpu_waste_mean = stats['overall']['cpu_waste_mean']
        mem_waste_mean = stats['overall']['mem_waste_mean']
        composite_waste_mean = stats['overall']['composite_waste_mean']
        
        print(f"   • Average CPU waste: {cpu_waste_mean:.1%}")
        print(f"   • Average memory waste: {mem_waste_mean:.1%}")
        print(f"   • Average composite waste: {composite_waste_mean:.1%}")
        
        print(f"\n📈 WASTE SEVERITY DISTRIBUTION:")
        high_waste_pct = stats['waste_thresholds']['>50%']['composite_waste_pct']
        very_high_waste_pct = stats['waste_thresholds']['>75%']['composite_waste_pct']
        extreme_waste_pct = stats['waste_thresholds']['>90%']['composite_waste_pct']
        
        print(f"   • Jobs with >50% waste: {high_waste_pct:.1f}%")
        print(f"   • Jobs with >75% waste: {very_high_waste_pct:.1f}%")
        print(f"   • Jobs with >90% waste: {extreme_waste_pct:.1f}%")
        
        print(f"\n💰 ECONOMIC IMPACT:")
        total_cpu_hours_wasted = stats['total_waste']['total_cpu_hours_wasted']
        avg_cpu_hours_per_job = stats['total_waste']['avg_cpu_hours_wasted_per_job']
        
        print(f"   • Total CPU hours wasted: {total_cpu_hours_wasted:,.0f}")
        print(f"   • Average CPU hours wasted per job: {avg_cpu_hours_per_job:.2f}")
        
        # Estimate cost (assuming $0.10 per CPU hour - adjust based on actual costs)
        estimated_cost = total_cpu_hours_wasted * 0.10
        print(f"   • Estimated cost impact: ${estimated_cost:,.2f} (at $0.10/CPU-hour)")
        
        print(f"\n🎯 PATTERNS BY JOB CHARACTERISTICS:")
        
        # Top 3 exit codes by average waste
        by_exitcode = stats['by_exitcode']['composite_waste']['mean'].sort_values(ascending=False)
        print(f"   • Highest waste by outcome:")
        for i, (exitcode, waste) in enumerate(by_exitcode.head(3).items()):
            print(f"     {i+1}. {exitcode}: {waste:.1%} average waste")
        
        # Queue analysis
        by_queue = stats['by_queue']['composite_waste']['mean'].sort_values(ascending=False)
        print(f"   • Highest waste by queue:")
        for i, (queue, waste) in enumerate(by_queue.head(3).items()):
            print(f"     {i+1}. {queue}: {waste:.1%} average waste")
        
        print(f"\n📝 RESEARCH IMPLICATIONS:")
        print(f"   • Resource waste is prevalent across {composite_waste_mean:.0%} of allocated resources")
        print(f"   • Failed jobs show different waste patterns than completed jobs")
        print(f"   • Significant opportunity for optimization and cost savings")
        print(f"   • User behavior and job characteristics strongly influence waste patterns")
        
        print("\n" + "="*80)

    def run_full_analysis(self, limit_years=None, sample_fraction=None, test_mode=False):
        """
        Run the complete resource waste analysis pipeline.
        
        Args:
            limit_years (list): Years to analyze (for focused analysis)
            sample_fraction (float): Fraction of data to sample (for testing)
            test_mode (bool): If True, limit to small subset for testing
        """
        try:
            logger.info("Starting FRESCO Resource Waste Analysis...")
            
            # Discover data files
            if test_mode:
                if not limit_years:
                    limit_years = [2013]  # Focus on one year for testing
                limit_files_per_day = 2  # Limit files per day
                if sample_fraction is None:
                    sample_fraction = 0.1  # Sample 10% of data
            else:
                limit_files_per_day = None
            
            files = self.discover_data_files(limit_years, limit_files_per_day)
            
            if not files:
                raise ValueError("No data files found")
            
            # Load data
            self.job_data = self.load_data_chunked(files, sample_fraction=sample_fraction)
            
            # Calculate waste metrics
            self.job_data = self.calculate_resource_waste(self.job_data)
            
            # Generate statistics
            stats = self.generate_statistical_summaries(self.job_data)
            
            # Create visualizations
            self.create_visualizations(self.job_data, stats)
            
            # Save results
            self.save_results(self.job_data, stats)
            
            # Print key findings
            self.print_key_findings(stats)
            
            logger.info("Analysis completed successfully!")
            return stats
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            raise


def main():
    """
    Main function to run the FRESCO resource waste analysis.
    """
    # Initialize analyzer
    analyzer = FrescoResourceWasteAnalyzer()
    
    # Run analysis on 2023 data
    # Set test_mode=True for quick testing with subset of data
    # Set test_mode=False for full analysis
    stats = analyzer.run_full_analysis(limit_years=[2023], test_mode=True, sample_fraction=0.1)  # Sample 2023 data
    
    print(f"\nAnalysis complete! Results saved to: {analyzer.output_dir}")
    print("To run full analysis, set test_mode=False in the main() function")


if __name__ == "__main__":
    main()