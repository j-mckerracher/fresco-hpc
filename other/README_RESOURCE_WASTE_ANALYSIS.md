# FRESCO HPC Resource Waste Analysis

## Overview

This script analyzes resource waste in HPC jobs using the FRESCO dataset to answer research question RQ2: "How prevalent and severe is resource waste across different job types and user behaviors?"

## Features

- **Comprehensive Analysis**: CPU waste, memory waste, and composite waste metrics
- **Statistical Summaries**: Percentiles, thresholds, and distribution analysis
- **Visualization**: Publication-quality plots and charts
- **User Behavior Analysis**: Identifies top resource wasters
- **Temporal Patterns**: Daily and hourly waste analysis
- **Memory Efficient**: Chunked data loading for large datasets
- **Configurable**: Test mode and full analysis options

## Key Findings from Test Run (148,528 jobs)

- **Average CPU waste**: 1.9%
- **Average memory waste**: 100.0% (indicating memory estimation needs refinement)
- **Average composite waste**: 41.1%
- **Economic impact**: $6M+ in wasted CPU hours (at $0.10/CPU-hour)
- **Total CPU hours wasted**: 60.6M hours

## Usage

### Quick Test (Sample Data)
```bash
# Create virtual environment
python3 -m venv fresco_env
source fresco_env/bin/activate
pip install pandas pyarrow matplotlib seaborn tqdm

# Run analysis in test mode
python fresco_resource_waste_analysis.py
```

### Full Analysis (All Data)
Edit the script and change:
```python
stats = analyzer.run_full_analysis(test_mode=False)  # Full analysis
```

### Custom Analysis
```python
# Analyze specific years
analyzer.run_full_analysis(limit_years=[2013, 2014])

# Sample fraction of data
analyzer.run_full_analysis(sample_fraction=0.5)
```

## Output Files

The script generates the following outputs in `fresco_analysis_output/`:

### Data Files
- `job_data_with_waste_metrics.csv` - Complete job data with calculated waste metrics
- `waste_statistics_summary.txt` - Comprehensive statistical summary

### Analysis Tables
- `waste_by_exitcode.csv` - Waste breakdown by job outcome
- `waste_by_queue.csv` - Waste breakdown by queue
- `waste_by_duration.csv` - Waste breakdown by job duration
- `top_wasting_users.csv` - Users with highest resource waste

### Visualizations
- `waste_distributions.png` - Distribution histograms and pie charts
- `waste_by_characteristics.png` - Waste patterns by job attributes
- `temporal_patterns.png` - Daily and hourly waste trends
- `user_analysis.png` - Top resource wasting users

## Resource Waste Metrics

### CPU Waste
```
cpu_waste = 1 - (actual_cpu_usage / 100)
```

### Memory Waste
```
mem_waste = 1 - (actual_mem_used / estimated_requested_mem)
```
*Note: Memory estimation assumes 4GB per core*

### Composite Waste Score
```
composite_waste = (0.6 × cpu_waste) + (0.4 × mem_waste)
```

## Data Schema

The script expects FRESCO parquet files with the following structure:
- `jobname` → job exit codes (COMPLETED, FAILED, etc.)
- `host_list` → usernames
- `username` → CPU usage percentages
- `value_memused` → memory usage in GB
- `ncores`, `nhosts` → resource allocation
- Time columns: `time`, `start_time`, `end_time`

## Performance Considerations

- **Memory Usage**: ~2-4GB for test mode, scales with dataset size
- **Processing Time**: ~10 minutes for test data, hours for full dataset
- **Chunked Processing**: Handles large datasets efficiently
- **Sampling**: Use `sample_fraction` for quick analysis

## Academic Paper Integration

The script outputs key findings in a format suitable for academic papers:

- Prevalence statistics (percentages)
- Economic impact estimates
- Statistical significance tests
- Publication-quality visualizations
- Detailed breakdowns by job characteristics

## Customization

### Modify Waste Thresholds
```python
waste_thresholds = [0.5, 0.75, 0.9]  # 50%, 75%, 90%
```

### Adjust Memory Estimation
```python
df['estimated_requested_mem_gb'] = df['ncores'] * 4.0  # GB per core
```

### Change Composite Weight
```python
df['composite_waste'] = (0.6 * df['cpu_waste']) + (0.4 * df['mem_waste'])
```

## Requirements

- Python 3.8+
- pandas
- pyarrow
- matplotlib
- seaborn
- tqdm
- numpy

## Memory Estimation Note

The current memory waste calculation assumes 4GB per core, which may need adjustment based on:
- Actual cluster specifications
- Job submission parameters
- Queue-specific memory limits

Consider refining this based on your specific HPC environment.