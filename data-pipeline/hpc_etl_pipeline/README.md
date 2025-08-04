# HPC ETL Pipeline

A modular, configuration-driven data processing pipeline for HPC cluster monitoring data. This pipeline transforms raw CSV data from various sources into standardized Parquet format optimized for analysis.

## Features

- **Modular Architecture**: Configurable extractors, transformers, and loaders
- **Multiple Data Sources**: HTTP repositories, local filesystem, Globus transfers
- **HPC-Specific Transformations**: Block I/O, CPU, Memory, and NFS metrics processing
- **Chunking Support**: Automatic file splitting for large datasets
- **File Watching**: Real-time processing of incoming files
- **Memory Optimization**: Efficient processing of large datasets
- **Robust Error Handling**: Retry logic and validation

## Quick Start

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Dataset**:
   ```bash
   # Copy and modify the example configuration
   cp config/datasets/conte_hpc.yaml config/datasets/my_dataset.yaml
   ```

3. **Run Pipeline**:
   ```bash
   # Process from HTTP source
   python run_pipeline.py --config config/datasets/my_dataset.yaml
   
   # Process single file (Globus)
   python run_pipeline.py --config config/datasets/my_dataset.yaml --file /path/to/data.csv
   
   # Watch directory
   python run_pipeline.py --config config/datasets/my_dataset.yaml --watch --source-dir /input
   ```

## Configuration

Dataset configurations are stored in YAML format in `config/datasets/`. Here's the structure:

```yaml
dataset:
  name: "my_cluster"
  type: "hpc_cluster"
  version: "1.0"

source:
  type: "remote_http"  # or "local_fs", "globus"
  base_url: "https://data.example.com/repository/"
  file_patterns:
    - "block.csv"
    - "cpu.csv"
    - "mem.csv"
    - "llite.csv"

transformations:
  - type: "standardize_columns"
    output_schema:
      - "Job Id"
      - "Host"
      - "Event" 
      - "Value"
      - "Units"
      - "Timestamp"

output:
  format: "parquet"
  compression: "snappy"
  chunking:
    enabled: true
    max_size_gb: 2.0
```

## Supported File Types

The pipeline includes specialized transformers for common HPC monitoring files:

- **block.csv**: Disk I/O throughput (GB/s)
- **cpu.csv**: CPU user percentage
- **mem.csv**: Memory usage metrics (GB)
- **llite.csv**: NFS transfer rates (MB/s)

## Usage Examples

### HTTP Repository Processing
```bash
python run_pipeline.py --config config/datasets/conte_hpc.yaml
```

### Single File Processing (Globus)
```bash
python run_pipeline.py --config config/datasets/conte_hpc.yaml --file /globus/data/cpu.csv
```

### Local Directory Processing
```bash
python run_pipeline.py --config config/datasets/conte_hpc.yaml --folder /local/data/2024-01
```

### File Watching Mode
```bash
python run_pipeline.py --config config/datasets/conte_hpc.yaml --watch --source-dir /monitoring/input
```

## Architecture

```
hpc_etl_pipeline/
├── src/
│   ├── core/                 # Core pipeline logic
│   │   ├── pipeline.py       # Main orchestrator
│   │   ├── config_loader.py  # Configuration management
│   │   └── exceptions.py     # Custom exceptions
│   ├── extractors/           # Data extraction
│   │   ├── http_extractor.py    # Remote HTTP sources
│   │   ├── local_extractor.py   # Local filesystem
│   │   └── globus_extractor.py  # Globus transfers
│   ├── transformers/         # Data transformation
│   │   ├── hpc_transformers.py      # HPC-specific logic
│   │   └── generic_transformers.py  # Reusable transforms
│   ├── loaders/             # Data output
│   │   └── parquet_loader.py   # Parquet with chunking
│   ├── utils/               # Utilities
│   │   └── validators.py       # Data validation
│   └── watchers/            # File monitoring
│       └── file_watcher.py     # Directory watching
├── config/                  # Configuration files
│   └── datasets/            # Dataset-specific configs
└── run_pipeline.py         # Main entry point
```

## Adding New Datasets

1. Create a new configuration file in `config/datasets/`:

```yaml
dataset:
  name: "new_cluster"
  type: "hpc_cluster"
  version: "1.0"

source:
  type: "local_fs"
  base_path: "/data/new_cluster"
  file_patterns:
    - "*.csv"

# ... other configuration
```

2. Run the pipeline:

```bash
python run_pipeline.py --config config/datasets/new_cluster.yaml
```

## Extending the Pipeline

### Adding New Extractors

1. Create a new extractor class inheriting from `BaseExtractor`
2. Register it in `Pipeline.EXTRACTOR_REGISTRY`
3. Configure the source type in your dataset YAML

### Adding New Transformers

1. Create transformer class inheriting from `BaseTransformer`
2. Add to the appropriate transformer module
3. Register in pipeline or use generic `CompositeTransformer`

### Adding New Loaders

1. Create loader class inheriting from `BaseLoader`
2. Implement the `load()` and `validate_output()` methods
3. Configure in the pipeline initialization

## Monitoring and Logging

The pipeline provides comprehensive logging:

```bash
# Set log level
python run_pipeline.py --config config.yaml --log-level DEBUG

# Log to file
python run_pipeline.py --config config.yaml --log-file pipeline.log
```

## Performance Tuning

Key configuration parameters for performance:

```yaml
processing:
  max_workers: 4          # Parallel download threads
  batch_size: 500000      # Rows per batch
  memory_limit_gb: 80     # Memory limit
  
output:
  chunking:
    max_size_gb: 2.0      # Max chunk size
    min_rows_per_chunk: 500000  # Min rows per chunk
```

## Contributing

1. Follow the existing code structure
2. Add tests for new components
3. Update configuration examples
4. Document new features in README