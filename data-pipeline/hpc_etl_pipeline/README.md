
# HPC ETL Pipeline

This project provides a modular and extensible ETL (Extract, Transform, Load) pipeline for processing High-Performance Computing (HPC) cluster data. It is designed to be configuration-driven, allowing for easy adaptation to new datasets and processing requirements.

## Features

*   **Configuration-Driven:** Define your entire ETL process in a single YAML file.
*   **Modular Architecture:** Easily extend the pipeline with new extractors, transformers, and loaders.
*   **Parallel Processing:** Leverages a thread pool for efficient I/O-bound tasks.
*   **State Management:** Tracks processing state to allow for resumable pipelines.
*   **Data Versioning:** Automatically versions output files.
*   **Resource Management:** Includes utilities for managing disk space and optimizing memory usage.

## Architecture

The pipeline is composed of several core components that work together to process data.

### Directory Structure

```
hpc_etl_pipeline/
├── config/                 # Configuration files
├── data/                   # Data files (input, output, temp)
├── logs/                   # Log files
├── src/                    # Source code
│   ├── core/               # Core pipeline components
│   ├── extractors/         # Data extraction modules
│   ├── loaders/            # Data loading modules
│   ├── transformers/       # Data transformation modules
│   └── utils/              # Utility functions
├── tests/                  # Tests
├── run_pipeline.py         # Main entry point
└── requirements.txt        # Python dependencies
```

### Core Components

*   **`Pipeline`:** The main orchestrator that manages the ETL process.
*   **`ConfigLoader`:** Loads and validates dataset configurations.
*   **`BaseExtractor`:** Abstract base class for all data extractors.
*   **`BaseTransformer`:** Abstract base class for all data transformers.
*   **`BaseLoader`:** Abstract base class for all data loaders.

## Configuration

The pipeline is configured using YAML files. The main configuration is split into two parts: the dataset configuration and the pipeline configuration.

### Dataset Configuration

Each dataset has its own configuration file (e.g., `config/datasets/conte_hpc.yaml`). This file defines the specifics of the dataset, including:

*   **`dataset`:** Metadata about the dataset.
*   **`source`:** Where to find the data (e.g., HTTP, Globus).
*   **`output`:** How to format and store the output.
*   **`transformations`:** The sequence of transformations to apply.
*   **`processing`:** Parameters for the processing engine.
*   **`validation`:** Rules for validating the data.

### Pipeline Configuration

The `config/pipeline_config.yaml` file contains global settings for the pipeline, such as logging configuration and default values.

## Usage

### Installation

1.  **Create a virtual environment:**

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

### Running the Pipeline

To run the pipeline, use the `run_pipeline.py` script with the path to your dataset configuration file:

```bash
python run_pipeline.py --config config/datasets/conte_hpc.yaml
```

### Watching for Files

To watch a directory for new files and process them as they arrive, use the `--watch` flag:

```bash
python run_pipeline.py --config config/datasets/conte_hpc.yaml --watch
```

## Development

### Creating a New Transformer

To create a new transformer, you need to:

1.  Create a new Python class that inherits from `BaseTransformer`.
2.  Implement the `transform` method.
3.  Add your new transformer to the `hpc_etl_pipeline/src/transformers` directory.
4.  Reference your new transformer in your dataset configuration file.

### Running Tests

To run the test suite, use the following command:

```bash
pytest
```
