"""Configuration management for the HPC ETL Pipeline."""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from .exceptions import ConfigurationError


class ConfigLoader:
    """Loads and validates pipeline configurations."""
    
    DEFAULT_CONFIG = {
        "processing": {
            "max_workers": 4,
            "batch_size": 500000,
            "memory_limit_gb": 80,
            "temp_directory": "./temp"
        },
        "output": {
            "format": "parquet",
            "compression": "snappy",
            "chunking": {
                "enabled": True,
                "max_size_gb": 2.0,
                "min_rows_per_chunk": 500000
            }
        },
        "validation": {
            "min_rows": 1,
            "max_file_size_gb": 10
        }
    }
    
    def __init__(self):
        self.config: Optional[Dict[str, Any]] = None
    
    def load_dataset_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load dataset configuration from YAML file.
        
        Args:
            config_path: Path to the configuration file
            
        Returns:
            Dictionary containing the configuration
            
        Raises:
            ConfigurationError: If configuration is invalid or file not found
        """
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            # Merge with defaults
            merged_config = self.merge_with_defaults(config)
            
            # Validate configuration
            self.validate_config(merged_config)
            
            self.config = merged_config
            return merged_config
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML configuration: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {e}")
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """
        Validate the configuration structure and values.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            True if valid
            
        Raises:
            ConfigurationError: If configuration is invalid
        """
        required_sections = ['dataset', 'source', 'output']
        
        for section in required_sections:
            if section not in config:
                raise ConfigurationError(f"Missing required configuration section: {section}")
        
        # Validate dataset section
        dataset = config['dataset']
        required_dataset_fields = ['name', 'type', 'version']
        for field in required_dataset_fields:
            if field not in dataset:
                raise ConfigurationError(f"Missing required field in dataset section: {field}")
        
        # Validate source section
        source = config['source']
        if 'type' not in source:
            raise ConfigurationError("Missing 'type' in source configuration")
        
        # Validate transformations if present
        if 'transformations' in config:
            for i, transform in enumerate(config['transformations']):
                if 'type' not in transform:
                    raise ConfigurationError(f"Missing 'type' in transformation {i}")
        
        return True
    
    def merge_with_defaults(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge configuration with default values.
        
        Args:
            config: User configuration
            
        Returns:
            Merged configuration with defaults
        """
        merged = self.DEFAULT_CONFIG.copy()
        
        def deep_merge(default: Dict, override: Dict) -> Dict:
            """Recursively merge dictionaries."""
            result = default.copy()
            for key, value in override.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = deep_merge(result[key], value)
                else:
                    result[key] = value
            return result
        
        return deep_merge(merged, config)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.
        
        Args:
            key: Configuration key (e.g., 'processing.max_workers')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        if self.config is None:
            raise ConfigurationError("No configuration loaded")
        
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value