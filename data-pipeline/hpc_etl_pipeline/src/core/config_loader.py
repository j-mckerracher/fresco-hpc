
import yaml
from typing import Dict

from hpc_etl_pipeline.src.core.exceptions import ConfigurationError

class ConfigLoader:
    """Manages loading and validation of YAML configurations."""

    def __init__(self, default_config_path: str = None):
        self.default_config = self._load_yaml(default_config_path) if default_config_path else {}

    def load_dataset_config(self, config_path: str) -> Dict:
        """
        Loads a dataset-specific YAML configuration file.

        Args:
            config_path: Path to the dataset configuration file.

        Returns:
            A dictionary containing the loaded configuration.
        """
        config = self._load_yaml(config_path)
        if self.default_config:
            config = self.merge_with_defaults(config)
        self.validate_config(config)
        return config

    def validate_config(self, config: Dict) -> bool:
        """
        Validates the structure and values of the configuration.

        Args:
            config: The configuration dictionary to validate.

        Returns:
            True if the configuration is valid.

        Raises:
            ConfigurationError: If the configuration is invalid.
        """
        required_keys = ["dataset", "source", "output", "processing", "validation"]
        for key in required_keys:
            if key not in config:
                raise ConfigurationError(f"Missing required configuration key: {key}")

        if "name" not in config["dataset"]:
            raise ConfigurationError("Missing 'name' in dataset configuration.")

        return True

    def merge_with_defaults(self, config: Dict) -> Dict:
        """
        Merges the dataset-specific configuration with the default configuration.

        Args:
            config: The dataset-specific configuration.

        Returns:
            The merged configuration.
        """
        merged_config = self.default_config.copy()
        for key, value in config.items():
            if key in merged_config and isinstance(merged_config[key], dict) and isinstance(value, dict):
                merged_config[key].update(value)
            else:
                merged_config[key] = value
        return merged_config

    def _load_yaml(self, path: str) -> Dict:
        """
        Loads a YAML file from the given path.

        Args:
            path: The path to the YAML file.

        Returns:
            A dictionary containing the loaded YAML content.
        """
        try:
            with open(path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise ConfigurationError(f"Configuration file not found: {path}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML file: {e}")
