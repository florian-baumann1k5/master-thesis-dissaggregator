import os
import yaml


def load_config(config_filename: str = "base_config.yaml") -> dict:
    """
    Load a YAML configuration file from the configs folder and return it as a dictionary.

    :param config_filename: Name of the YAML file (e.g., "gas_config.yaml")
    :return: A dictionary with the configuration parameters
    """
    config_path = os.path.join(os.path.dirname(__file__), config_filename)
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    return config_data