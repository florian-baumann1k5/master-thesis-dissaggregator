"""
configs

This folder contains:
- YAML configuration files for different energy carriers (e.g., gas_config.yaml, power_config.yaml).
- A loader utility (config_loader.py) for reading and optionally merging these configs.
"""

from .config_loader import load_config

__all__ = [
    "load_config",
]