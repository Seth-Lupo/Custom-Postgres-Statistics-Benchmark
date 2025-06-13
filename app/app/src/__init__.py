import os
import importlib
from pathlib import Path

# Base classes
from .base import StatsSource, StatsSourceConfig

__all__ = ['StatsSource', 'StatsSourceConfig']

# Dynamically import all subpackages to make StatsSource subclasses available
src_dir = Path(__file__).parent
for module_file in src_dir.iterdir():
    if module_file.is_dir() and (module_file / '__init__.py').exists():
        module_name = module_file.name
        if not module_name.startswith('__'):
            try:
                importlib.import_module(f".{module_name}", __package__)
            except ImportError:
                pass # ignore errors 