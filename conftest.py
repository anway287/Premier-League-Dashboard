"""
Root conftest — adds project root to sys.path so `src` and `mocks` are importable,
and registers the MetricsCollector plugin for the whole session.
"""
import sys
from pathlib import Path

# Make src/ and mocks/ importable without installing the package
sys.path.insert(0, str(Path(__file__).parent))

from metrics.collector import MetricsCollector


def pytest_configure(config):
    config.pluginmanager.register(MetricsCollector(), "metrics-collector")
