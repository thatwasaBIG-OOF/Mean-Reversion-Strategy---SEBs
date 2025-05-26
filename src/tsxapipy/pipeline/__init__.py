# tsxapipy/pipeline/__init__.py
"""
Pipeline module for tsxapipy.

This module provides components for managing data flow from API to application,
including real-time data streaming and candle aggregation.
"""

from .data_manager import DataManager
from .candle_aggregator import LiveCandleAggregator
from tsxapipy.api import schemas as api_schemas  

__all__ = [
    'DataManager', 
    'LiveCandleAggregator',
    'api_schemas' # Add api_schemas to __all__ if you intend for users to do:
                  # from tsxapipy.pipeline import api_schemas
                  # Otherwise, it's just imported for use within the pipeline package
                  # and doesn't need to be in __all__.
                  # Given its location in tsxapipy.api, it's usually better to import from there directly.
]