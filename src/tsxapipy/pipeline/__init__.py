# C:\work\tsxapi4py\src\tsxapipy\pipeline\__init__.py
"""
The tsxapipy.pipeline subpackage provides components for processing and managing
streaming data, such as candlestick aggregation and data stream orchestration.
"""
from .candle_aggregator import LiveCandleAggregator
from .data_manager import DataManager

__all__ = [
    "LiveCandleAggregator",
    "DataManager",
]