# tsxapipy/historical/__init__.py
"""
The historical package provides tools for fetching, updating, and managing
historical market data, primarily using Parquet files for storage.
"""
from .updater import HistoricalDataUpdater, calculate_next_interval_start
from .parquet_handler import (
    get_last_timestamp_from_parquet,
    append_bars_to_parquet,
    ParquetHandlerError,
    ParquetReadError,
    ParquetWriteError
)
from .gap_detector import find_missing_trading_days

__all__ = [
    "HistoricalDataUpdater",
    "calculate_next_interval_start",  # Keep if used externally
    "get_last_timestamp_from_parquet",
    "append_bars_to_parquet",
    "ParquetHandlerError",
    "ParquetReadError",
    "ParquetWriteError",
    "find_missing_trading_days",
]