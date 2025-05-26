"""
Candle aggregator module for tsxapipy.

This module provides the LiveCandleAggregator class, which aggregates trade data into candles.
"""

import logging
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Callable, Optional

import pandas as pd
import numpy as np

from tsxapipy.common.time_utils import UTC_TZ

logger = logging.getLogger(__name__)

class LiveCandleAggregator:
    """
    Aggregates trade data into candles of a specified timeframe.
    
    This class receives trade data and aggregates it into OHLCV candles.
    """
    
    def __init__(self, contract_id: str, timeframe_seconds: int, new_candle_data_callback: Optional[Callable] = None):
        """
        Initialize the LiveCandleAggregator.
        
        Args:
            contract_id: The contract ID
            timeframe_seconds: The timeframe in seconds
            new_candle_data_callback: Callback function for when new candle data is available
        """
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"LiveCandleAggregator initialized for {contract_id}, timeframe: {timeframe_seconds}s")
        
        self.contract_id = contract_id
        self.timeframe_seconds = timeframe_seconds
        self.new_candle_data_callback = new_candle_data_callback
        
        # Initialize current candle
        self.current_candle = {
            'Time': None,
            'Open': None,
            'High': None,
            'Low': None,
            'Close': None,
            'Volume': 0
        }
        
        # Add lock for thread safety
        self.lock = threading.RLock()
        
        # Add flag for tracking if candle is forming
        self.is_forming_candle = False
    
    def add_trade(self, trade_data: Dict[str, Any]):
        """
        Add a trade to the aggregator.
        
        Args:
            trade_data: The trade data
        """
        # Extract timestamp from trade data
        timestamp_str = trade_data.get('Timestamp')
        if not timestamp_str:
            self.logger.warning("Trade data missing timestamp")
            return
        
        try:
            # Parse timestamp
            if isinstance(timestamp_str, str):
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            elif isinstance(timestamp_str, datetime):
                timestamp = timestamp_str
            else:
                self.logger.warning(f"Unexpected timestamp format: {type(timestamp_str)}")
                return
        except Exception as e:
            self.logger.error(f"Error parsing timestamp: {e}")
            return
        
        # Extract price and size from trade data
        price = trade_data.get('Price')
        size = trade_data.get('Volume', 0)
        
        if price is None:
            self.logger.warning("Trade data missing price")
            return
        
        # Update current candle with thread safety
        with self.lock:
            self._update_candle(timestamp, price, size)
    
    def process_trade(self, trade_data: Dict[str, Any]):
        """
        Process a trade from the data stream.
        
        Args:
            trade_data: The trade data
        """
        self.add_trade(trade_data)
    
    def _update_candle(self, timestamp: datetime, price: float, size: float = 0):
        """
        Update the current candle with new data.
        
        Args:
            timestamp: The timestamp of the data
            price: The price
            size: The size (volume)
        """
        # Calculate candle start time
        candle_start_time = self._calculate_candle_start_time(timestamp)
        
        # If this is a new candle
        if self.current_candle['Time'] is None or candle_start_time != self.current_candle['Time']:
            # If we have a previous candle, finalize it and notify callback
            if self.current_candle['Time'] is not None:
                self._finalize_current_candle()
            
            # Start a new candle
            self.current_candle = {
                'Time': candle_start_time,
                'Open': price,
                'High': price,
                'Low': price,
                'Close': price,
                'Volume': size
            }
            self.is_forming_candle = True
        else:
            # Update existing candle
            self.current_candle['High'] = max(self.current_candle['High'], price)
            self.current_candle['Low'] = min(self.current_candle['Low'], price)
            self.current_candle['Close'] = price
            self.current_candle['Volume'] += size
    
    def _calculate_candle_start_time(self, timestamp: datetime) -> datetime:
        """
        Calculate the start time of the candle containing the given timestamp.
        
        Args:
            timestamp: The timestamp
        
        Returns:
            datetime: The candle start time
        """
        # Ensure timestamp is timezone-aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        # Calculate seconds since epoch
        epoch_seconds = int(timestamp.timestamp())
        
        # Calculate candle start time
        candle_start_seconds = epoch_seconds - (epoch_seconds % self.timeframe_seconds)
        candle_start_time = datetime.fromtimestamp(candle_start_seconds, tz=timezone.utc)
        
        return candle_start_time
    
    def _finalize_current_candle(self):
        """Finalize the current candle and notify callback."""
        if self.new_candle_data_callback:
            try:
                # Convert candle to pandas Series for consistency
                candle_series = pd.Series(self.current_candle)
                
                # Call the callback with the candle data and forming flag
                self.new_candle_data_callback(candle_series)
                
                # Reset forming flag
                self.is_forming_candle = False
            except Exception as e:
                self.logger.error(f"Error in new candle data callback: {e}")
