# tsxapipy/pipeline/candle_aggregator.py

import logging
import threading
from datetime import datetime, timezone, timedelta # timedelta is used by _calculate_candle_start_time indirectly via tzinfo
from typing import Dict, Any, Callable, Optional

import pandas as pd
# import numpy as np # Not strictly needed in this version

# from tsxapipy.common.time_utils import UTC_TZ # timezone.utc is sufficient for this module

logger = logging.getLogger(__name__) # Module-level logger

class LiveCandleAggregator:
    """
    Aggregates trade data into candles of a specified timeframe.
    Emits updates for both forming and finalized candles.
    """
    
    def __init__(self, contract_id: str, timeframe_seconds: int, 
                 new_candle_data_callback: Optional[Callable[[pd.Series, bool, int], None]] = None):
        """
        Initialize the LiveCandleAggregator.
        
        Args:
            contract_id (str): The contract ID.
            timeframe_seconds (int): The timeframe in seconds for candle aggregation.
            new_candle_data_callback (Optional[Callable[[pd.Series, bool, int], None]]): 
                Callback function invoked with:
                - candle_data_series (pd.Series): The OHLCV data for the candle.
                - is_forming_candle (bool): True if the candle is still forming, False if finalized.
                - timeframe_seconds (int): The timeframe of the emitted candle.
        """
        # Use a more specific logger instance including the timeframe for easier debugging if multiple aggregators run
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}[{timeframe_seconds}s]")
        self.logger.info(f"LiveCandleAggregator initialized for contract '{contract_id}', timeframe: {timeframe_seconds}s")
        
        if not isinstance(timeframe_seconds, int) or timeframe_seconds <= 0:
            raise ValueError("timeframe_seconds must be a positive integer.")
        if new_candle_data_callback is not None and not callable(new_candle_data_callback):
            raise TypeError("new_candle_data_callback must be callable or None.")

        self.contract_id = contract_id
        self.timeframe_seconds = timeframe_seconds
        self.new_candle_data_callback = new_candle_data_callback
        
        self.current_candle: Dict[str, Any] = {
            'Time': None,   # datetime object, UTC
            'Open': None,   # float
            'High': None,   # float
            'Low': None,    # float
            'Close': None,  # float
            'Volume': 0.0   # float
        }
        
        self.lock = threading.RLock() # For thread-safe access to self.current_candle

    def add_trade(self, trade_data: Dict[str, Any]):
        """
        Adds a trade to the aggregator. Parses timestamp, price, and volume from trade_data.
        
        Args:
            trade_data (Dict[str, Any]): A dictionary representing the trade, expected to contain:
                'timestamp' (str or datetime): The trade timestamp. ISO format string (e.g., with 'Z') or datetime object. (Lowercase)
                'price' (float or int): The trade price. (Lowercase)
                'volume' (float or int, optional): The trade volume. Defaults to 0.0 if missing. (Lowercase)
        """
        # Use lowercase keys here to match the incoming data from DataStream
        log_price = trade_data.get('price')      # For logging
        log_volume = trade_data.get('volume')    # For logging
        log_timestamp = trade_data.get('timestamp') # For logging

        self.logger.info( 
            f"Aggregator ({self.timeframe_seconds}s) add_trade CALLED. Price: {log_price}, Vol: {log_volume}, TS: {log_timestamp}"
        )
        
        # --- Timestamp Parsing ---
        timestamp_input = trade_data.get('timestamp') # ACTUAL PARSING KEY (lowercase)
        if timestamp_input is None: 
            self.logger.warning(f"Trade data missing 'timestamp'. Trade ignored. Data: {trade_data}")
            return
        
        timestamp: datetime # Type hint for clarity
        try:
            if isinstance(timestamp_input, str):
                # Handle 'Z' for UTC and ensure it's parsed as timezone-aware
                processed_timestamp_input = timestamp_input
                if processed_timestamp_input.endswith('Z'):
                    processed_timestamp_input = processed_timestamp_input[:-1] + '+00:00'
                timestamp = datetime.fromisoformat(processed_timestamp_input)
            elif isinstance(timestamp_input, datetime):
                timestamp = timestamp_input
            else:
                # Attempt to convert to string if it's some other type that might be parseable
                self.logger.debug(f"Timestamp input type {type(timestamp_input)} is not str or datetime. Attempting str conversion.")
                timestamp_str_converted = str(timestamp_input)
                if timestamp_str_converted.endswith('Z'):
                    timestamp_str_converted = timestamp_str_converted[:-1] + '+00:00'
                timestamp = datetime.fromisoformat(timestamp_str_converted)

            # Ensure timestamp is UTC aware and explicitly set to UTC
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc) # Assume UTC if naive
            elif timestamp.tzinfo.utcoffset(timestamp) != timedelta(0): # If tz-aware but not UTC
                timestamp = timestamp.astimezone(timezone.utc)
        
        except Exception as e:
            self.logger.error(f"Error parsing timestamp '{timestamp_input}': {e}. Trade data: {trade_data}. Trade ignored.", exc_info=True)
            return
        
        # --- Price and Volume Parsing ---
        price_input = trade_data.get('price')     # ACTUAL PARSING KEY (lowercase)
        volume_input = trade_data.get('volume', 0.0) # ACTUAL PARSING KEY (lowercase), Default volume to 0.0 if missing

        if price_input is None:
            self.logger.warning(f"Trade data missing 'price'. Trade ignored. Data: {trade_data}")
            return
        
        try:
            price = float(price_input)
            size = float(volume_input) # API might send volume as string sometimes
            if size < 0:
                self.logger.warning(f"Received negative trade volume ({size}) for price {price}. Using 0.0 volume.")
                size = 0.0
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error converting price/volume to float. Price: '{price_input}', Volume: '{volume_input}'. Error: {e}. Trade ignored.", exc_info=True)
            return
        
        # --- Candle Update Logic (Thread-Safe) ---
        with self.lock: # Ensure thread safety for current_candle updates
            self._update_candle(timestamp, price, size)
            
    def _update_candle(self, timestamp: datetime, price: float, size: float):
        """
        Updates the current candle with new trade data.
        If a new candle period starts, the old candle is finalized and emitted.
        The forming candle is also emitted after each update.

        Args:
            timestamp (datetime): UTC datetime of the trade.
            price (float): Trade price.
            size (float): Trade volume.
        """
        candle_start_time = self._calculate_candle_start_time(timestamp)
        
        if self.current_candle.get('Time') is None or candle_start_time != self.current_candle['Time']:
            # A. New candle period has begun OR this is the very first trade.

            # A.1. Finalize and emit the *previous* candle if it existed and had data.
            if self.current_candle.get('Time') is not None and self.current_candle.get('Open') is not None:
                self.logger.debug(f"Finalizing previous candle at {self.current_candle['Time'].isoformat()}.")
                self._emit_candle_data(is_forming_candle=False) # Previous candle is now finalized
            
            # A.2. Start a new candle with the current trade's data.
            self.current_candle = {
                'Time': candle_start_time,
                'Open': price,
                'High': price,
                'Low': price,
                'Close': price,
                'Volume': size
            }
            self.logger.debug(f"New candle started. Time: {candle_start_time.isoformat()}, O: {price:.2f}, H: {price:.2f}, L: {price:.2f}, C: {price:.2f}, V: {size:.2f}")
        
        else:
            # B. This trade belongs to the existing `current_candle` (which is forming).
            # Ensure Open is set if it was somehow missed (should be set on candle start)
            if self.current_candle.get('Open') is None:
                self.current_candle['Open'] = price
                self.logger.warning(f"Current candle Open was None, setting to current price: {price}")

            self.current_candle['High'] = max(self.current_candle.get('High', price), price) # Handles if High was None
            self.current_candle['Low'] = min(self.current_candle.get('Low', price), price)   # Handles if Low was None
            self.current_candle['Close'] = price
            self.current_candle['Volume'] = self.current_candle.get('Volume', 0.0) + size
            self.logger.debug(
                f"Forming candle updated. Time: {self.current_candle['Time'].isoformat()}, "
                f"H: {self.current_candle['High']:.2f}, L: {self.current_candle['Low']:.2f}, "
                f"C: {self.current_candle['Close']:.2f}, V: {self.current_candle['Volume']:.2f}"
            )

        # C. Emit data for the current candle (which is forming, or was just started and is thus forming).
        # Ensure the candle has valid data before emitting.
        if self.current_candle.get('Time') is not None and self.current_candle.get('Open') is not None:
            self._emit_candle_data(is_forming_candle=True) 
        else:
            self.logger.warning(f"Attempted to emit candle data, but current_candle Time or Open is None. Current candle: {self.current_candle}")


    def _calculate_candle_start_time(self, timestamp: datetime) -> datetime:
        """
        Calculates the floored start time of the candle for a given UTC timestamp.

        Args:
            timestamp (datetime): The UTC timestamp of a trade.

        Returns:
            datetime: The UTC start time of the candle period this trade belongs to.
        """
        # Timestamp is assumed to be UTC-aware from add_trade
        if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) != timedelta(0):
            self.logger.warning(f"Timestamp '{timestamp}' was not UTC in _calculate_candle_start_time. Converting. This is unexpected.")
            timestamp = timestamp.astimezone(timezone.utc)
            
        # Calculate total seconds since the Unix epoch for the given timestamp
        epoch_seconds = int(timestamp.timestamp())
        
        # Floor to the nearest interval boundary
        candle_start_epoch_seconds = epoch_seconds - (epoch_seconds % self.timeframe_seconds)
        
        # Convert back to a datetime object, ensuring it's UTC
        candle_start_time_utc = datetime.fromtimestamp(candle_start_epoch_seconds, tz=timezone.utc)
        
        return candle_start_time_utc

    def _emit_candle_data(self, is_forming_candle: bool):
        """
        Emits the current candle data via the callback.
        Also logs the emitted data for debugging purposes.

        Args:
            is_forming_candle (bool): True if the candle being emitted is still forming,
                                      False if it's considered finalized.
        """
        if self.new_candle_data_callback and self.current_candle.get('Time') is not None:
            candle_to_emit = self.current_candle.copy() 

            for key in ['Open', 'High', 'Low', 'Close']:
                if candle_to_emit.get(key) is None:
                    self.logger.error(f"CRITICAL: Candle field '{key}' is None before emission. Candle: {candle_to_emit}")
                    return 
            
            try:
                candle_series_to_emit = pd.Series(candle_to_emit)
                
                # Logging at INFO level for crucial emission events.
                self.logger.info(
                    f"Aggregator ({self.timeframe_seconds}s): Emitting candle -> Time: {candle_series_to_emit['Time'].isoformat()}, "
                    f"O: {candle_series_to_emit['Open']:.2f}, H: {candle_series_to_emit['High']:.2f}, "
                    f"L: {candle_series_to_emit['Low']:.2f}, C: {candle_series_to_emit['Close']:.2f}, "
                    f"V: {candle_series_to_emit['Volume']:.2f}, IsForming: {is_forming_candle}"
                )
                
                self.new_candle_data_callback(candle_series_to_emit, is_forming_candle, self.timeframe_seconds)
            except Exception as e:
                self.logger.error(f"Error in new_candle_data_callback: {e}", exc_info=True)
        elif not self.new_candle_data_callback:
            self.logger.warning("new_candle_data_callback is None. Cannot emit candle data.")
        elif self.current_candle.get('Time') is None:
            self.logger.warning("Cannot emit candle data: current_candle['Time'] is None.")