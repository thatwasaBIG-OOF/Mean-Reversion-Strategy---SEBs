# tsxapipy/pipeline/candle_aggregator.py
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Callable
import pandas as pd

logger = logging.getLogger(__name__)

class LiveCandleAggregator:
    def __init__(self,
                 contract_id: str,
                 timeframe_seconds: int,
                 new_candle_data_callback: Callable[[pd.Series, bool], None]): # MODIFIED: Type hint for callback
        self.contract_id: str = contract_id
        self.timeframe_seconds: int = timeframe_seconds
        self.new_candle_data_callback: Callable[[pd.Series, bool], None] = new_candle_data_callback # MODIFIED
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info(
            f"LiveCandleAggregator initialized for {self.contract_id}, "
            f"timeframe: {self.timeframe_seconds}s"
        )
        self.current_candle_open_time: Optional[datetime] = None
        # self.current_ohlcv: Optional[Dict[str, Any]] = None # Will now become a Series when passed
        self._current_ohlcv_dict: Optional[Dict[str, Any]] = None # Internal storage as dict

    def _get_current_ohlcv_series(self) -> Optional[pd.Series]:
        if self._current_ohlcv_dict:
            # Ensure correct dtypes when creating the Series if necessary,
            # though DataManager will also handle schema.
            # 'Time' is already a datetime object here. OHLCV are floats/ints.
            return pd.Series(self._current_ohlcv_dict)
        return None

    def add_trade(self, trade_data: Dict[str, Any]):
        try:
            price_str = trade_data.get('price')
            size_str = trade_data.get('volume')
            timestamp_str = trade_data.get('timestamp')

            if price_str is None: price_str = trade_data.get('p')
            if size_str is None:
                size_str = trade_data.get('size')
                if size_str is None: size_str = trade_data.get('s')
            if timestamp_str is None: timestamp_str = trade_data.get('t')

            if price_str is None or size_str is None or timestamp_str is None:
                self.logger.warning(f"Incomplete trade data: {trade_data}")
                return

            price = float(price_str)
            size = int(size_str)
            
            try:
                ts_obj = pd.Timestamp(timestamp_str)
                if ts_obj.tzinfo is None:
                    trade_datetime_utc = ts_obj.tz_localize('UTC').to_pydatetime()
                else:
                    trade_datetime_utc = ts_obj.tz_convert('UTC').to_pydatetime()
            except Exception as e_ts:
                self.logger.error(f"Error parsing trade timestamp '{timestamp_str}': {e_ts}. Trade: {trade_data}")
                return

            trade_unix_seconds = int(trade_datetime_utc.timestamp())
            candle_start_unix_seconds = (trade_unix_seconds // self.timeframe_seconds) * self.timeframe_seconds
            candle_start_time_utc = datetime.fromtimestamp(candle_start_unix_seconds, tz=timezone.utc)

            self.logger.debug(
                 f"AGG: Trade P={price}, V={size}, TS={trade_datetime_utc.isoformat()} -> CandleStart={candle_start_time_utc.isoformat()}"
            )

            if self.current_candle_open_time is None or candle_start_time_utc > self.current_candle_open_time:
                if self._current_ohlcv_dict is not None:
                    current_series = self._get_current_ohlcv_series()
                    if current_series is not None:
                        self.logger.debug(f"AGG: Completing OLD candle ({self.current_candle_open_time.isoformat()}): {self._current_ohlcv_dict}")
                        self.new_candle_data_callback(current_series, False) 

                self._current_ohlcv_dict = { # Store as dict internally
                    'Time': candle_start_time_utc, 
                    'Open': price, 'High': price, 'Low': price, 'Close': price, 'Volume': float(size) # Ensure Volume is float like others for Series
                }
                self.current_candle_open_time = candle_start_time_utc
                current_series = self._get_current_ohlcv_series()
                if current_series is not None:
                    self.logger.debug(f"AGG: Starting NEW candle ({candle_start_time_utc.isoformat()}): {self._current_ohlcv_dict}")
                    self.new_candle_data_callback(current_series, True)

            elif candle_start_time_utc == self.current_candle_open_time:
                if self._current_ohlcv_dict is None: 
                    self.logger.error("AGG: Inconsistent state. Re-initializing candle.")
                    self._current_ohlcv_dict = {
                        'Time': candle_start_time_utc, 'Open': price, 'High': price,
                        'Low': price, 'Close': price, 'Volume': float(size)
                    }
                    current_series = self._get_current_ohlcv_series()
                    if current_series is not None:
                        self.new_candle_data_callback(current_series, True)
                else:
                    old_high = self._current_ohlcv_dict['High']
                    old_low = self._current_ohlcv_dict['Low']
                    
                    self._current_ohlcv_dict['High'] = max(self._current_ohlcv_dict['High'], price)
                    self._current_ohlcv_dict['Low'] = min(self._current_ohlcv_dict['Low'], price)
                    self._current_ohlcv_dict['Close'] = price
                    self._current_ohlcv_dict['Volume'] += float(size)
                    
                    self.logger.debug(
                        f"AGG: Updating CURRENT candle ({candle_start_time_utc.isoformat()}): "
                        f"Trade P={price}. Prev H={old_high}, Prev L={old_low}. "
                        f"New H={self._current_ohlcv_dict['High']}, New L={self._current_ohlcv_dict['Low']}, "
                        f"New C={self._current_ohlcv_dict['Close']}, New V={self._current_ohlcv_dict['Volume']}"
                    )
                    current_series = self._get_current_ohlcv_series()
                    if current_series is not None:
                        self.new_candle_data_callback(current_series, True)
            else:
                self.logger.warning(
                    f"AGG: Out-of-order trade. Trade: {trade_datetime_utc.isoformat()}, "
                    f"Current Candle: {self.current_candle_open_time.isoformat() if self.current_candle_open_time else 'None'}. Skipping."
                )
        except Exception as e:
            self.logger.error(f"AGG: Unexpected error in add_trade with data '{trade_data}': {e}", exc_info=True)

    def get_current_forming_candle_dict(self) -> Optional[Dict[str, Any]]: # Renamed for clarity
        if self._current_ohlcv_dict:
            return self._current_ohlcv_dict.copy()
        return None