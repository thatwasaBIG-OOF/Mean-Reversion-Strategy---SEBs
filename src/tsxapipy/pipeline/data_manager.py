# tsxapipy/pipeline/data_manager.py
import logging
import threading
from typing import Optional, Dict, Any, Tuple, List
from datetime import datetime, timedelta, timezone as dt_timezone

import pandas as pd
import numpy as np

from tsxapipy.auth import authenticate, AuthenticationError
from tsxapipy.api import APIClient, APIError, MAX_BARS_PER_REQUEST
from tsxapipy.real_time import DataStream
from tsxapipy.config import TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES
from tsxapipy.api.exceptions import ConfigurationError, APIResponseParsingError # Added APIResponseParsingError
from tsxapipy.pipeline.candle_aggregator import LiveCandleAggregator
from tsxapipy.api import schemas # Import Pydantic schemas


logger = logging.getLogger(__name__)

class DataManager:
    MAX_CANDLES = 500
    EMA_PERIOD = 9
    SMA_PERIOD = 20
    SUPPORTED_TIMEFRAMES_SECONDS: List[int] = [300, 900, 3600]

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.api_client: Optional[APIClient] = None
        self.data_stream: Optional[DataStream] = None
        self.candle_aggregators: Dict[int, LiveCandleAggregator] = {}
        self.all_candles_dfs: Dict[int, pd.DataFrame] = {}
        
        self.candle_dtypes = {
            'Time': 'datetime64[ns, UTC]', 'Open': 'float64', 'High': 'float64',
            'Low': 'float64', 'Close': 'float64',
            f'EMA{self.EMA_PERIOD}': 'float64', f'SMA{self.SMA_PERIOD}': 'float64'
        }
        for tf_sec in self.SUPPORTED_TIMEFRAMES_SECONDS:
            self.all_candles_dfs[tf_sec] = self._create_empty_candles_df()

        self.df_lock = threading.RLock()
        self.current_contract_id: Optional[str] = None
        self.is_streaming: bool = False
        self.last_stream_status: str = "uninitialized"

    def _create_empty_candles_df(self) -> pd.DataFrame:
        """Creates an empty DataFrame with the correct schema for storing candle data."""
        df = pd.DataFrame(columns=list(self.candle_dtypes.keys()))
        for col, dtype_str in self.candle_dtypes.items():
            if 'datetime' in dtype_str:
                # Ensure 'Time' column is specifically datetime64[ns, UTC]
                df[col] = pd.Series(dtype='datetime64[ns, UTC]')
            elif 'float' in dtype_str:
                df[col] = pd.Series(dtype='float64') # Ensure float columns start as float64
            else: # Should not happen with current dtypes, but for robustness
                df[col] = df[col].astype(dtype_str)
        return df
    
    def _ensure_df_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensures the DataFrame adheres to the predefined candle schema and dtypes."""
        # Create a new DataFrame to avoid modifying the input df if it's a view
        res_df = pd.DataFrame(index=df.index)
        for col, dtype_str in self.candle_dtypes.items():
            if col in df.columns:
                res_df[col] = df[col]
            else: # Column is missing, add it with appropriate NA type
                if 'datetime' in dtype_str:
                    res_df[col] = pd.NaT
                elif 'float' in dtype_str:
                    res_df[col] = float('nan')
                else: # Should not happen with current schema
                    res_df[col] = None # Or appropriate NA for other types
            
            # Attempt to convert to target dtype
            try:
                if col == 'Time':
                    res_df[col] = pd.to_datetime(res_df[col], errors='coerce')
                    if pd.api.types.is_datetime64_any_dtype(res_df[col]):
                        if res_df[col].dt.tz is None:
                            res_df[col] = res_df[col].dt.tz_localize('UTC')
                        else:
                            res_df[col] = res_df[col].dt.tz_convert('UTC')
                    else: # If to_datetime resulted in non-datetime (e.g., object of NaTs)
                        res_df[col] = pd.Series(res_df[col], dtype='datetime64[ns, UTC]')
                elif 'float' in dtype_str: 
                    res_df[col] = pd.to_numeric(res_df[col], errors='coerce').astype('float64')
                else: # Not expected for current schema
                    res_df[col] = res_df[col].astype(dtype_str, errors='ignore')
            except Exception as e_schema:
                self.logger.error(f"Error ensuring schema for column '{col}' to '{dtype_str}': {e_schema}. "
                                  f"Data head: {res_df[col].head() if col in res_df and not res_df.empty else 'N/A'}")
                # Fallback to NA series of the correct type if conversion fails
                if 'datetime' in dtype_str:
                    res_df[col] = pd.Series(pd.NaT, index=res_df.index, dtype='datetime64[ns, UTC]')
                elif 'float' in dtype_str:
                    res_df[col] = pd.Series(np.nan, index=res_df.index, dtype='float64')
                else:
                    res_df[col] = pd.Series(None, index=res_df.index, dtype='object') # Fallback for other types
        
        # Ensure columns are in the correct order
        return res_df[list(self.candle_dtypes.keys())]


    def _calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates EMA and SMA indicators on the DataFrame."""
        # Ensure df is a copy to avoid SettingWithCopyWarning if it's a slice
        df_copy = df.copy()
        ema_col_name = f'EMA{self.EMA_PERIOD}'
        sma_col_name = f'SMA{self.SMA_PERIOD}'

        # Ensure Close column is numeric and handle all-NaN case
        if 'Close' not in df_copy.columns or df_copy['Close'].isnull().all():
            self.logger.debug("DataManager CalcInd: 'Close' column missing or all NaN. Indicators will be NaN.")
            df_copy[ema_col_name] = np.nan
            df_copy[sma_col_name] = np.nan
            return df_copy
        
        # Ensure 'Close' is float64 for calculations
        df_copy['Close'] = pd.to_numeric(df_copy['Close'], errors='coerce').astype('float64')

        # EMA Calculation
        # ewm().mean() correctly handles initial NaNs if min_periods is met later in the series.
        df_copy[ema_col_name] = df_copy['Close'].ewm(span=self.EMA_PERIOD, adjust=False, min_periods=self.EMA_PERIOD).mean()
        self.logger.debug(f"DataManager CalcInd: Calculated EMA{self.EMA_PERIOD}. Non-NaN count: {df_copy[ema_col_name].notna().sum()}, Tail: {df_copy[ema_col_name].dropna().tail(3).to_list() if df_copy[ema_col_name].notna().any() else 'All NaN'}")
        
        # SMA Calculation
        # rolling().mean() also handles initial NaNs correctly due to min_periods.
        df_copy[sma_col_name] = df_copy['Close'].rolling(window=self.SMA_PERIOD, min_periods=self.SMA_PERIOD).mean()
        self.logger.debug(f"DataManager CalcInd: Calculated SMA{self.SMA_PERIOD}. Non-NaN count: {df_copy[sma_col_name].notna().sum()}, Tail: {df_copy[sma_col_name].dropna().tail(3).to_list() if df_copy[sma_col_name].notna().any() else 'All NaN'}")

        return df_copy

    def initialize_components(self, contract_id: str) -> bool:
        """Initializes API client, data stream, and candle aggregators for all supported timeframes."""
        self.logger.info(f"Initializing components for Contract: {contract_id} across all supported timeframes.")
        self.current_contract_id = contract_id
        try:
            if self.data_stream and self.is_streaming:
                self.logger.info("Previously streaming. Stopping existing stream before re-initializing.")
                self.stop_streaming() # Ensure clean stop
            
            self.logger.debug("Authenticating for DataManager...")
            initial_token, token_acquired_at = authenticate()
            self.api_client = APIClient(initial_token=initial_token, token_acquired_at=token_acquired_at)
            self.logger.info("APIClient initialized in DataManager.")

            self.candle_aggregators.clear()
            with self.df_lock:
                self.all_candles_dfs.clear() # Clear existing DataFrames
                for tf_sec in self.SUPPORTED_TIMEFRAMES_SECONDS:
                    # Important: Initialize with empty, schema-correct DataFrame
                    self.all_candles_dfs[tf_sec] = self._create_empty_candles_df()
                    
                    # Create a unique callback for each aggregator instance
                    def create_callback(timeframe): # Closure to capture timeframe
                        return lambda candle_data_series, is_forming: self._handle_new_candle_data_from_aggregator(candle_data_series, is_forming, timeframe)
                    
                    self.candle_aggregators[tf_sec] = LiveCandleAggregator(
                        contract_id=contract_id, timeframe_seconds=tf_sec,
                        new_candle_data_callback=create_callback(tf_sec)
                    )
                    self.logger.info(f"LiveCandleAggregator for {tf_sec}s initialized.")

            self.data_stream = DataStream(
                api_client=self.api_client, contract_id_to_subscribe=contract_id,
                on_trade_callback=self._pass_trade_to_aggregators,
                on_state_change_callback=self._handle_stream_state_change,
                on_error_callback=self._handle_stream_error
            )
            self.logger.info("DataStream configured.")
            self.last_stream_status = "initialized"
            return True
        except (AuthenticationError, ConfigurationError, APIError, ValueError) as e:
            self.logger.error(f"Failed to initialize components: {e}", exc_info=False) # exc_info=False for brevity on expected errors
            self.api_client = None; self.data_stream = None; self.candle_aggregators.clear()
            self.last_stream_status = f"init_error: {type(e).__name__}"
            return False
        except Exception as e_init: # Catch-all for truly unexpected issues
            self.logger.error(f"Unexpected error during component initialization: {e_init}", exc_info=True)
            self.last_stream_status = f"unexpected_init_error: {type(e_init).__name__}"
            return False
        
    def _pass_trade_to_aggregators(self, trade_data: Dict[str, Any]):
        """Passes a single trade from DataStream to all registered candle aggregators."""
        for tf_sec, aggregator in self.candle_aggregators.items():
            try:
                aggregator.add_trade(trade_data)
            except Exception as e: # pylint: disable=broad-except
                self.logger.error(f"Error passing trade to aggregator for {tf_sec}s: {e}", exc_info=True)
        
    def _handle_stream_state_change(self, state: str): # state is str name of StreamConnectionState Enum
        """Handles state changes from the DataStream."""
        self.logger.info(f"DataStream state for '{self.current_contract_id}' changed to: {state}")
        self.last_stream_status = state 
        # Compare with string representation of Enum states
        if state.upper() == "CONNECTED": 
            self.is_streaming = True
        # Check against all non-connected states that imply not actively streaming usable data
        elif state.upper() in ["DISCONNECTED", "ERROR", "RECONNECTING_TOKEN", "RECONNECTING_UNEXPECTED", "STOPPING"]:
            self.is_streaming = False

    def _handle_stream_error(self, error: Any):
        """Handles errors reported by the DataStream."""
        self.logger.error(f"DataStream error for '{self.current_contract_id}': {error}")
        self.is_streaming = False 
        self.last_stream_status = f"stream_error: {str(error)[:50]}" 

    def start_streaming(self) -> bool:
        """Starts the data streaming process if components are initialized."""
        if not self.data_stream or not self.candle_aggregators or not self.api_client:
            self.logger.error("Cannot start streaming, components not fully initialized.")
            self.last_stream_status = "start_fail_not_init"; return False
        
        # Compare with Enum state from DataStream
        if self.is_streaming and self.data_stream.connection_status == schemas.StreamConnectionState.CONNECTED:
            self.logger.warning("Stream is already CONNECTED. Start call ignored."); return True
        
        self.logger.info(f"Attempting to start DataStream for {self.current_contract_id}...")
        if self.data_stream.start():
            self.logger.info("DataStream start initiated successfully."); return True 
        else:
            self.logger.error("Failed to initiate DataStream start (DataStream.start() returned False)."); self.is_streaming = False
            self.last_stream_status = "start_fail_api"; return False

    def stop_streaming(self):
        """Stops the data streaming process."""
        if self.data_stream:
            self.logger.info(f"Stopping DataStream for {self.current_contract_id}...")
            self.data_stream.stop() 
        else:
            self.logger.info("No active DataStream to stop.")
        self.is_streaming = False 
        self.last_stream_status = "stopped_by_user"
        
    def _handle_new_candle_data_from_aggregator(self, candle_data_series: pd.Series, _is_update_forming_candle: bool, timeframe_sec: int):
        """
        Handles new or updated candle data received from a LiveCandleAggregator.
        Updates the corresponding DataFrame in `all_candles_dfs`, calculates indicators,
        and ensures schema. This method is now modified to address the Pandas FutureWarning.
        """
        if not isinstance(candle_data_series, pd.Series) or candle_data_series.empty:
            self.logger.warning(f"DM ({timeframe_sec}s): Received invalid or empty candle_data_series (type: {type(candle_data_series)}). Skipping.")
            return

        required_keys = ['Time', 'Open', 'High', 'Low', 'Close'] # Volume is intentionally dropped
        for key in required_keys:
            if key not in candle_data_series.index:
                self.logger.warning(f"DM ({timeframe_sec}s): Candle data series missing required key '{key}'. Data: {candle_data_series}. Skipping.")
                return
        
        try:
            candle_time_utc = pd.Timestamp(candle_data_series['Time'])
            if pd.isna(candle_time_utc):
                raise ValueError("Timestamp is NaT after conversion from Series")
            if candle_time_utc.tzinfo is None:
                candle_time_utc = candle_time_utc.tz_localize('UTC')
            elif str(candle_time_utc.tzinfo) != 'UTC': # Ensure it's strictly UTC
                candle_time_utc = candle_time_utc.tz_convert('UTC')
        except Exception as e_ts:
            self.logger.warning(f"DM ({timeframe_sec}s): Invalid 'Time' in candle data series: {candle_data_series.get('Time')}. Error: {e_ts}. Skipping.")
            return

        # Validate and prepare OHLC values
        ohlc_values = {}
        for key in ['Open', 'High', 'Low', 'Close']:
            val = candle_data_series.get(key)
            if val is None: # Should not happen if required_keys check passed, but defensive
                self.logger.warning(f"DM ({timeframe_sec}s): Candle data series has None for '{key}'. Data: {candle_data_series}. Skipping.")
                return
            try:
                ohlc_values[key] = float(val)
                if ohlc_values[key] < 0: # Basic sanity check
                    self.logger.warning(f"DM ({timeframe_sec}s): Candle data series has negative value for '{key}': {ohlc_values[key]}. Data: {candle_data_series}. Using absolute value.")
                    ohlc_values[key] = abs(ohlc_values[key])
            except (ValueError, TypeError):
                self.logger.warning(f"DM ({timeframe_sec}s): Cannot convert '{key}' value '{val}' to float from series. Data: {candle_data_series}. Skipping.")
                return
        
        # Sanity check: High >= Low
        if ohlc_values['High'] < ohlc_values['Low']:
            self.logger.warning(f"DM ({timeframe_sec}s): High ({ohlc_values['High']}) < Low ({ohlc_values['Low']}) in candle data series. Data: {candle_data_series}. Swapping H/L.")
            ohlc_values['High'], ohlc_values['Low'] = ohlc_values['Low'], ohlc_values['High']

        self.logger.debug(f"DM ({timeframe_sec}s) <-- Agg: Rcvd VALID Series. IsFormingFlag(unused): {_is_update_forming_candle}, CandleTime: {candle_time_utc.isoformat()}")
        
        with self.df_lock:
            current_df_for_tf = self.all_candles_dfs.get(timeframe_sec)
            if current_df_for_tf is None: # Should be initialized, but safeguard
                self.logger.error(f"DM ({timeframe_sec}s): CRITICAL - DataFrame for timeframe not found. Re-creating.")
                current_df_for_tf = self._create_empty_candles_df()
                self.all_candles_dfs[timeframe_sec] = current_df_for_tf
            
            # Prepare data for the new/updated row (excluding Volume)
            new_row_data_dict = {col: np.nan for col in self.candle_dtypes.keys()} # Initialize with NaNs for all expected cols
            new_row_data_dict['Time'] = candle_time_utc
            for key, val in ohlc_values.items(): # ohlc_values only contains O,H,L,C
                new_row_data_dict[key] = val
            
            # This will become the DataFrame to work with for this update cycle
            df_for_processing: pd.DataFrame

            is_new_candle_period = current_df_for_tf.empty or candle_time_utc > current_df_for_tf['Time'].iloc[-1]
            is_update_to_last_candle = not current_df_for_tf.empty and candle_time_utc == current_df_for_tf['Time'].iloc[-1]

            if is_new_candle_period:
                # Convert the new row dict to a single-row DataFrame with correct schema
                new_row_df = pd.DataFrame([new_row_data_dict])
                new_row_df = self._ensure_df_schema(new_row_df) # Ensure dtypes match before concat

                if current_df_for_tf.empty:
                    df_for_processing = new_row_df # Assign directly if main DF is empty
                else:
                    # Concatenate new row. Both DFs should have matching schemas now.
                    df_for_processing = pd.concat([current_df_for_tf, new_row_df], ignore_index=True)
                self.logger.info(f"DM ({timeframe_sec}s): Appended new candle for time {candle_time_utc.isoformat()}.")

            elif is_update_to_last_candle:
                # Update the last row of the existing DataFrame
                last_idx = current_df_for_tf.index[-1]
                for key, val in new_row_data_dict.items():
                    if key in current_df_for_tf.columns: # Ensure column exists
                        try:
                            # Explicitly convert OHLC to float, Time to datetime64[ns, UTC] if needed
                            if key == 'Time':
                                current_df_for_tf.loc[last_idx, key] = pd.Timestamp(val, tz='UTC')
                            elif key in ['Open', 'High', 'Low', 'Close']:
                                current_df_for_tf.loc[last_idx, key] = float(val)
                            # Indicators will be recalculated, so no need to set them here
                        except (ValueError, TypeError) as e_update:
                             self.logger.warning(f"DM ({timeframe_sec}s): Could not convert value '{val}' for column '{key}' on update: {e_update}")
                             current_df_for_tf.loc[last_idx, key] = np.nan # Fallback to NaN
                df_for_processing = current_df_for_tf # Process the modified DataFrame
                self.logger.debug(f"DM ({timeframe_sec}s): Updated forming candle for time {candle_time_utc.isoformat()}.")
            else: # Out-of-order candle (older than last known)
                self.logger.warning(f"DM ({timeframe_sec}s): Received out-of-order candle data "
                                    f"(New: {candle_time_utc.isoformat()}, Last in DF: "
                                    f"{current_df_for_tf['Time'].iloc[-1].isoformat() if not current_df_for_tf.empty else 'N/A'}). "
                                    "Ignoring this candle.")
                return

            # Recalculate indicators on the potentially modified/extended DataFrame
            df_with_indicators = self._calculate_indicators(df_for_processing)
            
            # Ensure final schema and trim to MAX_CANDLES
            final_df_for_storage = self._ensure_df_schema(df_with_indicators)
            if len(final_df_for_storage) > DataManager.MAX_CANDLES:
                final_df_for_storage = final_df_for_storage.iloc[-DataManager.MAX_CANDLES:]
            
            self.all_candles_dfs[timeframe_sec] = final_df_for_storage
            self.logger.debug(f"DM ({timeframe_sec}s): DF updated. Len: {len(self.all_candles_dfs[timeframe_sec])}. "
                              f"EMA head: {self.all_candles_dfs[timeframe_sec][f'EMA{self.EMA_PERIOD}'].head().tolist() if f'EMA{self.EMA_PERIOD}' in self.all_candles_dfs[timeframe_sec] else 'N/A'}")


    def get_chart_data(self, timeframe_seconds: int) -> pd.DataFrame:
        """Returns a copy of the candle DataFrame for the specified timeframe."""
        with self.df_lock:
            df_to_return = self.all_candles_dfs.get(timeframe_seconds)
            if df_to_return is None or df_to_return.empty:
                self.logger.debug(f"No data available for timeframe {timeframe_seconds}s. Returning empty schema-correct DataFrame.")
                return self._create_empty_candles_df() # Return an empty DF with correct schema
            
            # Ensure schema and create a copy before returning to ensure thread safety for the caller
            df_copy = self._ensure_df_schema(df_to_return.copy())
            self.logger.debug(f"Returning DataFrame copy for TF {timeframe_seconds}s, len: {len(df_copy)}. "
                              f"EMA head: {df_copy[f'EMA{self.EMA_PERIOD}'].head().to_list() if f'EMA{self.EMA_PERIOD}' in df_copy and not df_copy.empty else 'N/A'}")
            return df_copy

    def update_stream_token_if_needed(self):
        """Checks if the APIClient's token needs refresh and updates the DataStream's token."""
        if self.api_client and self.data_stream:
            try:
                self.logger.info("DM: Checking/refreshing APIClient token for DataStream...")
                latest_token = self.api_client.current_token 
                
                stream_conn_status_enum = self.data_stream.connection_status # This is StreamConnectionState Enum
                self.logger.info(f"DM: Calling update_token on DataStream (current stream state: {stream_conn_status_enum.name}).")
                self.data_stream.update_token(latest_token)
            except AuthenticationError as e_auth:
                self.logger.error(f"DM: AuthenticationError during token refresh for stream: {e_auth}")
                self.last_stream_status = f"token_auth_err" 
            except APIError as e_api: 
                self.logger.error(f"DM: APIError during token refresh for stream: {e_api}")
                self.last_stream_status = f"token_api_err"
            except Exception as e_token: 
                self.logger.error(f"DM: Unexpected error updating stream token: {e_token}", exc_info=True)
                self.last_stream_status = f"token_err"
        else:
            self.logger.debug("DM: APIClient or DataStream not available for token update check.")
    
    def get_current_status_summary(self) -> str:
        """Provides a summary string of the DataManager's current operational status."""
        if not self.current_contract_id:
            return "Status: Idle / Not Initialized"
        
        status_parts = [f"Contract: {self.current_contract_id}"]
        status_parts.append(f"App Stream Status: '{self.last_stream_status}'") 
        if self.data_stream:
            # Access .name for string representation of Enum state
            status_parts.append(f"Lib Stream Conn: '{self.data_stream.connection_status.name}'")
        status_parts.append("Streaming Active" if self.is_streaming else "Streaming Inactive")
        return " | ".join(status_parts)
    
    def _map_timeframe_to_api_params(self, timeframe_seconds_for_hist: int) -> Optional[Tuple[int, int]]:
        """Maps internal timeframe (seconds) to API `unit` and `unitNumber` for history calls."""
        if timeframe_seconds_for_hist is None: 
            self.logger.error("DM History: Timeframe not provided for API param mapping.")
            return None
            
        tf_sec = timeframe_seconds_for_hist
        if tf_sec == 1: return (1, 1)      
        if tf_sec == 60: return (2, 1)     
        if tf_sec == 300: return (2, 5)    
        if tf_sec == 900: return (2, 15)   
        if tf_sec == 1800: return (2, 30)  
        if tf_sec == 3600: return (3, 1)   
        if tf_sec == 14400: return (3, 4)  
        if tf_sec == 86400: return (4, 1)  
        
        self.logger.warning(f"DM History: Timeframe {tf_sec}s does not map directly. Attempting general mapping.")
        if tf_sec < 60 : return (1, tf_sec) 
        if tf_sec % 60 == 0 and tf_sec // 60 < 60 : return (2, tf_sec // 60) 
        if tf_sec % 3600 == 0 and tf_sec // 3600 < 24: return (3, tf_sec // 3600) 
        
        self.logger.error(f"DM History: Could not reliably map timeframe {tf_sec}s to API history parameters.")
        return None

    def load_initial_history(self, num_candles_to_load: int = 200) -> bool:
        """
        Loads initial historical candle data for all supported timeframes.
        Data is fetched from the API (now returning Pydantic models) and
        prepended to the existing DataFrames.
        """
        if not self.api_client or not self.current_contract_id:
            self.logger.error("DM History: Cannot load history - APIClient or Contract ID not set.")
            return False
            
        overall_success = True
        for tf_sec in self.SUPPORTED_TIMEFRAMES_SECONDS:
            self.logger.info(f"DM History: Loading up to {num_candles_to_load} historical candles "
                             f"for TF {tf_sec}s, Contract {self.current_contract_id}.")
            
            api_contract_param: str = self.current_contract_id 
            
            timeframe_params = self._map_timeframe_to_api_params(tf_sec)
            if not timeframe_params:
                self.logger.error(f"DM History ({tf_sec}s): Failed to map timeframe. Skipping history.")
                overall_success = False; continue
                
            api_unit, api_unit_number = timeframe_params
            end_time_utc = datetime.now(dt_timezone.utc)
            safety_factor = 1.75 
            total_seconds_to_look_back = int(num_candles_to_load * tf_sec * safety_factor)
            start_time_utc_calculated = end_time_utc - timedelta(seconds=total_seconds_to_look_back)
            
            start_iso = start_time_utc_calculated.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_iso = end_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ") 
            limit_for_api_call = min(num_candles_to_load, MAX_BARS_PER_REQUEST) 
            
            self.logger.info(f"DM History ({tf_sec}s): Requesting bars - Unit: {api_unit}, Num: {api_unit_number}, Lim: {limit_for_api_call}")
            try:
                # APIClient.get_historical_bars returns schemas.HistoricalBarsResponse
                historical_bars_response: schemas.HistoricalBarsResponse = self.api_client.get_historical_bars(
                    contract_id=api_contract_param, start_time_iso=start_iso, end_time_iso=end_iso,
                    unit=api_unit, unit_number=api_unit_number, limit=limit_for_api_call,
                    include_partial_bar=False 
                )
                
                # Access .bars attribute which is List[schemas.BarData]
                fetched_bar_models: List[schemas.BarData] = historical_bars_response.bars
                
                if not fetched_bar_models:
                    self.logger.info(f"DM History ({tf_sec}s): No historical bars returned by API."); continue 
                
                historical_candles_data = []
                for bar_model in fetched_bar_models: # Iterate over Pydantic models
                    try:
                        # Access attributes directly from the Pydantic model
                        # bar_model.t is already a datetime object (UTC assumed by schema)
                        historical_candles_data.append({
                            'Time': bar_model.t, # bar_model.t is datetime
                            'Open': bar_model.o, # Already float
                            'High': bar_model.h,
                            'Low': bar_model.l,
                            'Close': bar_model.c
                        })
                    except (TypeError, AttributeError, KeyError) as e_bar_proc: 
                         self.logger.warning(f"DM History ({tf_sec}s): Error processing bar model: {bar_model}. Error: {e_bar_proc}. Skipping.")
                         continue 
                
                if not historical_candles_data:
                    self.logger.info(f"DM History ({tf_sec}s): No valid bars processed from API model response (count: {len(fetched_bar_models)}).")
                    continue

                hist_df = pd.DataFrame(historical_candles_data)
                for col in ['Open', 'High', 'Low', 'Close']: 
                    hist_df[col] = pd.to_numeric(hist_df[col], errors='coerce').astype('float64')
                # Ensure 'Time' is datetime64[ns, UTC] after DataFrame creation from list of dicts
                hist_df['Time'] = pd.to_datetime(hist_df['Time'], errors='coerce', utc=True) 
                hist_df.dropna(subset=['Time'] + ['Open', 'High', 'Low', 'Close'], inplace=True) 
                
                if hist_df.empty:
                    self.logger.info(f"DM History ({tf_sec}s): Historical DataFrame empty after processing/validation.")
                    continue

                hist_df_with_indicators = self._calculate_indicators(hist_df)
                
                with self.df_lock:
                    current_tf_df = self.all_candles_dfs.get(tf_sec)
                    if current_tf_df is None: 
                        current_tf_df = self._create_empty_candles_df()
                        self.all_candles_dfs[tf_sec] = current_tf_df

                    hist_df_schema_ok = self._ensure_df_schema(hist_df_with_indicators)
                    current_tf_df_schema_ok = self._ensure_df_schema(current_tf_df)

                    combined_df = pd.concat([hist_df_schema_ok, current_tf_df_schema_ok], ignore_index=True)
                    combined_df.drop_duplicates(subset=['Time'], keep='first', inplace=True) 
                    combined_df.sort_values(by='Time', inplace=True, ascending=True)
                    
                    final_stored_df = self._ensure_df_schema(combined_df)
                    if len(final_stored_df) > DataManager.MAX_CANDLES:
                        final_stored_df = final_stored_df.iloc[-DataManager.MAX_CANDLES:]
                    
                    self.all_candles_dfs[tf_sec] = final_stored_df

                self.logger.info(f"DM History ({tf_sec}s): Loaded {len(hist_df_with_indicators)} candles. Total: {len(self.all_candles_dfs[tf_sec])}.")
                self.logger.debug(f"DM History ({tf_sec}s) Head:\n{self.all_candles_dfs[tf_sec].head(3)}")
                self.logger.debug(f"DM History ({tf_sec}s) Tail:\n{self.all_candles_dfs[tf_sec].tail(3)}")

            except APIResponseParsingError as e_parse_hist: # Catch Pydantic validation errors
                 self.logger.error(f"DM History ({tf_sec}s): Error parsing historical bars response: {e_parse_hist}")
                 overall_success = False
            except APIError as e_api_hist:
                self.logger.error(f"DM History ({tf_sec}s): APIError loading history: {e_api_hist}")
                overall_success = False
            except Exception as e_hist_generic: 
                self.logger.error(f"DM History ({tf_sec}s): Unexpected error loading history: {e_hist_generic}", exc_info=True)
                overall_success = False
        return overall_success