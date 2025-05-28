# tsxapipy/pipeline/data_manager.py

import logging
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from typing import Dict, List, Callable, Optional, Any, Union, Tuple
import threading # Ensure threading is imported if df_lock is used
import time
import inspect

# Import from tsxapipy package
from tsxapipy.common.time_utils import UTC_TZ
from tsxapipy.api import schemas
from tsxapipy.api.exceptions import (
    APIError,                     # General API Error
    ValueError as APIValueError, # Aliased from tsxapipy.api.exceptions.ValueError
    APIResponseParsingError,      # For issues parsing API responses
    AuthenticationError,          # For auth failures
    ConfigurationError            # For config issues
)
from tsxapipy.auth import authenticate 
from tsxapipy.api.client import APIClient 
from tsxapipy.pipeline.candle_aggregator import LiveCandleAggregator 
from tsxapipy.real_time.data_stream import DataStream 
from tsxapipy.real_time.stream_state import StreamConnectionState 
from tsxapipy.config import ACCOUNT_ID_TO_WATCH as DEFAULT_DM_ACCOUNT_ID 

logger = logging.getLogger(__name__)

# REMOVED the standalone _safe_authenticate() as it's not used by the class version of initialize_components
# The class initialize_components calls the main tsxapipy.auth.authenticate

class DataManager:
    """
    Manages data flow from API to application components.
    
    This class handles authentication, data streaming, and candle aggregation.
    """
    
    # Added class constants for default indicator periods as per project description context
    EMA_PERIOD = 9
    SMA_PERIOD = 20
    MAX_CANDLES = 1000 # Default max candles if not configured otherwise by app

    def __init__(self, 
                 supported_timeframes: Optional[List[int]] = None, # Corrected type hint
                 ema_period: Optional[int] = None, # Allow overriding class defaults
                 sma_period: Optional[int] = None, # Allow overriding class defaults
                 account_id_for_history: Optional[int] = None,
                 max_candles_to_store: Optional[int] = None): # Added for consistency
        """
        Initialize the DataManager.
        
        Args:
            supported_timeframes: List of timeframes in seconds to support (default: [300, 900, 3600])
            ema_period: Period for EMA calculation (default: class EMA_PERIOD)
            sma_period: Period for SMA calculation (default: class SMA_PERIOD)
            account_id_for_history: Account ID for historical data calls.
            max_candles_to_store: Max number of candles to keep per timeframe.
        """
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}") # More specific logger name
        
        # Handle account_id_for_history parsing from config if not directly provided
        parsed_default_dm_account_id: Optional[int] = None
        if isinstance(DEFAULT_DM_ACCOUNT_ID, int) and DEFAULT_DM_ACCOUNT_ID > 0:
            parsed_default_dm_account_id = DEFAULT_DM_ACCOUNT_ID
        elif isinstance(DEFAULT_DM_ACCOUNT_ID, str) and DEFAULT_DM_ACCOUNT_ID.strip():
            try:
                val = int(DEFAULT_DM_ACCOUNT_ID)
                if val > 0: parsed_default_dm_account_id = val
            except ValueError:
                self.logger.warning(f"Could not parse DEFAULT_DM_ACCOUNT_ID ('{DEFAULT_DM_ACCOUNT_ID}') to a positive int.")
        
        self.account_id_for_history = account_id_for_history if isinstance(account_id_for_history, int) and account_id_for_history > 0 \
                                     else parsed_default_dm_account_id
        
        if self.account_id_for_history is None:
            self.logger.warning("DataManager initialized without a specific valid positive account_id_for_history. History calls might fail if accountId is required.")
                    
        self.supported_timeframes = supported_timeframes if supported_timeframes is not None else [300, 900, 3600]
        
        # Use provided periods or fall back to class defaults
        self.EMA_PERIOD = ema_period if ema_period is not None else DataManager.EMA_PERIOD
        self.SMA_PERIOD = sma_period if sma_period is not None else DataManager.SMA_PERIOD
        self.MAX_CANDLES = max_candles_to_store if max_candles_to_store is not None else DataManager.MAX_CANDLES
        
        self.api_client: Optional[APIClient] = None
        self.data_stream: Optional[DataStream] = None
        self.candle_aggregators: Dict[int, LiveCandleAggregator] = {}
        
        self.is_streaming = False
        self.current_contract_id: Optional[str] = None
        self.last_stream_status: str = StreamConnectionState.NOT_INITIALIZED.name # Store enum name
        
        # These seem to be for external subscription, not internal use by DataStream directly
        # self.on_candle_update_callbacks: Dict[int, List[Callable]] = {tf: [] for tf in self.supported_timeframes}
        # self.on_quote_update_callbacks: List[Callable] = []
        # self.on_trade_update_callbacks: List[Callable] = []
        # self.on_stream_state_change_callbacks: List[Callable] = []
        
        # self.historical_data = {} # This attribute was present but not clearly used; remove if unused.

        self.candle_dtypes = {
            'Time': 'datetime64[ns, UTC]', 'Open': 'float64', 'High': 'float64',
            'Low': 'float64', 'Close': 'float64', 'Volume': 'float64',
            f'EMA{self.EMA_PERIOD}': 'float64', f'SMA{self.SMA_PERIOD}': 'float64'
        }
        
        self.all_candles_dfs: Dict[int, pd.DataFrame] = {}
        for tf_sec in self.supported_timeframes:
            self.all_candles_dfs[tf_sec] = self._create_empty_candles_df()
        
        self.logger.info(
            f"DataManager initialized. Timeframes: {self.supported_timeframes}s. "
            f"EMA: {self.EMA_PERIOD}, SMA: {self.SMA_PERIOD}, MaxCandles: {self.MAX_CANDLES}. "
            f"History Account: {self.account_id_for_history or 'Not Set'}."
        )
        
        self.df_lock = threading.RLock()
        # self.latest_data_lock = threading.RLock() # If used for latest_quote/trade
        # self.latest_quote = None
        # self.latest_trade = None
        # self.latest_quote_timestamp = None
        # self.latest_trade_timestamp = None

        # The callbacks like self.on_quote_callback, self.on_trade_callback were set to None
        # in your version. If DataStream needs to call methods on this DataManager instance,
        # they should be actual methods like self._on_quote_received, not attributes that are None.

    def _on_quote_received(self, quote_data: Dict[str, Any]):
        self.logger.debug(
            f"DM _on_quote_received for {self.current_contract_id}: "
            f"LP={quote_data.get('lastPrice')}, V={quote_data.get('volume')}, "
            f"TS(Orig)={quote_data.get('timestamp')}"
    )
        
    # No synthetic trade generation for this test
    pass

    def _on_depth_received(self, depth_data: Any): # Using Any for now
        self.logger.debug(f"DM _on_depth_received: Received depth data for {self.current_contract_id}. Type: {type(depth_data)}")
        if isinstance(depth_data, list) and depth_data:
            self.logger.info(f"DM _on_depth_received: First depth level: {depth_data[0]}")
        # Actual processing would go here
        pass

    def _on_stream_error(self, error: Any):
        """Handles errors reported by the DataStream instance passed via on_error_callback."""
        self.logger.error(f"DM received stream error for '{self.current_contract_id}': {error}")
        self.is_streaming = False 
        self.last_stream_status = StreamConnectionState.ERROR.name
        # If external callbacks are registered for DataManager's stream state:
        # for callback in self.on_stream_state_change_callbacks: # Use appropriate callback list
        #     try: callback(self.last_stream_status)
        #     except Exception as e: self.logger.error(f"Error in DM stream error -> state change callback: {e}")

    def _on_stream_state_change(self, state_name: str): 
        """Handles state changes from the DataStream passed via on_state_change_callback."""
        self.logger.info(f"DM received stream state change for '{self.current_contract_id}': {state_name}")
        self.last_stream_status = state_name
        
        if state_name == StreamConnectionState.CONNECTED.name:
            self.is_streaming = True
        else: 
            self.is_streaming = False
        
        # If external callbacks are registered for DataManager's stream state:
        # for callback in self.on_stream_state_change_callbacks:
        #     try: callback(state_name)
        #     except Exception as e: self.logger.error(f"Error in DM stream state change callback: {e}")

    def initialize_components(self, contract_id: str, account_id_for_data: Optional[int] = None) -> bool: # Added account_id_for_data
        """Initializes API client, data stream, and candle aggregators for all supported timeframes."""
        self.logger.info(f"Initializing components for Contract: {contract_id} across all supported timeframes.")
        self.current_contract_id = contract_id

        # Update account_id_for_history if a valid one is passed
        if account_id_for_data is not None and isinstance(account_id_for_data, int) and account_id_for_data > 0:
            self.account_id_for_history = account_id_for_data
            self.logger.info(f"DataManager account_id_for_history explicitly set/updated to: {self.account_id_for_history}")
        elif self.account_id_for_history is None: # Check if it's still None after __init__ and this potential update
             self.logger.warning("DataManager: account_id_for_history remains None for initialize_components. History calls may require it.")

        try:
            # Check current stream status more robustly
            current_stream_status_is_active = False
            if self.data_stream and hasattr(self.data_stream, 'connection_status'):
                current_stream_status_is_active = self.data_stream.connection_status in [
                    StreamConnectionState.CONNECTING, 
                    StreamConnectionState.CONNECTED,
                    StreamConnectionState.RECONNECTING_TOKEN,
                    StreamConnectionState.RECONNECTING_UNEXPECTED
                ]

            if self.data_stream and (self.is_streaming or current_stream_status_is_active):
                self.logger.info("Previously streaming or connecting. Stopping existing stream before re-initializing.")
                self.stop_streaming(reason="Re-initializing components") # Pass reason to stop_streaming
                time.sleep(1.0) # Allow time for stop to fully process, adjust if needed

            self.logger.debug("Authenticating for DataManager...")
            # Use the main authenticate function from tsxapipy.auth
            auth_result = authenticate() # This should use config USERNAME/API_KEY
            if not auth_result or not auth_result[0] or not auth_result[1]:
                self.logger.error("Authentication failed: No token or acquisition time received.")
                self.last_stream_status = "init_auth_failed" # Custom status
                return False
            initial_token, token_acquired_at = auth_result
            
            self.api_client = APIClient( # Pass only token and acquired_at
                initial_token=initial_token, 
                token_acquired_at=token_acquired_at
                # APIClient's __init__ should pick up reauth credentials from config if needed
            )
            self.logger.info("APIClient initialized in DataManager.")
            
            self.candle_aggregators = {} # Clear previous aggregators
            for tf_sec in self.supported_timeframes: # Use self.supported_timeframes
                self.candle_aggregators[tf_sec] = LiveCandleAggregator(
                    contract_id=contract_id,
                    timeframe_seconds=tf_sec, # Pass correct arg name
                    new_candle_data_callback=self._handle_new_candle_data_from_aggregator # Correct callback
                )
            
            self.logger.info(f"LiveCandleAggregator for {tf_sec}s initialized.")
            
            # Initialize data stream with corrected callback names
            self.data_stream = DataStream(
                api_client=self.api_client,
                contract_id_to_subscribe=contract_id, # Match DataStream's __init__
                on_quote_callback=self._on_quote_received,    # Now defined
                on_trade_callback=self._pass_trade_to_aggregators, # Correct method for trades
                on_depth_callback=self._on_depth_received,  
                on_error_callback=self._on_stream_error,      # Now defined
                on_state_change_callback=self._on_stream_state_change, # Now defined
                auto_subscribe_quotes=True,  
                auto_subscribe_trades=True, 
                auto_subscribe_depth=False     
            )
            
            self.last_stream_status = StreamConnectionState.NOT_INITIALIZED.name # Reset before DataStream start
            return True
        except (AuthenticationError, ConfigurationError, APIError) as e:
            self.logger.error(f"Failed to initialize components due to API/Auth/Config error: {e}", exc_info=False) # Log less for these
            self._cleanup_after_init_failure(f"init_error_{type(e).__name__}")
            return False
        except Exception as e_init: # Catch any other unexpected error during init
            self.logger.error(f"Unexpected error during component initialization: {e_init}", exc_info=True)
            self._cleanup_after_init_failure(f"unexpected_init_error_{type(e_init).__name__}")
            return False

    def _cleanup_after_init_failure(self, status_message: str):
        """Helper to reset components and state on initialization failure."""
        self.api_client = None
        self.data_stream = None
        self.candle_aggregators.clear()
        self.last_stream_status = status_message
        self.is_streaming = False # Ensure is_streaming is reset

    def _pass_trade_to_aggregators(self, trade_data: Dict[str, Any]):
        self.logger.info(f"DM _pass_trade_to_aggregators CALLED with trade: Price={trade_data.get('Price')}, Vol={trade_data.get('Volume')}, TS={trade_data.get('Timestamp')}")

        """Passes a single trade from DataStream to all registered candle aggregators."""
        if not isinstance(trade_data, dict):
            self.logger.warning(f"DM: _pass_trade_to_aggregators received non-dict trade_data: {type(trade_data)}")
            return
        
        for tf_sec, aggregator in self.candle_aggregators.items():
            if aggregator: 
                try:
                    aggregator.add_trade(trade_data)
                except Exception as e:
                    self.logger.error(f"DM: Error passing trade to aggregator for {tf_sec}s: {e}", exc_info=True)
        
        # If external callbacks for raw trades from DataManager are needed:
        # for callback in self.on_trade_update_callbacks:
        #     try: callback(trade_data)
        #     except Exception as e: self.logger.error(f"Error in DM trade update callback: {e}")

    def start_streaming(self) -> bool:
        """Starts the DataStream if it's initialized."""
        if not self.data_stream:
            self.logger.error("DM StartStream: DataStream not initialized. Cannot start.")
            return False
        
        current_status = self.data_stream.connection_status if hasattr(self.data_stream, 'connection_status') else None
        if current_status == StreamConnectionState.CONNECTED:
            self.logger.warning("DM StartStream: DataStream already connected.")
            self.is_streaming = True 
            if current_status: self.last_stream_status = current_status.name
            return True
        if current_status == StreamConnectionState.CONNECTING:
            self.logger.warning("DM StartStream: DataStream already attempting to connect.")
            if current_status: self.last_stream_status = current_status.name
            return True 

        self.logger.info("DM StartStream: Attempting to start DataStream...")
        
        if self.data_stream.start(): # DataStream.start() now returns bool
            self.logger.info("DM StartStream: DataStream start method called successfully (connection is async).")
            # Actual state (CONNECTING, CONNECTED, ERROR) will be set by _on_stream_state_change callback
            return True
        else:
            self.logger.error("DM StartStream: DataStream.start() returned False or failed to initiate.")
            return False

    def stop_streaming(self, reason: str = "User requested DataManager stop"): # Added reason parameter
        """Stops the DataStream if it's active or trying to connect."""
        if not self.data_stream:
            self.logger.info("DM StopStream: No DataStream instance to stop.")
            return # No action needed
        
        current_status = self.data_stream.connection_status if hasattr(self.data_stream, 'connection_status') else None
        # Only attempt to stop if it's not already in a fully stopped/uninitialized state
        if current_status not in [StreamConnectionState.DISCONNECTED, StreamConnectionState.NOT_INITIALIZED]:
            self.logger.info(f"DM StopStream: Attempting to stop DataStream. Reason: {reason}. Current Status: {current_status.name if current_status else 'N/A'}")
            self.data_stream.stop(reason_for_stop=reason) 
            self.logger.info("DM StopStream: DataStream stop method called (disconnection is async).")
            # self.is_streaming and self.last_stream_status will be updated by _on_stream_state_change
        else:
            self.logger.info(f"DM StopStream: DataStream already stopped/not initialized (Status: {current_status.name if current_status else 'N/A'}). Ensuring local state is consistent.")
            self.is_streaming = False # Ensure consistency
            if current_status: self.last_stream_status = current_status.name
            else: self.last_stream_status = StreamConnectionState.DISCONNECTED.name


    # _create_empty_candles_df, _ensure_df_schema, _calculate_indicators,
    # load_initial_history, _handle_new_candle_data_from_aggregator,
    # get_chart_data, update_stream_token_if_needed, get_current_status_summary,
    # _map_timeframe_to_api_params
    # --- These methods should be included below, as per their last corrected versions ---
    # --- For brevity, I will assume they are correctly placed here from previous interactions ---
    # --- Make sure to copy them from the version that includes all fixes for them ---

    # Copied from previous "complete and consolidated code for load_initial_history"
    def load_initial_history(self, timeframe_seconds: int, num_candles_to_load: Optional[int] = None) -> bool:
        effective_num_candles_to_load = num_candles_to_load if num_candles_to_load is not None else (self.MAX_CANDLES // 2)
        
        self.logger.info(
            f"DM History: Loading up to {effective_num_candles_to_load} candles for "
            f"{timeframe_seconds}s timeframe for contract {self.current_contract_id} "
            f"(Account: {self.account_id_for_history or 'None'})"
        )

        if not self.api_client:
            self.logger.error(f"DM History ({timeframe_seconds}s): APIClient not initialized. Cannot load history.")
            return False
        if not self.current_contract_id:
            self.logger.error(f"DM History ({timeframe_seconds}s): Current contract ID not set. Cannot load history.")
            return False

        api_params = self._map_timeframe_to_api_params(timeframe_seconds)
        if not api_params:
            self.logger.error(f"DM History ({timeframe_seconds}s): Could not map timeframe to API parameters.")
            self._ensure_empty_df_exists(timeframe_seconds)
            return False 
        
        unit, unit_number = api_params
        end_time_dt = datetime.now(UTC_TZ)
        estimated_total_seconds_needed = effective_num_candles_to_load * timeframe_seconds
        
        if timeframe_seconds >= 86400: 
            buffer_days = effective_num_candles_to_load * 1.7 
            start_time_dt = end_time_dt - timedelta(days=int(buffer_days))
        else: 
            buffer_multiplier = 5.0
            max_buffer_days_for_intraday = 15 
            calculated_buffer_seconds = estimated_total_seconds_needed * buffer_multiplier
            start_time_dt_candidate1 = end_time_dt - timedelta(seconds=calculated_buffer_seconds)
            start_time_dt_candidate2 = end_time_dt - timedelta(days=max_buffer_days_for_intraday)
            start_time_dt = max(start_time_dt_candidate1, start_time_dt_candidate2)

        start_time_iso = start_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time_iso = end_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        self.logger.info(
            f"DM History ({timeframe_seconds}s): Fetching API - Unit: {unit}, Num: {unit_number}, "
            f"Start: {start_time_iso}, End: {end_time_iso}, Limit: {effective_num_candles_to_load}."
        )

        try:
            historical_response: schemas.HistoricalBarsResponse = self.api_client.get_historical_bars(
                contract_id=self.current_contract_id,
                start_time_iso=start_time_iso,
                end_time_iso=end_time_iso,
                unit=unit,
                unit_number=unit_number,
                limit=effective_num_candles_to_load, 
                account_id=self.account_id_for_history, 
                include_partial_bar=False,
                live=False
            )

            if not historical_response.bars:
                self.logger.info(
                    f"DM History ({timeframe_seconds}s): No historical bars returned by API for "
                    f"{self.current_contract_id} (Account: {self.account_id_for_history})."
                )
                self._ensure_empty_df_exists(timeframe_seconds)
                return True 

            bars_list_of_dicts = [bar.model_dump() for bar in historical_response.bars]
            hist_df = pd.DataFrame(bars_list_of_dicts)
            rename_map = {'t': 'Time', 'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'}
            hist_df.rename(columns=rename_map, inplace=True)
            self.logger.info(f"DM History ({timeframe_seconds}s): Received {len(hist_df)} raw bars from API.")

            with self.df_lock:
                hist_df_schema_correct = self._ensure_df_schema(hist_df)
                if hist_df_schema_correct.empty:
                    self.logger.warning(f"DM History ({timeframe_seconds}s): Historical DataFrame empty after schema enforcement.")
                    self._ensure_empty_df_exists(timeframe_seconds)
                    return True 

                df_with_indicators = self._calculate_indicators(hist_df_schema_correct)
                final_df = self._ensure_df_schema(df_with_indicators)
                
                if not final_df.empty and 'Time' in final_df.columns:
                    final_df.sort_values(by='Time', inplace=True) 
                    if len(final_df) > self.MAX_CANDLES: 
                        self.logger.info(f"DM History ({timeframe_seconds}s): Trimming loaded history from {len(final_df)} to {self.MAX_CANDLES} bars.")
                        final_df = final_df.iloc[-self.MAX_CANDLES:]
                
                self.all_candles_dfs[timeframe_seconds] = final_df
                self.logger.info(
                    f"DM History ({timeframe_seconds}s): Stored {len(final_df)} historical bars."
                )
            return True
        except APIValueError as e_val: 
            self.logger.error(f"DM History ({timeframe_seconds}s): APIValueError for {self.current_contract_id}: {e_val}")
        except APIResponseParsingError as e_parse: 
            self.logger.error(f"DM History ({timeframe_seconds}s): APIResponseParsingError for {self.current_contract_id}: {e_parse}")
        except APIError as e_api: 
            self.logger.error(f"DM History ({timeframe_seconds}s): APIError for {self.current_contract_id}: {e_api}")
        except Exception as e_hist: 
            self.logger.error(f"DM History ({timeframe_seconds}s): Unexpected error for {self.current_contract_id}: {e_hist}", exc_info=True)
        
        self._ensure_empty_df_exists(timeframe_seconds) 
        return False

    def _ensure_empty_df_exists(self, timeframe_seconds: int):
        with self.df_lock:
            if timeframe_seconds not in self.all_candles_dfs or \
               self.all_candles_dfs.get(timeframe_seconds) is None or \
               self.all_candles_dfs.get(timeframe_seconds, pd.DataFrame()).empty:
                self.all_candles_dfs[timeframe_seconds] = self._create_empty_candles_df()


    def _handle_new_candle_data_from_aggregator(self, 
                                                candle_data_series: pd.Series,
                                                is_forming_candle: bool, 
                                                timeframe_sec: int):
        """
        Handles new candle data (pd.Series) from LiveCandleAggregator.
        Updates the corresponding DataFrame, calculates indicators, and manages data integrity.
        Includes detailed logging for debugging MA issues.
        """
        func_exec_start_time = time.monotonic()

        if not isinstance(candle_data_series, pd.Series) or candle_data_series.empty:
            self.logger.warning(f"DM HandleNewCandle ({timeframe_sec}s): Received invalid or empty candle_data_series. Skipping.")
            return

        required_keys = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        if not all(key in candle_data_series.index for key in required_keys):
            self.logger.warning(
                f"DM HandleNewCandle ({timeframe_sec}s): Candle series missing required keys "
                f"(need {required_keys}). Got: {list(candle_data_series.index)}. Data: {candle_data_series.to_dict()}. Skipping."
            )
            return
        
        # --- Timestamp Processing: Ensure UTC pd.Timestamp ---
        try:
            candle_time_input = candle_data_series['Time']
            candle_time_pd_utc: pd.Timestamp 

            if isinstance(candle_time_input, datetime):
                if candle_time_input.tzinfo is None: 
                    self.logger.debug(f"DM HandleNewCandle ({timeframe_sec}s): Localizing naive datetime '{candle_time_input}' to UTC.")
                    candle_time_pd_utc = pd.Timestamp(candle_time_input, tz='UTC')
                elif str(candle_time_input.tzinfo).upper() != 'UTC': 
                    self.logger.debug(f"DM HandleNewCandle ({timeframe_sec}s): Converting non-UTC datetime '{candle_time_input}' to UTC.")
                    candle_time_pd_utc = pd.Timestamp(candle_time_input).tz_convert('UTC')
                else: 
                    candle_time_pd_utc = pd.Timestamp(candle_time_input) 
            elif isinstance(candle_time_input, pd.Timestamp):
                if candle_time_input.tzinfo is None: 
                    candle_time_pd_utc = candle_time_input.tz_localize('UTC', ambiguous='infer', nonexistent='NaT')
                elif str(candle_time_input.tzinfo).upper() != 'UTC': 
                    candle_time_pd_utc = candle_time_input.tz_convert('UTC')
                else: 
                    candle_time_pd_utc = candle_time_input
            else: 
                self.logger.debug(f"DM HandleNewCandle ({timeframe_sec}s): Parsing timestamp string '{candle_time_input}' as UTC.")
                candle_time_pd_utc = pd.Timestamp(str(candle_time_input), tz='UTC')

            if pd.isna(candle_time_pd_utc): 
                raise ValueError(f"Timestamp '{candle_data_series.get('Time')}' became NaT after processing.")
        except Exception as e_ts:
            self.logger.error(f"DM HandleNewCandle ({timeframe_sec}s): Invalid 'Time' in candle series: {e_ts}. Skipping candle."); return

        # --- OHLCV Processing: Ensure numeric and valid relationships ---
        new_row_data_ohlcv = {}
        conversion_successful = True
        for key in ['Open', 'High', 'Low', 'Close', 'Volume']:
            try: 
                val = float(candle_data_series[key])
                if val < 0 and key != 'Volume': 
                    self.logger.warning(f"DM HandleNewCandle ({timeframe_sec}s): Negative {key} ('{val}'). Using abs.")
                    val = abs(val)
                elif val < 0 and key == 'Volume':
                    self.logger.warning(f"DM HandleNewCandle ({timeframe_sec}s): Negative volume ('{val}'). Setting to 0.")
                    val = 0.0
                new_row_data_ohlcv[key] = val
            except (ValueError, TypeError, KeyError): 
                self.logger.error(f"DM HandleNewCandle ({timeframe_sec}s): Invalid/missing value for {key} ('{candle_data_series.get(key)}'). Skipping candle.")
                conversion_successful = False
                break
        if not conversion_successful: return
        
        if new_row_data_ohlcv['High'] < new_row_data_ohlcv['Low']:
            self.logger.warning(
                f"DM HandleNewCandle ({timeframe_sec}s): High < Low ({new_row_data_ohlcv['High']} < {new_row_data_ohlcv['Low']}) "
                f"for candle {candle_time_pd_utc.isoformat()}. Swapping."
            )
            new_row_data_ohlcv['High'], new_row_data_ohlcv['Low'] = new_row_data_ohlcv['Low'], new_row_data_ohlcv['High']
        new_row_data_ohlcv['Open'] = max(min(new_row_data_ohlcv['Open'], new_row_data_ohlcv['High']), new_row_data_ohlcv['Low'])
        new_row_data_ohlcv['Close'] = max(min(new_row_data_ohlcv['Close'], new_row_data_ohlcv['High']), new_row_data_ohlcv['Low'])

        # --- DataFrame Update Logic (thread-safe) ---
        with self.df_lock:
            current_df = self.all_candles_dfs.get(timeframe_sec)
            if current_df is None: 
                self.logger.error(f"DM HandleNewCandle ({timeframe_sec}s): DataFrame was None, this should not happen. Re-initializing.")
                current_df = self._create_empty_candles_df()
                self.all_candles_dfs[timeframe_sec] = current_df

            df_for_processing: pd.DataFrame 
            last_df_time_utc: Optional[pd.Timestamp] = None

            if not current_df.empty and 'Time' in current_df.columns and current_df['Time'].notna().any():
                valid_times = current_df['Time'].dropna() 
                if not valid_times.empty:
                    last_df_time_utc = valid_times.iloc[-1] 
            
            action = "unknown"
            if last_df_time_utc is not None: 
                if candle_time_pd_utc == last_df_time_utc:
                    action = "update"
                elif candle_time_pd_utc > last_df_time_utc:
                    action = "append"
                else: 
                    self.logger.warning(
                        f"DM HandleNewCandle ({timeframe_sec}s): Received out-of-order candle data "
                        f"(New: {candle_time_pd_utc.isoformat()}, Last in DF: {last_df_time_utc.isoformat()}). Ignoring this candle."
                    )
                    return 
            else: 
                action = "append_to_empty"
                self.logger.debug(
                    f"DM HandleNewCandle ({timeframe_sec}s): DataFrame is empty or has no valid last timestamp. "
                    f"Appending new candle: {candle_time_pd_utc.isoformat()}"
                )

            if action == "update":
                last_idx = current_df.index[-1]
                # Keep existing Open if it's already set for this forming candle
                if pd.isna(current_df.loc[last_idx, 'Open']):
                    current_df.loc[last_idx, 'Open'] = new_row_data_ohlcv['Open']
                
                current_df.loc[last_idx, 'High'] = max(current_df.loc[last_idx, 'High'] if pd.notna(current_df.loc[last_idx, 'High']) else -np.inf, 
                                                       new_row_data_ohlcv['High'])
                current_df.loc[last_idx, 'Low'] = min(current_df.loc[last_idx, 'Low'] if pd.notna(current_df.loc[last_idx, 'Low']) else np.inf, 
                                                      new_row_data_ohlcv['Low'])
                current_df.loc[last_idx, 'Close'] = new_row_data_ohlcv['Close']
                current_df.loc[last_idx, 'Volume'] = (current_df.loc[last_idx, 'Volume'] if pd.notna(current_df.loc[last_idx, 'Volume']) else 0) + new_row_data_ohlcv['Volume']
                df_for_processing = current_df
                self.logger.debug(f"DM HandleNewCandle ({timeframe_sec}s): Updated forming candle for {candle_time_pd_utc.isoformat()}")
            
            elif action == "append" or action == "append_to_empty":
                new_row_dict = {'Time': candle_time_pd_utc, **new_row_data_ohlcv}
                for col_name in self.candle_dtypes.keys(): # Ensure all schema columns are present
                    if col_name not in new_row_dict: new_row_dict[col_name] = np.nan
                
                temp_df_for_concat = pd.DataFrame([new_row_dict])
                temp_df_for_concat = self._ensure_df_schema(temp_df_for_concat) 
                
                if action == "append_to_empty" or current_df.empty:
                    df_for_processing = temp_df_for_concat
                else:
                    # Ensure current_df schema before concat if there's any doubt
                    current_df_schema_checked = self._ensure_df_schema(current_df.copy())
                    df_for_processing = pd.concat([current_df_schema_checked, temp_df_for_concat], ignore_index=True)
                
                self.logger.debug(f"DM HandleNewCandle ({timeframe_sec}s): Appended new candle for {candle_time_pd_utc.isoformat()}. IsForming: {is_forming_candle}")
            else: 
                self.logger.error(f"DM HandleNewCandle ({timeframe_sec}s): Unhandled action '{action}'. This should not occur. Candle: {candle_time_pd_utc.isoformat()}")
                return

            # --- LOG DF BEFORE INDICATORS ---
            if not df_for_processing.empty:
                self.logger.debug(
                    f"DM HandleNewCandle ({timeframe_sec}s): df_for_processing (len {len(df_for_processing)}) "
                    f"BEFORE indicators. Last 3 'Time': {df_for_processing['Time'].tail(3).dt.strftime('%Y-%m-%dT%H:%M:%S%z').tolist()}, "
                    f"'Close': {df_for_processing['Close'].tail(3).tolist()}"
                )
            else:
                self.logger.debug(f"DM HandleNewCandle ({timeframe_sec}s): df_for_processing is EMPTY before indicators.")

            df_with_indicators = self._calculate_indicators(df_for_processing)
            
            # --- LOG DF AFTER INDICATORS ---
            ema_col = f'EMA{self.EMA_PERIOD}'
            sma_col = f'SMA{self.SMA_PERIOD}'
            if not df_with_indicators.empty:
                self.logger.debug(
                    f"DM HandleNewCandle ({timeframe_sec}s): df_with_indicators (len {len(df_with_indicators)}) "
                    f"AFTER indicators. Last 3 '{ema_col}': {df_with_indicators[ema_col].tail(3).tolist() if ema_col in df_with_indicators else 'N/A'}, "
                    f"'{sma_col}': {df_with_indicators[sma_col].tail(3).tolist() if sma_col in df_with_indicators else 'N/A'}"
                )
            else:
                self.logger.debug(f"DM HandleNewCandle ({timeframe_sec}s): df_with_indicators is EMPTY after indicators.")

            final_df_for_storage = self._ensure_df_schema(df_with_indicators)
            
            if len(final_df_for_storage) > self.MAX_CANDLES:
                final_df_for_storage = final_df_for_storage.iloc[-self.MAX_CANDLES:].reset_index(drop=True)
            
            self.all_candles_dfs[timeframe_sec] = final_df_for_storage
            
            func_exec_end_time = time.monotonic()
            duration_ms = (func_exec_end_time - func_exec_start_time) * 1000
            
            self.logger.info(
                f"DM HandleNewCandle ({timeframe_sec}s) PROCESSED: CandleTime='{candle_time_pd_utc.strftime('%H:%M:%S') if 'candle_time_pd_utc' in locals() else 'N/A'}', "
                f"IsForming={is_forming_candle}, Took={duration_ms:.2f}ms. DF len={len(self.all_candles_dfs.get(timeframe_sec, []))}")
            
            self.logger.debug(
                f"DM HandleNewCandle ({timeframe_sec}s): DataFrame for {self.current_contract_id} updated. "
                f"New length: {len(final_df_for_storage)}. Last candle time: "
                f"{final_df_for_storage['Time'].iloc[-1].isoformat() if not final_df_for_storage.empty else 'N/A'}."
            )
            
            # Handle external callbacks if defined (e.g., for UI updates)
            # This part depends on how your DataManager is designed to notify subscribers
            # Assuming you have `self.on_candle_update_callbacks` as described in your `__init__`
            if timeframe_sec in getattr(self, 'on_candle_update_callbacks', {}) and \
               self.on_candle_update_callbacks[timeframe_sec]:
                
                # Decide what data to send: the full df, the last candle, or just an update signal
                # For now, sending the last candle if it's not a forming one (or always if forming is also needed)
                if not is_forming_candle or True: # Send update for forming candles too if needed by UI
                    if not final_df_for_storage.empty:
                        # Pass a copy to prevent modification by callbacks
                        data_to_send = final_df_for_storage.iloc[-1].copy() 
                        for callback in self.on_candle_update_callbacks[timeframe_sec]:
                            try:
                                # Callback signature might be (candle_series, timeframe_sec, is_forming)
                                callback(data_to_send, timeframe_sec, is_forming_candle) 
                            except Exception as e_cb:
                                self.logger.error(
                                    f"DM HandleNewCandle: Error in external candle update callback for TF {timeframe_sec}s: {e_cb}", 
                                    exc_info=True
                                )
                                
    def get_chart_data(self, timeframe_seconds: int) -> pd.DataFrame:
        current_logger = self.logger # Assuming self.logger is set in __init__
        df_to_return: Optional[pd.DataFrame] = None # Initialize

        with self.df_lock:
            df_to_return = self.all_candles_dfs.get(timeframe_seconds)
            if df_to_return is None or df_to_return.empty:
                current_logger.debug(
                    f"DM get_chart_data ({timeframe_seconds}s): No data or empty DataFrame found. Returning new empty DF."
                )
                return self._create_empty_candles_df() # Ensure it returns a DF with correct schema

            # Log details of the data being returned
            df_copy = self._ensure_df_schema(df_to_return.copy()) # Ensure schema and work with a copy
            if not df_copy.empty:
                last_candle_time = df_copy['Time'].iloc[-1] if 'Time' in df_copy.columns else "N/A"
                last_candle_close = df_copy['Close'].iloc[-1] if 'Close' in df_copy.columns else "N/A"
                ema_col = f'EMA{self.EMA_PERIOD}'
                sma_col = f'SMA{self.SMA_PERIOD}'
                last_ema = df_copy[ema_col].iloc[-1] if ema_col in df_copy.columns and not df_copy[ema_col].empty else "N/A"
                last_sma = df_copy[sma_col].iloc[-1] if sma_col in df_copy.columns and not df_copy[sma_col].empty else "N/A"

                current_logger.info( # Changed to INFO for easier spotting
                    f"DM get_chart_data ({timeframe_seconds}s): Returning DF with {len(df_copy)} rows. "
                    f"Last Candle - Time: {last_candle_time}, Close: {last_candle_close}, "
                    f"{ema_col}: {last_ema}, {sma_col}: {last_sma}"
                )
            else:
                 current_logger.info(
                    f"DM get_chart_data ({timeframe_seconds}s): Returning EMPTY DF after schema ensure/copy."
                )
            return df_copy # Return the schema-ensured copy
            
    def update_stream_token_if_needed(self):
        if self.api_client and self.data_stream and hasattr(self.data_stream, 'update_token'):
            try:
                self.logger.info("DM TokenUpdate: Checking/refreshing APIClient token for DataStream...")
                latest_token = self.api_client.current_token 
                stream_conn_status_obj = self.data_stream.connection_status
                self.logger.info(f"DM TokenUpdate: Calling update_token on DataStream (state: {stream_conn_status_obj.name}).")
                self.data_stream.update_token(latest_token)
            except (AuthenticationError, APIError) as e_auth_api:
                 self.logger.error(f"DM TokenUpdate: API/Auth error during token refresh: {e_auth_api}")
                 self.last_stream_status = StreamConnectionState.ERROR.name 
            except Exception as e_token: 
                self.logger.error(f"DM TokenUpdate: Unexpected error: {e_token}", exc_info=True)
                self.last_stream_status = StreamConnectionState.ERROR.name 
        else:
            self.logger.debug("DM TokenUpdate: APIClient or DataStream (or update_token) not available.")
    
    def get_current_status_summary(self) -> str:
        ds_status_name = "N/A"
        if self.data_stream and hasattr(self.data_stream, 'connection_status'):
            ds_status_name = self.data_stream.connection_status.name
        elif self.last_stream_status: 
            ds_status_name = self.last_stream_status
        dm_streaming_status = "Streaming" if self.is_streaming else "Not Streaming"
        return f"DM: {dm_streaming_status} | DataStream State: '{ds_status_name}'"

    def _map_timeframe_to_api_params(self, timeframe_seconds: int) -> Optional[Tuple[int, int]]:
        if not isinstance(timeframe_seconds, int) or timeframe_seconds < 1:
            self.logger.error(f"DM MapTF: Invalid timeframe_seconds '{timeframe_seconds}'.")
            return None
        
        if timeframe_seconds < 60: return (1, timeframe_seconds)      
        if timeframe_seconds == 60: return (2, 1)     
        if timeframe_seconds == 300: return (2, 5)    
        if timeframe_seconds == 900: return (2, 15)   
        if timeframe_seconds == 1800: return (2, 30)  
        if timeframe_seconds == 3600: return (3, 1)   
        if timeframe_seconds == 14400: return (3, 4)  
        if timeframe_seconds == 86400: return (4, 1)  
        
        if timeframe_seconds % 3600 == 0 : 
            hours = timeframe_seconds // 3600
            if 0 < hours <= 23: return (3, hours) 
        if timeframe_seconds % 60 == 0 : 
            minutes = timeframe_seconds // 60
            if 0 < minutes <= 59: return (2, minutes)
        
        self.logger.error(f"DM MapTF: Could not map timeframe {timeframe_seconds}s.")
        return None
    
    def _create_empty_candles_df(self) -> pd.DataFrame:
        df = pd.DataFrame(columns=list(self.candle_dtypes.keys()))
        for col, dtype_str in self.candle_dtypes.items():
            try:
                df[col] = df[col].astype(dtype_str)
            except Exception as e_astype: 
                self.logger.critical(f"CRITICAL: Error setting initial DF dtype for {col} to {dtype_str}: {e_astype}.")
                if 'datetime' in dtype_str: df[col] = pd.Series(dtype='datetime64[ns, UTC]')
                elif 'float' in dtype_str: df[col] = pd.Series(dtype='float64')
                else: df[col] = pd.Series(dtype='object')
        return df
    
    def _ensure_df_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        res_df = pd.DataFrame(index=df.index) 
        for col, target_dtype_str in self.candle_dtypes.items():
            if col in df.columns:
                current_series = df[col]
                try:
                    if col == 'Time':
                        converted_series = pd.to_datetime(current_series, errors='coerce', utc=True)
                        res_df[col] = pd.Series(converted_series, dtype='datetime64[ns, UTC]')
                    elif target_dtype_str == 'float64': 
                        res_df[col] = pd.to_numeric(current_series, errors='coerce').astype('float64')
                    else: 
                        res_df[col] = current_series.astype(target_dtype_str, errors='ignore')
                except Exception as e_schema:
                    self.logger.error(f"Error ensuring schema for '{col}' to '{target_dtype_str}': {e_schema}.")
                    if col == 'Time': res_df[col] = pd.Series(pd.NaT, index=res_df.index, dtype='datetime64[ns, UTC]')
                    else: res_df[col] = pd.Series(np.nan, index=res_df.index, dtype='float64')
            else: 
                if col == 'Time': res_df[col] = pd.Series(pd.NaT, index=res_df.index, dtype='datetime64[ns, UTC]')
                else: res_df[col] = pd.Series(np.nan, index=res_df.index, dtype='float64')
        return res_df[list(self.candle_dtypes.keys())]

    def _calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates EMA and SMA indicators on the provided DataFrame with detailed logging
        focused on diagnosing MA rendering issues. Includes timing.

        Args:
            df (pd.DataFrame): Input DataFrame with at least a 'Close' column.
                               It's assumed 'Time' column is present for context.

        Returns:
            pd.DataFrame: DataFrame with added EMA and SMA columns. If 'Close' is missing
                          or unsuitable, indicator columns will be filled with NaNs.
        """
        calc_ind_start_time = time.monotonic() # <--- TIMING START

        current_logger = self.logger 
        df_copy = df.copy() # Work on a copy to avoid modifying the original

        ema_col_name = f'EMA{self.EMA_PERIOD}'
        sma_col_name = f'SMA{self.SMA_PERIOD}'
        df_length = len(df_copy)

        current_logger.debug(
            f"DM CalcInd: Received DataFrame with {df_length} rows. "
            f"Targeting EMA({self.EMA_PERIOD}) and SMA({self.SMA_PERIOD})."
        )

        # 1. Validate 'Close' column existence
        if 'Close' not in df_copy.columns:
            current_logger.warning(
                f"DM CalcInd: 'Close' column MISSING in input DataFrame (columns: {df_copy.columns.tolist()}). "
                f"Cannot calculate MAs. Returning DataFrame with NaN indicators."
            )
            df_copy[ema_col_name] = np.nan
            df_copy[sma_col_name] = np.nan
            
            calc_ind_end_time = time.monotonic() # <--- TIMING END (early exit)
            calc_ind_duration_ms = (calc_ind_end_time - calc_ind_start_time) * 1000
            current_logger.debug(
                f"DM CalcInd: Exited early (no 'Close' col). Took={calc_ind_duration_ms:.2f}ms."
            )
            return df_copy
        
        # 2. Convert 'Close' to numeric and check for all NaNs
        close_numeric = pd.to_numeric(df_copy['Close'], errors='coerce')
        
        if close_numeric.isnull().all():
            current_logger.warning(
                f"DM CalcInd: 'Close' column is entirely NaN after to_numeric coercion. "
                f"Cannot calculate MAs. Returning DataFrame with NaN indicators. Original 'Close' sample: {df_copy['Close'].head().tolist()}"
            )
            df_copy[ema_col_name] = np.nan
            df_copy[sma_col_name] = np.nan

            calc_ind_end_time = time.monotonic() # <--- TIMING END (early exit)
            calc_ind_duration_ms = (calc_ind_end_time - calc_ind_start_time) * 1000
            current_logger.debug(
                f"DM CalcInd: Exited early ('Close' all NaN). Took={calc_ind_duration_ms:.2f}ms."
            )
            return df_copy

        # 3. Log detailed stats of the 'Close' series being used
        num_valid_close_prices = close_numeric.notna().sum()
        total_close_prices = len(close_numeric)
        
        log_close_stats_msg = (
            f"DM CalcInd: 'Close' series for MAs - Total: {total_close_prices}, "
            f"Valid (non-NaN): {num_valid_close_prices}. "
        )
        if num_valid_close_prices > 0:
            log_close_stats_msg += (
                f"Min: {close_numeric.dropna().min():.2f}, Max: {close_numeric.dropna().max():.2f}, "
                f"Mean: {close_numeric.dropna().mean():.2f}. "
                f"First 3 valid: {close_numeric.dropna().head(3).round(2).tolist()}, "
                f"Last 3 valid: {close_numeric.dropna().tail(3).round(2).tolist()}."
            )
        else:
            log_close_stats_msg += "No valid (non-NaN) close prices found in the series."
        current_logger.debug(log_close_stats_msg)

        # 4. EMA Calculation
        if self.EMA_PERIOD > 0:
            if num_valid_close_prices >= self.EMA_PERIOD:
                df_copy[ema_col_name] = close_numeric.ewm(
                    span=self.EMA_PERIOD, 
                    adjust=False, 
                    min_periods=self.EMA_PERIOD # Ensures EMA starts only when enough data is present
                ).mean()
                
                ema_valid_count = df_copy[ema_col_name].notna().sum()
                current_logger.debug(
                    f"DM CalcInd: {ema_col_name} calculated. Valid (non-NaN) EMA values: {ema_valid_count}. "
                    f"Last 3 valid EMA: {df_copy[ema_col_name].dropna().tail(3).round(2).tolist() if ema_valid_count > 0 else 'All EMA NaN or no valid points'}"
                )
            else:
                current_logger.debug(
                    f"DM CalcInd: Not enough valid 'Close' data points ({num_valid_close_prices}) "
                    f"for {ema_col_name} (requires {self.EMA_PERIOD}). {ema_col_name} column will be NaN."
                )
                df_copy[ema_col_name] = np.nan
        else:
            current_logger.warning(f"DM CalcInd: EMA_PERIOD is {self.EMA_PERIOD}, which is invalid. {ema_col_name} will be NaN.")
            df_copy[ema_col_name] = np.nan

        # 5. SMA Calculation
        if self.SMA_PERIOD > 0:
            if num_valid_close_prices >= self.SMA_PERIOD:
                df_copy[sma_col_name] = close_numeric.rolling(
                    window=self.SMA_PERIOD, 
                    min_periods=self.SMA_PERIOD # Ensures SMA starts only when enough data
                ).mean()

                sma_valid_count = df_copy[sma_col_name].notna().sum()
                current_logger.debug(
                    f"DM CalcInd: {sma_col_name} calculated. Valid (non-NaN) SMA values: {sma_valid_count}. "
                    f"Last 3 valid SMA: {df_copy[sma_col_name].dropna().tail(3).round(2).tolist() if sma_valid_count > 0 else 'All SMA NaN or no valid points'}"
                )
            else:
                current_logger.debug(
                    f"DM CalcInd: Not enough valid 'Close' data points ({num_valid_close_prices}) "
                    f"for {sma_col_name} (requires {self.SMA_PERIOD}). {sma_col_name} column will be NaN."
                )
                df_copy[sma_col_name] = np.nan
        else:
            current_logger.warning(f"DM CalcInd: SMA_PERIOD is {self.SMA_PERIOD}, which is invalid. {sma_col_name} will be NaN.")
            df_copy[sma_col_name] = np.nan
        
        calc_ind_end_time = time.monotonic() # <--- TIMING END
        calc_ind_duration_ms = (calc_ind_end_time - calc_ind_start_time) * 1000
        current_logger.debug(
            f"DM CalcInd: EMA({self.EMA_PERIOD}), SMA({self.SMA_PERIOD}) on {df_length} rows Took={calc_ind_duration_ms:.2f}ms."
        )
            
        return df_copy
    
    