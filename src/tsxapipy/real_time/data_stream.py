# tsxapipy/real_time/data_stream.py

import logging
import json
import threading
import time
from typing import Dict, Any, Callable, Optional, List

from signalrcore.hub_connection_builder import HubConnectionBuilder
from tsxapipy.config import MARKET_HUB_URL # TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES not used directly here
from tsxapipy.api import APIClient # For type hinting and getting token
from .stream_state import StreamConnectionState

logger = logging.getLogger(__name__)

# Callback type aliases for clarity
QuoteCallback = Callable[[Dict[str, Any]], None]
TradeCallback = Callable[[Dict[str, Any]], None]
DepthCallback = Callable[[Dict[str, Any]], None]
StreamErrorCallback = Callable[[Any], None]
StreamStateChangeCallback = Callable[[str], None] # Argument is state name (str)

class DataStream:
    """
    Handles real-time market data streaming from the TopStepX Market Hub
    using SignalR.
    """
    
    def __init__(
        self, 
        api_client: APIClient, 
        contract_id_to_subscribe: str,
        on_quote_callback: Optional[QuoteCallback] = None,
        on_trade_callback: Optional[TradeCallback] = None,
        on_depth_callback: Optional[DepthCallback] = None,
        on_error_callback: Optional[StreamErrorCallback] = None,
        on_state_change_callback: Optional[StreamStateChangeCallback] = None,
        auto_subscribe_quotes: bool = True,
        auto_subscribe_trades: bool = True,
        auto_subscribe_depth: bool = False
    ):
        self.logger = logger 
        self.api_client = api_client
        self.contract_id = contract_id_to_subscribe
        
        self._callbacks = {
            "quote": on_quote_callback,
            "trade": on_trade_callback,
            "depth": on_depth_callback,
        }
        self.on_error_callback = on_error_callback
        self.on_state_change_callback = on_state_change_callback

        self.auto_subscribe_quotes = auto_subscribe_quotes
        self.auto_subscribe_trades = auto_subscribe_trades
        self.auto_subscribe_depth = auto_subscribe_depth

        self.connection: Optional[HubConnectionBuilder] = None
        self.connection_status = StreamConnectionState.NOT_INITIALIZED
        
        self._current_token_for_auth: Optional[str] = None # Stores the token for header/query
        self._wss_hub_url_no_token: str = "" # Base WSS URL without token
        
        self._connection_lock = threading.RLock() 
        self._is_manually_stopping = False 

        self._prepare_websocket_url_base()
        try:
            self._refresh_auth_token() # Get initial token
        except Exception as e:
            self.logger.error(f"DataStream (Contract: {self.contract_id}): Failed to get initial token during __init__: {e}")
            self._set_connection_state(StreamConnectionState.ERROR, f"Initial token fail: {type(e).__name__}")
        
        self._build_connection() # Build connection object using the refreshed token
        self.logger.info(f"DataStream initialized for contract: {self.contract_id}")

    def _prepare_websocket_url_base(self):
        """Prepares the base WebSocket URL (wss://...) from MARKET_HUB_URL."""
        if MARKET_HUB_URL.startswith("https://"):
            self._wss_hub_url_no_token = "wss://" + MARKET_HUB_URL[len("https://"):]
        elif MARKET_HUB_URL.startswith("http://"):
            self._wss_hub_url_no_token = "ws://" + MARKET_HUB_URL[len("http://"):]
        else:
            self.logger.warning(f"MARKET_HUB_URL '{MARKET_HUB_URL}' does not start with http(s). Using as is for wss base.")
            self._wss_hub_url_no_token = MARKET_HUB_URL
        self.logger.debug(f"DataStream (Contract: {self.contract_id}): Prepared base WebSocket URL: {self._wss_hub_url_no_token}")

    def _refresh_auth_token(self):
        """Fetches/refreshes token from APIClient and stores it in _current_token_for_auth."""
        self.logger.debug(f"DataStream (Contract: {self.contract_id}): Refreshing auth token.")
        if not self.api_client:
            raise ConnectionError("DataStream: APIClient not available for token refresh.")
        
        self._current_token_for_auth = self.api_client.current_token 
        if not self._current_token_for_auth:
            raise ConnectionError("DataStream: Failed to obtain a valid token from APIClient.")
        self.logger.debug(f"DataStream (Contract: {self.contract_id}): Auth token refreshed/obtained.")

    def _build_connection(self):
        """Builds or rebuilds the SignalR connection object, attempting token in headers."""
        if not self._current_token_for_auth:
            self.logger.error(f"DataStream (Contract: {self.contract_id}): Cannot build connection, current token is missing.")
            # Attempt to refresh token if missing before failing build
            try:
                self._refresh_auth_token()
            except ConnectionError as e:
                self.logger.error(f"DataStream (Contract: {self.contract_id}): Failed to refresh token during _build_connection: {e}")
                return # Cannot proceed without a token
                
        if not self._wss_hub_url_no_token:
            self.logger.error(f"DataStream (Contract: {self.contract_id}): Cannot build connection, base WSS URL is not set.")
            if not self._wss_hub_url_no_token: self._prepare_websocket_url_base() # Try to prepare it
            if not self._wss_hub_url_no_token: return # Still not set, cannot proceed

        self.logger.debug(f"DataStream (Contract: {self.contract_id}): Building new SignalR connection using token in headers.")
        
        self.connection = HubConnectionBuilder() \
            .with_url(self._wss_hub_url_no_token, options={ # Use URL without token query param
                "verify_ssl": True, # Standard security, set to False for local/dev self-signed certs if needed
                "skip_negotiation": False, # Default, allows negotiation phase where headers are typically sent
                "headers": {
                    "Authorization": f"Bearer {self._current_token_for_auth}"
                }
            }) \
            .with_automatic_reconnect({
                "type": "interval",
                "keep_alive_interval": 10,
                "intervals": [0, 2, 5, 10, 15, 30, 60, 120] # Retry intervals in seconds
            }) \
            .build()

        # Standard SignalR lifecycle events
        self.connection.on_open(self._on_open)
        self.connection.on_close(self._on_close)
        self.connection.on_error(self._on_signalr_error) # For transport/protocol level errors
        
        # Hub-specific message handlers
        if self._callbacks["quote"]:
            self.connection.on("GatewayQuote", self._handle_quote_message)
        if self._callbacks["trade"]:
            self.connection.on("GatewayTrade", self._handle_trade_message)
        if self._callbacks["depth"]:
            self.connection.on("GatewayDepth", self._handle_depth_message)
        
        self.logger.debug(f"DataStream (Contract: {self.contract_id}): SignalR event handlers registered.")

    def _set_connection_state(self, new_state: StreamConnectionState, reason: Optional[str] = None):
        """Safely updates and logs connection state, invoking callback if changed."""
        with self._connection_lock:
            if self.connection_status != new_state:
                old_state_name = self.connection_status.name
                self.connection_status = new_state
                log_msg = f"DataStream (Contract: {self.contract_id}): State changed from {old_state_name} to {new_state.name}."
                if reason: log_msg += f" Reason: {reason}"
                self.logger.info(log_msg)
                if self.on_state_change_callback:
                    try:
                        self.on_state_change_callback(new_state.name)
                    except Exception as e_cb:
                        self.logger.error(f"DataStream (Contract: {self.contract_id}): Error in on_state_change_callback: {e_cb}", exc_info=True)
    
    def _on_open(self):
        """Callback for when the SignalR connection successfully opens."""
        self._set_connection_state(StreamConnectionState.CONNECTED, "SignalR connection opened")
        self._send_subscriptions()

    def _on_close(self):
        """Callback for when the SignalR connection closes."""
        if not self._is_manually_stopping: 
            self.logger.warning(f"DataStream (Contract: {self.contract_id}): SignalR connection closed unexpectedly. Current state: {self.connection_status.name}.")
            self._set_connection_state(StreamConnectionState.DISCONNECTED, "Connection closed (possibly after failed retries or server disconnect)")
        else: # Manual stop
             self._set_connection_state(StreamConnectionState.DISCONNECTED, "Connection stopped by client")

    def _on_signalr_error(self, error_payload: Any):
        """Callback for fundamental SignalR connection errors (not hub invocation errors)."""
        error_message = str(error_payload) if error_payload else "Unknown SignalR transport error"
        self.logger.error(f"DataStream (Contract: {self.contract_id}): Underlying SignalR connection error: {error_message}", exc_info=isinstance(error_payload, Exception))
        self._set_connection_state(StreamConnectionState.ERROR, f"SignalR transport error: {error_message[:100]}")
        if self.on_error_callback:
            try:
                self.on_error_callback(error_payload) # Pass original error
            except Exception as e_cb:
                self.logger.error(f"DataStream (Contract: {self.contract_id}): Error in on_error_callback: {e_cb}", exc_info=True)

    def _parse_payload(self, args: List[Any], event_name: str) -> Optional[Dict[str, Any]]:
        """Helper to parse common SignalR list-wrapped dictionary payloads."""
        if isinstance(args, list) and len(args) > 0 and isinstance(args[0], dict):
            return args[0]
        self.logger.warning(f"DataStream (Contract: {self.contract_id}): Received {event_name} payload with unexpected structure: {args}")
        return None

    def _handle_quote_message(self, args: List[Any]):
        payload = self._parse_payload(args, "GatewayQuote")
        if payload and self._callbacks["quote"]:
            try: self._callbacks["quote"](payload)
            except Exception as e: self.logger.error(f"DataStream (Contract: {self.contract_id}): Error in on_quote_callback: {e}", exc_info=True)

    def _handle_trade_message(self, args: List[Any]):
        payload = self._parse_payload(args, "GatewayTrade")
        if payload and self._callbacks["trade"]:
            try: self._callbacks["trade"](payload)
            except Exception as e: self.logger.error(f"DataStream (Contract: {self.contract_id}): Error in on_trade_callback: {e}", exc_info=True)

    def _handle_depth_message(self, args: List[Any]):
        payload = self._parse_payload(args, "GatewayDepth")
        if payload and self._callbacks["depth"]:
            try: self._callbacks["depth"](payload)
            except Exception as e: self.logger.error(f"DataStream (Contract: {self.contract_id}): Error in on_depth_callback: {e}", exc_info=True)

    def _send_subscriptions(self):
        """Sends market data subscriptions to the Market Hub."""
        if not self.connection or self.connection_status != StreamConnectionState.CONNECTED:
            self.logger.warning(f"DataStream (Contract: {self.contract_id}): Cannot send subscriptions, not in CONNECTED state (current: {self.connection_status.name}).")
            return
        try:
            self.logger.info(f"DataStream (Contract: {self.contract_id}): Sending subscriptions...")
            if self.auto_subscribe_quotes and self._callbacks["quote"]:
                self.logger.info(f"DataStream: Subscribing to Quotes for {self.contract_id}")
                self.connection.send("SubscribeToQuotes", [self.contract_id])
                time.sleep(0.1) 
            if self.auto_subscribe_trades and self._callbacks["trade"]:
                self.logger.info(f"DataStream: Subscribing to Trades for {self.contract_id}")
                self.connection.send("SubscribeToTrades", [self.contract_id])
                time.sleep(0.1)
            if self.auto_subscribe_depth and self._callbacks["depth"]:
                self.logger.info(f"DataStream: Subscribing to Depth for {self.contract_id}")
                self.connection.send("SubscribeToDepth", [self.contract_id])
            self.logger.info(f"DataStream (Contract: {self.contract_id}): Subscription messages sent.")
        except Exception as e: 
            self.logger.error(f"DataStream (Contract: {self.contract_id}): Error sending subscriptions: {e}", exc_info=True)
            self._set_connection_state(StreamConnectionState.ERROR, f"Subscription send error: {type(e).__name__}")
    
    def start(self) -> bool:
        """Starts the SignalR connection to the Market Hub."""
        with self._connection_lock:
            self.logger.info(f"DataStream (Contract: {self.contract_id}): Start called. Current state: {self.connection_status.name}")
            self._is_manually_stopping = False 

            if self.connection_status == StreamConnectionState.CONNECTED:
                self.logger.warning(f"DataStream (Contract: {self.contract_id}): Already CONNECTED.")
                return True
            if self.connection_status == StreamConnectionState.CONNECTING:
                self.logger.warning(f"DataStream (Contract: {self.contract_id}): Already CONNECTING.")
                return True 
            
            # Ensure token and connection object are ready
            if not self.connection or not self._current_token_for_auth:
                self.logger.info(f"DataStream (Contract: {self.contract_id}): Connection or token not ready, re-initializing/building.")
                try:
                    if not self._wss_hub_url_no_token: self._prepare_websocket_url_base()
                    self._refresh_auth_token() # Ensure token is fresh
                    self._build_connection()   # Rebuild connection with potentially new token in headers
                except Exception as e_prep: 
                    self.logger.error(f"DataStream (Contract: {self.contract_id}): Prep failed in start(): {e_prep}", exc_info=True)
                    self._set_connection_state(StreamConnectionState.ERROR, f"Prep failed in start: {type(e_prep).__name__}")
                    return False
            
            if not self.connection: # Check again after build attempt
                self.logger.error(f"DataStream (Contract: {self.contract_id}): Connection object is None after build. Cannot start.")
                self._set_connection_state(StreamConnectionState.ERROR, "Connection object None post-build")
                return False
            
            self._set_connection_state(StreamConnectionState.CONNECTING, "Start method initiated connection attempt")
            try:
                self.connection.start() 
                self.logger.info(f"DataStream (Contract: {self.contract_id}): SignalR connection.start() called. Waiting for _on_open or _on_error.")
                return True # Indicates start was attempted. Actual connection is async.
            except Exception as e_conn_start: 
                self.logger.error(f"DataStream (Contract: {self.contract_id}): Exception calling connection.start(): {e_conn_start}", exc_info=True)
                self._set_connection_state(StreamConnectionState.ERROR, f"Exception during connection.start(): {str(e_conn_start)[:50]}")
                return False

    def stop(self, reason_for_stop: Optional[str] = "User requested stop"):
        """Stops the SignalR connection."""
        with self._connection_lock:
            self.logger.info(f"DataStream (Contract: {self.contract_id}): Stop called. Reason: '{reason_for_stop}'. Current state: {self.connection_status.name}")
            self._is_manually_stopping = True 

            if self.connection_status == StreamConnectionState.DISCONNECTED:
                self.logger.info(f"DataStream (Contract: {self.contract_id}): Already DISCONNECTED.")
                return

            self._set_connection_state(StreamConnectionState.STOPPING, reason_for_stop)
            
            if self.connection:
                try: 
                    self.connection.stop() # This should trigger _on_close eventually
                except Exception as e_conn_stop:
                    self.logger.error(f"DataStream (Contract: {self.contract_id}): Exception during connection.stop(): {e_conn_stop}", exc_info=True)
                    self._set_connection_state(StreamConnectionState.ERROR, f"Exception during stop: {str(e_conn_stop)[:50]}")
            else:
                self.logger.info(f"DataStream (Contract: {self.contract_id}): No connection object to stop. Setting state to DISCONNECTED.")
                self._set_connection_state(StreamConnectionState.DISCONNECTED, "No connection object was present")

    def update_token(self, new_token: str):
        """
        Updates the authentication token and restarts the SignalR connection.
        The new token is used in the headers for the new connection.
        """
        with self._connection_lock:
            if not new_token:
                self.logger.error(f"DataStream (Contract: {self.contract_id}): Attempted to update token with an empty new token. Ignoring.")
                return
            
            self.logger.info(f"DataStream (Contract: {self.contract_id}): Token update requested.")

            if new_token == self._current_token_for_auth and self.connection_status == StreamConnectionState.CONNECTED:
                self.logger.debug(f"DataStream (Contract: {self.contract_id}): New token is same as current, and stream connected. Forcing APIClient token re-validation.")
                try:
                    # Accessing current_token on APIClient might trigger its internal validation/refresh if token is nearing expiry.
                    # This DataStream doesn't need to restart if the token string itself didn't change.
                    _ = self.api_client.current_token 
                except Exception as e_access_token:
                    self.logger.warning(f"DataStream (Contract: {self.contract_id}): Error accessing current_token from APIClient during same-token update: {e_access_token}")
                return

            self.logger.info(f"DataStream (Contract: {self.contract_id}): Proceeding with connection restart for token update.")
            
            original_manual_stop_state = self._is_manually_stopping
            
            if self.connection_status not in [StreamConnectionState.NOT_INITIALIZED, StreamConnectionState.DISCONNECTED, StreamConnectionState.ERROR]:
                self.logger.info(f"DataStream (Contract: {self.contract_id}): Stopping existing connection (state: {self.connection_status.name}) before token update.")
                self.stop(reason_for_stop="Token update initiated stop")
                # It's crucial that stop() is effective and blocking or we wait for state change
                # to DISCONNECTED before proceeding. A simple sleep is a pragmatic approach for now.
                time.sleep(1.5) # Increased sleep to allow stop and _on_close to process

            self._is_manually_stopping = original_manual_stop_state
            
            try:
                self._current_token_for_auth = new_token # Store the new token
                
                self.logger.info(f"DataStream (Contract: {self.contract_id}): Rebuilding connection with new token in headers...")
                self._build_connection() # Rebuilds connection (which will use new _current_token_for_auth in headers)
                
                self.logger.info(f"DataStream (Contract: {self.contract_id}): Attempting to start connection after token update.")
                if not self.start(): 
                    self.logger.error(f"DataStream (Contract: {self.contract_id}): Failed to restart stream after token update.")
                # else: connection state will be updated by start() and its _on_open callback
            except Exception as e_rebuild:
                self.logger.error(f"DataStream (Contract: {self.contract_id}): Error rebuilding/restarting connection after token update: {e_rebuild}", exc_info=True)
                self._set_connection_state(StreamConnectionState.ERROR, f"Token update rebuild/restart fail: {type(e_rebuild).__name__}")