# tsxapipy/real_time/data_stream.py
import logging
import time
from enum import Enum, auto
from typing import Callable, Optional, Any, List, Dict

from signalrcore.hub_connection_builder import HubConnectionBuilder 

from tsxapipy.config import MARKET_HUB_URL
from tsxapipy.api import APIClient 

logger = logging.getLogger(__name__)

# Callback type aliases for better type hinting and clarity
QuoteCallback = Callable[[Dict[str, Any]], None]
"""Callback signature for quote updates. Receives a dictionary of quote data."""
TradeCallback = Callable[[Dict[str, Any]], None] # Or List[Dict[str,Any]] if API can send batches
"""Callback signature for trade updates. Receives a dictionary (or list of dicts) of trade data."""
DepthCallback = Callable[[Dict[str, Any]], None]
"""Callback signature for market depth updates. Receives a dictionary of depth data."""
StreamErrorCallback = Callable[[Any], None] 
"""Callback signature for stream errors. Receives error details."""
StreamStateChangeCallback = Callable[[str], None] 
"""Callback signature for stream state changes. Receives a string describing the new state."""


class StreamConnectionState(Enum):
    """Enumerates the possible states of the stream connection."""
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    RECONNECTING_TOKEN = auto() # Specifically for token update cycle
    RECONNECTING_UNEXPECTED = auto() # For automatic retries by signalrcore after unexpected disconnect
    STOPPING = auto()
    ERROR = auto() # A terminal error state if connection cannot be established/recovered

class DataStream:
    """
    Manages a SignalR connection to the TopStep Market Hub for real-time market data.

    This class handles connecting (using `skip_negotiation: True`), subscribing to market events
    (quotes, trades, depth) for a specified contract, and invoking user-provided callbacks.
    It requires an `APIClient` instance to obtain the initial authentication token.
    The token is embedded directly into the WebSocket connection URL.

    Token Refresh:
    For long-lived connections, if the API session token (managed by `APIClient`) is
    refreshed, the application is responsible for calling the `update_token()` method
    on this `DataStream` instance. This method will:
    1. Transition the stream to a `RECONNECTING_TOKEN` state.
    2. Stop the current SignalR connection.
    3. Reconfigure the WebSocket URL with the new token.
    4. Restart the SignalR connection.

    The underlying `signalrcore` library is configured for automatic reconnection attempts,
    which will use the last known valid URL (with its token) until `update_token()`
    provides a new one. Connection state changes are managed internally and exposed
    via the `connection_status` property and `on_state_change_callback`.

    Attributes:
        api_client (APIClient): The API client instance used for obtaining tokens.
        contract_id (str): The contract ID for which to subscribe to market data.
        connection (Optional[Any]): The SignalR hub connection object from `signalrcore`.
                                    Typically `HubConnection` or similar.
        connection_status (StreamConnectionState): Read-only property describing the current state of the connection.
        token (Optional[str]): Read-only property for the current token used in the connection URL.
    """
    def __init__(self,
                 api_client: APIClient,
                 contract_id_to_subscribe: str,
                 on_quote_callback: Optional[QuoteCallback] = None,
                 on_trade_callback: Optional[TradeCallback] = None, 
                 on_depth_callback: Optional[DepthCallback] = None,
                 on_error_callback: Optional[StreamErrorCallback] = None,
                 on_state_change_callback: Optional[StreamStateChangeCallback] = None):
        """
        Initializes the DataStream.

        Args:
            api_client (APIClient): An initialized `APIClient` instance.
            contract_id_to_subscribe (str): The contract ID (e.g., "CON.F.US.NQ.H24")
                                            for which to stream market data.
            on_quote_callback (Optional[QuoteCallback]): Callback for quote updates.
            on_trade_callback (Optional[TradeCallback]): Callback for trade updates.
            on_depth_callback (Optional[DepthCallback]): Callback for market depth updates.
            on_error_callback (Optional[StreamErrorCallback]): Callback for stream errors.
            on_state_change_callback (Optional[StreamStateChangeCallback]): Callback for
                                                                          stream state changes.

        Raises:
            TypeError: If `api_client` is not an `APIClient` instance.
            ValueError: If `contract_id_to_subscribe` is invalid.
            RuntimeError: If initial token fetch or URL preparation fails critically.
        """
        if not isinstance(api_client, APIClient):
            raise TypeError("api_client must be an instance of APIClient.")
        self.api_client = api_client

        if not contract_id_to_subscribe or not isinstance(contract_id_to_subscribe, str):
            raise ValueError("Contract ID to subscribe must be a non-empty string for DataStream.")
        
        self.contract_id: str = contract_id_to_subscribe
        self._callbacks: Dict[str, Optional[Callable]] = {
            "quote": on_quote_callback,
            "trade": on_trade_callback, 
            "depth": on_depth_callback,
            "error": on_error_callback,
            "state_change": on_state_change_callback
        }
        self.connection: Optional[Any] = None # Will be HubConnection type from signalrcore
        self._http_hub_base_url: str = MARKET_HUB_URL 
        self._wss_hub_url_no_token: Optional[str] = None 
        self._current_token_for_url: Optional[str] = None 
        self._hub_url_with_token: Optional[str] = None 

        self._current_state: StreamConnectionState = StreamConnectionState.DISCONNECTED # Initial state
        
        logger.info(f"DataStream initialized for Contract: {self.contract_id}.")
        self._prepare_websocket_url_base() # Derive ws/wss URL from http/https
        try:
            self._reinitialize_token_and_url() # Get initial token and form full URL
        except Exception as e: # Catches errors from APIClient.current_token or URL prep
            logger.error(f"DataStream ({self.contract_id}): Failed to get initial token or prepare URL during __init__: {e}", exc_info=True)
            self._set_connection_state(StreamConnectionState.ERROR, f"Initial setup failed: {type(e).__name__}")
            # Do not build connection if URL/token failed, allow start() to retry
            return 
            
        self._build_connection() # Setup SignalR connection object

    @property
    def connection_status(self) -> StreamConnectionState:
        """Current status of the SignalR connection (e.g., 'connected', 'disconnected')."""
        return self._current_state
        
    @property
    def token(self) -> Optional[str]: 
        """The current authentication token being used for the connection URL."""
        return self._current_token_for_url

    def _set_connection_state(self, new_state: StreamConnectionState, reason: Optional[str] = None):
        """Internal method to update and log connection state changes."""
        if self._current_state != new_state:
            old_state_name = self._current_state.name
            self._current_state = new_state
            new_state_name = new_state.name
            log_msg = f"DataStream ({self.contract_id}): Connection state changed from '{old_state_name}' to '{new_state_name}'."
            if reason:
                log_msg += f" Reason: {reason}"
            logger.info(log_msg) # Log state changes at INFO level for better visibility
            if self._callbacks["state_change"]:
                try:
                    self._callbacks["state_change"](new_state_name) # Pass string name of enum state
                except Exception as e_cb_state: # pylint: disable=broad-except
                    logger.error(f"DataStream ({self.contract_id}): Error in on_state_change_callback for state '{new_state_name}': {e_cb_state}", exc_info=True)

    def _prepare_websocket_url_base(self):
        """Derives the base WebSocket URL (ws:// or wss://) from the HTTP/HTTPS hub URL."""
        if not self._http_hub_base_url or "://" not in self._http_hub_base_url:
            logger.error(f"DataStream ({self.contract_id}): Invalid http_hub_base_url '{self._http_hub_base_url}'. Cannot derive WebSocket URL base.")
            self._wss_hub_url_no_token = None
            return

        try:
            scheme, rest_of_url = self._http_hub_base_url.split("://", 1)
            if scheme.lower() == "http":
                self._wss_hub_url_no_token = f"ws://{rest_of_url}"
            elif scheme.lower() == "https":
                self._wss_hub_url_no_token = f"wss://{rest_of_url}"
            else: # If already a ws/wss URL or unknown scheme
                self._wss_hub_url_no_token = self._http_hub_base_url # Use as is
                if not (scheme.lower().startswith("ws")): # Log if not explicitly ws/wss
                     logger.warning(f"DataStream ({self.contract_id}): Unexpected scheme '{scheme}' in hub URL. Using URL directly for WebSocket: {self._wss_hub_url_no_token}")
        except ValueError: # If split fails
            logger.error(f"DataStream ({self.contract_id}): Malformed http_hub_base_url '{self._http_hub_base_url}'. Cannot derive WebSocket URL base.")
            self._wss_hub_url_no_token = None
        
        if self._wss_hub_url_no_token:
            logger.debug(f"DataStream ({self.contract_id}): Prepared WebSocket base URL: {self._wss_hub_url_no_token}")

    def _reinitialize_token_and_url(self):
        """Fetches the current token from APIClient and constructs the full WebSocket URL."""
        logger.debug(f"DataStream ({self.contract_id}): Re-initializing token and WebSocket URL.")
        if not self.api_client:
            raise RuntimeError(f"DataStream ({self.contract_id}): APIClient instance not available for token re-initialization.")
        if not self._wss_hub_url_no_token: # Ensure base WebSocket URL is ready
            self._prepare_websocket_url_base() 
            if not self._wss_hub_url_no_token: # If still not ready after attempt
                 raise RuntimeError(f"DataStream ({self.contract_id}): Base WebSocket URL could not be prepared. Cannot form connection URL.")

        try:
            # This call to api_client.current_token might trigger re-auth if token is stale
            self._current_token_for_url = self.api_client.current_token 
            self._hub_url_with_token = f"{self._wss_hub_url_no_token}?access_token={self._current_token_for_url}"
            log_token_preview = f"{self._current_token_for_url[:15]}..." if self._current_token_for_url and len(self._current_token_for_url) > 15 else "Token (short or None)"
            logger.info(f"DataStream ({self.contract_id}): Token obtained/refreshed. Full WebSocket URL configured (token preview: {log_token_preview}).")
        except Exception as e_token_fetch: # Catch errors from api_client.current_token
            logger.error(f"DataStream ({self.contract_id}): Failed to get token from APIClient for WebSocket URL construction: {e_token_fetch}", exc_info=True)
            self._hub_url_with_token = None # Ensure URL is None if token fetch fails
            raise # Re-raise to signal failure to the caller

    def _build_connection(self):
        """Builds or rebuilds the SignalR connection object with current URL and handlers."""
        logger.debug(f"DataStream ({self.contract_id}): Building new SignalR connection object.")
        
        if not self._hub_url_with_token: 
            logger.critical(f"DataStream ({self.contract_id}): Full WebSocket Hub URL (with token) is not prepared. Cannot build SignalR connection.")
            self._set_connection_state(StreamConnectionState.ERROR, "Hub URL with token not ready for build")
            return

        logger.info(f"DataStream ({self.contract_id}): Configuring SignalR connection with skip_negotiation=True.")
        
        # Prepare User-Agent header for the WebSocket connection
        user_agent_header_val = "tsxapipy-DataStream/unknown-version" 
        if self.api_client and self.api_client.session and self.api_client.session.headers:
            base_user_agent = self.api_client.session.headers.get('User-Agent', 'tsxapipy-unknown-version')
            user_agent_header_val = f"tsxapipy-DataStream/{base_user_agent}" # Prepend stream identifier

        connection_options = {
            "headers": {"User-Agent": user_agent_header_val},
            "skip_negotiation": True # Crucial for token in URL method
        }
        
        self.connection = HubConnectionBuilder() \
            .with_url(self._hub_url_with_token, options=connection_options) \
            .with_automatic_reconnect({ # Configure retry behavior
                "type": "interval", 
                "keep_alive_interval": 10, # Seconds
                "intervals": [0, 2000, 5000, 10000, 15000, 30000, 60000, 120000, 300000] # Milliseconds
            }) \
            .build()
        
        # Register core SignalR lifecycle event handlers
        self.connection.on_open(self._on_open)
        self.connection.on_close(self._on_close)
        self.connection.on_error(self._on_error)

        # Register handlers for specific market data messages from the hub
        if self._callbacks["quote"]: self.connection.on("GatewayQuote", self._handle_gateway_quote_event)
        if self._callbacks["trade"]: self.connection.on("GatewayTrade", self._handle_gateway_trade_event)
        if self._callbacks["depth"]: self.connection.on("GatewayDepth", self._handle_gateway_depth_event)
        logger.debug(f"DataStream ({self.contract_id}): SignalR connection object built and event handlers registered.")

    def _on_open(self):
        """Internal callback invoked by signalrcore when connection is successfully opened."""
        conn_id_str = self.connection.connection_id if self.connection and hasattr(self.connection, 'connection_id') else "N/A"
        logger.info(f"DataStream ({self.contract_id}) CONNECTED to Market Hub (Client Connection ID: {conn_id_str}).")
        self._set_connection_state(StreamConnectionState.CONNECTED)
        self._send_subscriptions() # Send subscriptions now that connection is open

    def _on_close(self):
        """Internal callback invoked by signalrcore when connection is closed."""
        log_message = (
            f"DataStream ({self.contract_id}): SignalR connection _on_close CALLED. "
            f"Current state before this event: {self._current_state.name}"
        )
        logger.info(log_message)

        if self._current_state == StreamConnectionState.STOPPING: # Explicit stop by user or token update
            self._set_connection_state(StreamConnectionState.DISCONNECTED, "Connection intentionally closed")
        elif self._current_state == StreamConnectionState.CONNECTED or self._current_state == StreamConnectionState.CONNECTING:
            # Unexpected close if not in STOPPING or RECONNECTING_TOKEN state
            self._set_connection_state(StreamConnectionState.RECONNECTING_UNEXPECTED, "Connection closed unexpectedly; auto-reconnect will attempt")
        # If already RECONNECTING_TOKEN or RECONNECTING_UNEXPECTED, let signalrcore manage the retry attempts.
        # If ERROR, it's a terminal state.
        # If DISCONNECTED, it's already handled.

    def _on_error(self, error: Any):
        """Internal callback invoked by signalrcore on connection errors."""
        log_message = (
            f"DataStream ({self.contract_id}): SignalR connection _on_error CALLED. Error: '{error}', Type: {type(error)}. "
            f"Current state: {self._current_state.name}"
        )
        logger.error(log_message) # Errors are typically significant
             
        if self._callbacks["error"]: # Invoke user's error callback
            try: self._callbacks["error"](error)
            except Exception as e_cb_err: # pylint: disable=broad-except
                logger.error(f"DataStream ({self.contract_id}): Exception in user's on_error_callback: {e_cb_err}", exc_info=True)
        
        # If the error occurs during an active state or connection attempt,
        # and not during a controlled stop, transition to a reconnecting state.
        # signalrcore's `with_automatic_reconnect` should handle the actual retry logic.
        if self._current_state not in [StreamConnectionState.STOPPING, StreamConnectionState.DISCONNECTED, StreamConnectionState.ERROR]:
             self._set_connection_state(StreamConnectionState.RECONNECTING_UNEXPECTED, f"Stream error occurred: {str(error)[:50]}")


    def update_token(self, new_token_from_api_client: str):
        """
        Updates the authentication token used for the SignalR connection.

        This method stops the current connection, rebuilds it with the new token
        embedded in the URL, and then restarts the connection. This is necessary
        for long-lived streams where the session token might expire.

        Args:
            new_token_from_api_client (str): The new, valid session token obtained
                                             from the `APIClient` instance.
        """
        logger.info(f"DataStream ({self.contract_id}): update_token called. Current state: {self.connection_status.name}. "
                    f"New token (first 15 chars): {new_token_from_api_client[:15] if new_token_from_api_client else 'None'}...")
        
        if not new_token_from_api_client:
            logger.error(f"DataStream ({self.contract_id}): update_token called with an empty new token. Ignoring request."); return
        
        # Optimization: If token is the same and already connected, no need to restart.
        if new_token_from_api_client == self._current_token_for_url and self.connection_status == StreamConnectionState.CONNECTED:
            logger.debug(f"DataStream ({self.contract_id}): update_token called with the current token while stream is connected. No forced restart needed.")
            return

        original_state_before_token_update = self._current_state
        self._set_connection_state(StreamConnectionState.RECONNECTING_TOKEN, "Token update procedure initiated")
        
        logger.info(f"DataStream ({self.contract_id}): Token update in progress. Stream will be stopped, reconfigured, and restarted.")
        
        # Stop existing connection first if it's active
        if self.connection and original_state_before_token_update not in [StreamConnectionState.DISCONNECTED, StreamConnectionState.ERROR]:
            logger.debug(f"DataStream ({self.contract_id}): Stopping current connection (state: {original_state_before_token_update.name}) as part of token update.")
            self.stop(reason_for_stop="Token update procedure") # stop will set state to STOPPING then DISCONNECTED
            # Short pause to allow underlying stop to process, especially if signalrcore stop() is async.
            # This is heuristic. Robust solution might involve waiting for _on_close confirmation.
            time.sleep(0.5) 
        
        try:
            # Update token and URL
            self._current_token_for_url = new_token_from_api_client 
            if not self._wss_hub_url_no_token: self._prepare_websocket_url_base() # Ensure base URL is ready
            if self._wss_hub_url_no_token: 
                self._hub_url_with_token = f"{self._wss_hub_url_no_token}?access_token={self._current_token_for_url}"
                logger.info(f"DataStream ({self.contract_id}): New WebSocket URL with updated token configured for restart.")
            else: # Should not happen if _prepare_websocket_url_base is robust
                logger.error(f"DataStream ({self.contract_id}): Critical error - cannot update WebSocket URL as base WSS URL is not set. Token update failed.")
                self._set_connection_state(StreamConnectionState.ERROR, "Base WSS URL not set during token update")
                return
            
            # Rebuild and restart the connection
            self._build_connection() # Rebuilds self.connection with the new URL
            if not self.start(): # start() will transition to CONNECTING, then CONNECTED or ERROR
                logger.error(f"DataStream ({self.contract_id}): Failed to restart stream after token update procedure.")
                # If start() itself failed, it should set an appropriate error state.
                if self._current_state not in [StreamConnectionState.CONNECTING, StreamConnectionState.ERROR]:
                     self._set_connection_state(StreamConnectionState.ERROR, "Failed to initiate start after token update")
            # If start() returns True, state becomes CONNECTING. _on_open will set CONNECTED.
        except Exception as e_token_update_seq: # Catch any error during the update sequence
            logger.error(f"DataStream ({self.contract_id}): Exception during token update and stream restart sequence: {e_token_update_seq}", exc_info=True)
            self._set_connection_state(StreamConnectionState.ERROR, f"Exception during token update sequence: {type(e_token_update_seq).__name__}")

    def _parse_market_payload(self, args: List[Any], event_name: str) -> Optional[Dict[str, Any]]:
        """
        Internal helper to parse common SignalR list-wrapped dictionary payloads for Market Hub.
        Market Hub events like GatewayQuote often come as `[contract_id_str, payload_dict]`.
        """
        logger.debug(f"DataStream ({self.contract_id}): Raw {event_name} event args received: {args}")
        payload_data: Optional[Dict[str, Any]] = None
        if isinstance(args, list) and len(args) > 0:
            if len(args) == 2 and isinstance(args[0], str) and isinstance(args[1], dict):
                # Expected structure: [contract_id_string, data_dictionary]
                event_contract_id, payload_data = args[0], args[1]
                if event_contract_id != self.contract_id:
                    logger.debug(f"DataStream ({self.contract_id}): Received {event_name} for a different contract_id ('{event_contract_id}'). Ignoring.")
                    return None
            elif len(args) == 1 and isinstance(args[0], dict):
                # If payload comes directly as a single dictionary in the list
                payload_data = args[0]
            else: # Unexpected list structure
                 logger.warning(f"DataStream ({self.contract_id}): {event_name} args list has an unexpected structure: {args}")
        elif isinstance(args, dict): # If payload comes directly as a dict (less common for these events)
            payload_data = args
        
        if payload_data is not None:
            return payload_data
        
        logger.warning(f"DataStream ({self.contract_id}): Received {event_name} with an unhandled argument structure: {args}")
        return None

    def _handle_gateway_quote_event(self, args: List[Any]):
        """Internal handler for "GatewayQuote" messages."""
        payload = self._parse_market_payload(args, "GatewayQuote")
        if payload and self._callbacks["quote"]:
            try:
                self._callbacks["quote"](payload)
            except Exception as e_cb_quote: # pylint: disable=broad-except
                logger.error(f"DataStream ({self.contract_id}): Unhandled error in on_quote_callback: {e_cb_quote}", exc_info=True)

    def _handle_gateway_trade_event(self, args: List[Any]):
        """
        Internal handler for "GatewayTrade" messages.
        GatewayTrade payloads might be a list of trades: `[contract_id_str, [trade_dict1, trade_dict2]]`
        or a single trade: `[contract_id_str, trade_dict_single]`.
        This handler iterates if a list of trades is received.
        """
        logger.debug(f"DataStream ({self.contract_id}): Raw GatewayTrade event args received: {args}")
        if not self._callbacks["trade"]: return # No callback registered

        actual_trade_payloads_to_process: List[Dict[str, Any]] = []
        
        if isinstance(args, list):
            if len(args) == 2 and isinstance(args[0], str): # Potential [contract_id, data_part]
                event_contract_id = args[0]
                if event_contract_id == self.contract_id: # Match contract ID
                    data_part = args[1]
                    if isinstance(data_part, list): # List of trades [trade1, trade2, ...]
                        for item in data_part: 
                            if isinstance(item, dict): actual_trade_payloads_to_process.append(item)
                            else: logger.warning(f"DataStream ({self.contract_id}): GatewayTrade - item in trade list is not a dict: {item}")
                    elif isinstance(data_part, dict): # Single trade
                        actual_trade_payloads_to_process.append(data_part)
                    else:
                        logger.warning(f"DataStream ({self.contract_id}): GatewayTrade data part for contract {event_contract_id} has unexpected type: {type(data_part)}")
                else: # Mismatched contract_id
                    logger.debug(f"DataStream ({self.contract_id}): Received GatewayTrade for a different contract_id ('{event_contract_id}'). Ignoring.")
                    return 
            elif len(args) == 1 and isinstance(args[0], dict): # Single dict in list wrapper
                actual_trade_payloads_to_process.append(args[0])
            elif len(args) > 0 and all(isinstance(item, dict) for item in args): # List of dicts directly
                 actual_trade_payloads_to_process.extend(args)
            else: # Unhandled list structure
                logger.warning(f"DataStream ({self.contract_id}): GatewayTrade args list has an unhandled structure: {args}")
        elif isinstance(args, dict): # Single dict directly (less common from hub for trades)
            actual_trade_payloads_to_process.append(args)
        else: # Completely unexpected type
            logger.warning(f"DataStream ({self.contract_id}): Received GatewayTrade with an unexpected argument type: {type(args)}, data: {args}")
            
        if not actual_trade_payloads_to_process: return # No valid payloads to process

        # Invoke callback for each processed trade payload
        for trade_payload_dict in actual_trade_payloads_to_process:
            try:
                # Ensure callback exists again, as it could be unset between loop iterations (unlikely)
                if self._callbacks["trade"]: self._callbacks["trade"](trade_payload_dict) 
            except Exception as e_cb_trade: # pylint: disable=broad-except
                logger.error(f"DataStream ({self.contract_id}): Unhandled error in on_trade_callback for trade payload {trade_payload_dict}: {e_cb_trade}", exc_info=True)

    def _handle_gateway_depth_event(self, args: List[Any]):
        """Internal handler for "GatewayDepth" messages."""
        payload = self._parse_market_payload(args, "GatewayDepth")
        if payload and self._callbacks["depth"]:
            try:
                self._callbacks["depth"](payload)
            except Exception as e_cb_depth: # pylint: disable=broad-except
                logger.error(f"DataStream ({self.contract_id}): Unhandled error in on_depth_callback: {e_cb_depth}", exc_info=True)

    def _send_subscriptions(self):
        """Sends configured subscriptions to the Market Hub after connection."""
        if self.connection_status != StreamConnectionState.CONNECTED:
            logger.warning(f"DataStream ({self.contract_id}): Cannot send subscriptions, stream not CONNECTED. Current state: '{self.connection_status.name}'")
            return
        if not self.connection: # Should not happen if status is "connected"
            logger.error(f"DataStream ({self.contract_id}): Critical error - cannot send subscriptions, connection object is None despite state being CONNECTED.")
            return
            
        try:
            logger.info(f"DataStream ({self.contract_id}): Sending subscriptions to Market Hub.")
            time_between_sends = 0.2 # Small delay between subscription messages if needed
            subscriptions_attempted_count = 0
            
            # Subscribe based on which callbacks are registered
            if self._callbacks["quote"]:
                logger.debug(f"DataStream ({self.contract_id}): Attempting to subscribe to ContractQuotes.")
                self.connection.send("SubscribeContractQuotes", [self.contract_id])
                subscriptions_attempted_count += 1
                time.sleep(time_between_sends) 
            if self._callbacks["trade"]: 
                logger.debug(f"DataStream ({self.contract_id}): Attempting to subscribe to ContractTrades.")
                self.connection.send("SubscribeContractTrades", [self.contract_id])
                subscriptions_attempted_count += 1
                time.sleep(time_between_sends)
            if self._callbacks["depth"]:
                logger.debug(f"DataStream ({self.contract_id}): Attempting to subscribe to ContractMarketDepth.")
                self.connection.send("SubscribeContractMarketDepth", [self.contract_id])
                subscriptions_attempted_count += 1
            
            if subscriptions_attempted_count > 0:
                logger.info(f"DataStream ({self.contract_id}): Finished sending {subscriptions_attempted_count} configured subscription request(s).")
            else:
                logger.info(f"DataStream ({self.contract_id}): No data subscriptions were configured (no callbacks provided for quote, trade, or depth).")
        except Exception as e_sub_send: # pylint: disable=broad-except
            logger.error(f"DataStream ({self.contract_id}): Exception occurred while sending subscriptions to Market Hub: {e_sub_send}", exc_info=True)
            self._set_connection_state(StreamConnectionState.ERROR, f"Exception sending subscriptions: {type(e_sub_send).__name__}")

    def start(self) -> bool:
        """
        Starts the SignalR connection to the Market Hub.

        Returns:
            bool: True if the connection attempt was initiated successfully,
                  False if there was an immediate issue preventing the start.
        """
        logger.info(f"DataStream ({self.contract_id}): Start called. Current state: {self.connection_status.name}")
        
        if not self.connection: # If connection wasn't built in __init__ (e.g., initial token fail)
             # Attempt to re-initialize token/URL and build connection now
             if not self._wss_hub_url_no_token: self._prepare_websocket_url_base()
             if not self._current_token_for_url: # If token is still missing
                try:
                    self._reinitialize_token_and_url()
                except Exception as e_reinit_start: # pylint: disable=broad-except
                    logger.error(f"DataStream ({self.contract_id}): Failed to re-initialize token/URL during start(): {e_reinit_start}. Cannot start stream.", exc_info=True)
                    self._set_connection_state(StreamConnectionState.ERROR, f"Token fetch/URL prep failed in start: {type(e_reinit_start).__name__}")
                    return False
             self._build_connection() # Build connection object now
             if not self.connection: # If build still fails
                  logger.error(f"DataStream ({self.contract_id}): Connection object is still None after attempting build in start(). Cannot start stream."); return False
        
        # Check current state before proceeding
        if self._current_state == StreamConnectionState.CONNECTED:
            logger.warning(f"DataStream ({self.contract_id}): Start called but stream is already CONNECTED.")
            return True
        if self._current_state == StreamConnectionState.CONNECTING:
            logger.warning(f"DataStream ({self.contract_id}): Start called but stream is already CONNECTING.")
            return True # Assume the ongoing connection attempt will resolve
        
        self._set_connection_state(StreamConnectionState.CONNECTING, "Start method initiated connection attempt")
        logger.info(f"Attempting to start DataStream SignalR connection for Contract: {self.contract_id}...")
        try:
            # Ensure URL with token is ready, might need re-init if token became None for some reason
            if not self._hub_url_with_token: 
                logger.info(f"DataStream ({self.contract_id}): Hub URL with token not ready, reinitializing before start.")
                self._reinitialize_token_and_url() # This will re-fetch token if needed
                self._build_connection() # Rebuild connection as URL might have changed
            
            if not self.connection: # Final check for connection object
                logger.error(f"DataStream ({self.contract_id}): Connection object is None even after re-init attempt in start(). Cannot start stream."); return False

            self.connection.start() # Initiate SignalR connection
            logger.info(f"DataStream ({self.contract_id}): SignalR connection process initiated. Waiting for _on_open callback for confirmation.")
            return True # Successfully initiated the start process
        except Exception as e_conn_start: # pylint: disable=broad-except
            logger.error(f"DataStream ({self.contract_id}): Failed to initiate SignalR connection start: {e_conn_start}", exc_info=True)
            self._set_connection_state(StreamConnectionState.ERROR, f"Exception during start: {str(e_conn_start)[:50]}")
            return False

    def stop(self, reason_for_stop: Optional[str] = "User requested stop"):
        """
        Stops the SignalR connection to the Market Hub.

        Args:
            reason_for_stop (Optional[str], optional): A string describing why stop is called,
                used for logging and status updates. Defaults to "User requested stop".
        """
        logger.info(f"DataStream ({self.contract_id}): Stop called. Reason: '{reason_for_stop}'. "
                    f"Current state: {self._current_state.name}")
        
        if self._current_state == StreamConnectionState.DISCONNECTED or self._current_state == StreamConnectionState.ERROR:
            logger.info(f"DataStream ({self.contract_id}): Stop called but stream already {self._current_state.name}. No action taken.")
            return

        self._set_connection_state(StreamConnectionState.STOPPING, reason_for_stop)
        
        if self.connection:
            logger.debug(f"DataStream ({self.contract_id}): Calling stop on underlying SignalR connection object...")
            try:
                self.connection.stop() 
                logger.info(f"DataStream ({self.contract_id}): SignalR connection stop request sent.")
                # _on_close will handle transition to DISCONNECTED
            except Exception as e_conn_stop: 
                logger.error(f"Error stopping DataStream ({self.contract_id}) connection: {e_conn_stop}", exc_info=True)
                self._set_connection_state(StreamConnectionState.ERROR, f"Exception during stop: {str(e_conn_stop)[:50]}")
        else: 
            logger.info(f"DataStream ({self.contract_id}): No active SignalR connection object to stop (was None). Setting state to DISCONNECTED.")
            self._set_connection_state(StreamConnectionState.DISCONNECTED, "No connection object to stop")