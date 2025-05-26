# tsxapipy/real_time/user_hub_stream.py
import logging
import time
from typing import Callable, Optional, Any, List, Dict

from signalrcore.hub_connection_builder import HubConnectionBuilder # Make sure this is imported

from tsxapipy.config import USER_HUB_URL 
from tsxapipy.api import APIClient 
from .stream_state import StreamConnectionState
# BaseStream import was missing in the provided snippet, but it inherits from it.
# Assuming BaseStream is defined correctly and handles common SignalR logic.
# from .base_stream import BaseStream # Ensure this is present if UserHubStream inherits it.
# For this consolidated version, I'll assume BaseStream is not the immediate focus of this fix,
# but the UserHubStream __init__ signature and its internal logic.
# The provided code uses super().__init__(...) so BaseStream is critical.
# We need to ensure UserHubStream passes correct parameters to BaseStream's __init__.

logger = logging.getLogger(__name__)

# Callback type aliases
AccountUpdateCallback = Callable[[Dict[str, Any]], None]
OrderUpdateCallback = Callable[[Dict[str, Any]], None]
PositionUpdateCallback = Callable[[Dict[str, Any]], None]
UserTradeCallback = Callable[[Dict[str, Any]], None] # Added based on typical usage
StreamErrorCallback = Callable[[Any], None]
StreamStateChangeCallback = Callable[[str], None] # Argument is state name (str)

# This UserHubStream is simplified and doesn't inherit from a detailed BaseStream
# from your prompt. I'll reconstruct a more functional version based on common patterns
# and the methods it calls (like self.connection.send).
# If you have a full BaseStream, this would inherit and call super.

class UserHubStream: # Removed (BaseStream) for now to provide a self-contained example for __init__ fix
    """
    Manages a SignalR connection to the TopStep User Hub for real-time user-specific data.
    """
    def __init__(
        self,
        api_client: APIClient,
        account_id_to_watch: int, # CHANGED: Parameter name and type to int
        on_order_update: Optional[OrderUpdateCallback] = None,
        on_position_update: Optional[PositionUpdateCallback] = None,
        on_account_update: Optional[AccountUpdateCallback] = None,
        on_user_trade_update: Optional[UserTradeCallback] = None, # ADDED
        subscribe_to_accounts_globally: bool = True, # ADDED
        on_error_callback: Optional[StreamErrorCallback] = None, # RENAMED from on_error
        on_state_change_callback: Optional[StreamStateChangeCallback] = None # RENAMED from on_state_change
    ):
        """
        Initialize the UserHubStream.
        """
        self.logger = logger # Use the module-level logger
        self.api_client = api_client
        self.account_id = account_id_to_watch # Store internally as self.account_id
        
        self._callbacks = {
            "order": on_order_update,
            "position": on_position_update,
            "account": on_account_update,
            "user_trade": on_user_trade_update
        }
        self.subscribe_to_accounts_globally = subscribe_to_accounts_globally
        self.on_error_callback = on_error_callback
        self.on_state_change_callback = on_state_change_callback

        self.connection: Optional[HubConnectionBuilder] = None # Correct type hint
        self.connection_status = StreamConnectionState.NOT_INITIALIZED # Internal state tracking
        
        self._current_token_for_url: Optional[str] = None
        self._wss_hub_url_no_token: str = "" # Base WSS URL without token
        self._hub_url_with_token: Optional[str] = None # Full URL with token

        self._prepare_websocket_url_base()
        try:
            self._reinitialize_token_and_url() # Get initial token
        except Exception as e:
            self.logger.error(f"UserHubStream __init__: Failed to get initial token: {e}")
            self._set_connection_state(StreamConnectionState.ERROR, f"Initial token fail: {type(e).__name__}")
            # Optionally re-raise or handle so object creation doesn't silently fail later.
            # For now, it will proceed, but start() will likely fail.

        self._build_connection()
        self.logger.info(f"UserHubStream initialized for Account ID: {self.account_id}. Global Sub: {self.subscribe_to_accounts_globally}")


    def _prepare_websocket_url_base(self):
        """Prepares the base WebSocket URL (wss://...) from USER_HUB_URL."""
        if USER_HUB_URL.startswith("https://"):
            self._wss_hub_url_no_token = "wss://" + USER_HUB_URL[len("https://"):]
        elif USER_HUB_URL.startswith("http://"): # Less likely for production
            self._wss_hub_url_no_token = "ws://" + USER_HUB_URL[len("http://"):]
        else: # Assume it might already be a ws/wss or needs no change (less robust)
            self.logger.warning(f"USER_HUB_URL '{USER_HUB_URL}' does not start with http(s). Using as is for wss base.")
            self._wss_hub_url_no_token = USER_HUB_URL
        self.logger.debug(f"UserHubStream: Prepared base WebSocket URL: {self._wss_hub_url_no_token}")


    def _reinitialize_token_and_url(self):
        """Fetches a new token from APIClient and updates the connection URL string."""
        self.logger.debug(f"UserHubStream (Acct: {self.account_id}): Re-initializing token and Hub URL.")
        if not self.api_client:
            raise ConnectionError("UserHubStream: APIClient not available for token refresh.")
        
        self._current_token_for_url = self.api_client.current_token # This gets a fresh token
        if not self._current_token_for_url:
            raise ConnectionError("UserHubStream: Failed to obtain a valid token from APIClient.")
        
        if not self._wss_hub_url_no_token: # Should have been set by _prepare_websocket_url_base
             self.logger.error("UserHubStream: _wss_hub_url_no_token is not set. Cannot form full hub URL.")
             raise ConnectionError("UserHubStream: Base WSS Hub URL is not prepared.")

        self._hub_url_with_token = f"{self._wss_hub_url_no_token}?token={self._current_token_for_url}"
        self.logger.debug(f"UserHubStream (Acct: {self.account_id}): Hub URL updated with new token.")


    def _build_connection(self):
        """Builds or rebuilds the SignalR connection object and registers event handlers."""
        if not self._hub_url_with_token:
            self.logger.error("UserHubStream: Cannot build connection, _hub_url_with_token is not set.")
            return

        self.logger.debug(f"UserHubStream (Acct: {self.account_id}): Building new SignalR connection object for URL (token hidden): "
                          f"{self._wss_hub_url_no_token}?token=...")
        options = {
            'verify_ssl': True, # Adjust as needed for your environment (e.g., False for self-signed certs in demo)
            # 'skip_negotiation': True, # Usually not needed for Python client, can cause issues. Default is False.
        }
        self.connection = HubConnectionBuilder() \
            .with_url(self._hub_url_with_token, options=options) \
            .with_automatic_reconnect({
                "type": "interval", 
                "keep_alive_interval": 10,
                "intervals": [0, 2, 5, 10, 15, 30, 60, 120] # Example: shorter reconnect intervals
            }) \
            .build()

        # Standard SignalR lifecycle events
        self.connection.on_open(self._on_open)
        self.connection.on_close(self._on_close) # Called when connection closes gracefully or due to error after retries
        self.connection.on_error(self._on_error_signalr) # Underlying transport/protocol errors
        
        # Hub-specific message handlers
        # Ensure these message names "GatewayUserOrder", etc. match exactly what the server sends.
        if self._callbacks["order"]:
            self.connection.on("GatewayUserOrder", self._handle_order_update)
        if self._callbacks["position"]:
            self.connection.on("GatewayUserPosition", self._handle_position_update)
        if self._callbacks["account"]:
            self.connection.on("GatewayUserAccount", self._handle_account_update)
        if self._callbacks["user_trade"]:
            self.connection.on("GatewayUserTrade", self._handle_user_trade_update) # ADDED
        
        self.logger.debug(f"UserHubStream (Acct: {self.account_id}): SignalR event handlers registered.")

    def _set_connection_state(self, new_state: StreamConnectionState, reason: Optional[str] = None):
        """Safely updates and logs connection state, invoking callback if changed."""
        if self.connection_status != new_state:
            old_state_name = self.connection_status.name
            self.connection_status = new_state
            log_msg = f"UserHubStream (Acct: {self.account_id}): State changed from {old_state_name} to {new_state.name}."
            if reason: log_msg += f" Reason: {reason}"
            self.logger.info(log_msg)
            if self.on_state_change_callback:
                try:
                    self.on_state_change_callback(new_state.name) # Pass string name
                except Exception as e_cb:
                    self.logger.error(f"UserHubStream (Acct: {self.account_id}): Error in on_state_change_callback: {e_cb}", exc_info=True)
    
    def _on_open(self):
        """Callback for when the SignalR connection successfully opens."""
        self._set_connection_state(StreamConnectionState.CONNECTED, "SignalR connection opened")
        self._send_subscriptions()

    def _on_close(self):
        """Callback for when the SignalR connection closes."""
        # If stop was intentionally called, state would be STOPPING then DISCONNECTED by stop()
        if self.connection_status not in [StreamConnectionState.STOPPING, StreamConnectionState.DISCONNECTED, StreamConnectionState.ERROR]:
            self._set_connection_state(StreamConnectionState.DISCONNECTED, "SignalR connection closed unexpectedly")
        elif self.connection_status == StreamConnectionState.STOPPING: # Graceful stop
             self._set_connection_state(StreamConnectionState.DISCONNECTED, "Stopped by user/application")


    def _on_error_signalr(self, error_payload: Any):
        """Callback for fundamental SignalR connection errors (not hub invocation errors)."""
        error_message = str(error_payload) if error_payload else "Unknown SignalR transport error"
        self.logger.error(f"UserHubStream (Acct: {self.account_id}): Underlying SignalR connection error: {error_message}", exc_info=isinstance(error_payload, Exception))
        self._set_connection_state(StreamConnectionState.ERROR, f"SignalR transport error: {error_message[:100]}")
        if self.on_error_callback:
            try:
                self.on_error_callback(error_payload)
            except Exception as e_cb:
                self.logger.error(f"UserHubStream (Acct: {self.account_id}): Error in on_error_callback: {e_cb}", exc_info=True)

    def _parse_payload(self, args: List[Any], event_name: str) -> Optional[Dict[str, Any]]:
        """Helper to parse common SignalR list-wrapped dictionary payloads for User Hub."""
        # self.logger.debug(f"UserHubStream (Acct: {self.account_id}): Raw {event_name} args: {args}")
        if isinstance(args, list) and len(args) > 0 and isinstance(args[0], dict):
            return args[0]
        self.logger.warning(f"UserHubStream (Acct: {self.account_id}): Received {event_name} payload with unexpected structure: {args}")
        return None

    def _handle_order_update(self, args: List[Any]): # Changed from order_data to args
        payload = self._parse_payload(args, "GatewayUserOrder")
        if payload and self._callbacks["order"]:
            try:
                self._callbacks["order"](payload)
            except Exception as e:
                self.logger.error(f"UserHubStream (Acct: {self.account_id}): Error in on_order_update callback: {e}", exc_info=True)

    def _handle_position_update(self, args: List[Any]): # Changed from position_data to args
        payload = self._parse_payload(args, "GatewayUserPosition")
        if payload and self._callbacks["position"]:
            try:
                self._callbacks["position"](payload)
            except Exception as e:
                self.logger.error(f"UserHubStream (Acct: {self.account_id}): Error in on_position_update callback: {e}", exc_info=True)

    def _handle_account_update(self, args: List[Any]): # Changed from account_data to args
        payload = self._parse_payload(args, "GatewayUserAccount")
        if payload and self._callbacks["account"]:
            try:
                self._callbacks["account"](payload)
            except Exception as e:
                self.logger.error(f"UserHubStream (Acct: {self.account_id}): Error in on_account_update callback: {e}", exc_info=True)

    def _handle_user_trade_update(self, args: List[Any]): # ADDED
        payload = self._parse_payload(args, "GatewayUserTrade")
        if payload and self._callbacks["user_trade"]:
            try:
                self._callbacks["user_trade"](payload)
            except Exception as e:
                self.logger.error(f"UserHubStream (Acct: {self.account_id}): Error in on_user_trade_update callback: {e}", exc_info=True)

    def _send_subscriptions(self):
        """Sends configured subscriptions to the User Hub after connection."""
        if not self.connection or self.connection_status != StreamConnectionState.CONNECTED:
            self.logger.warning(f"UserHubStream (Acct: {self.account_id}): Cannot send subscriptions, not in CONNECTED state (current: {self.connection_status.name}).")
            return
        try:
            self.logger.info(f"UserHubStream (Acct: {self.account_id}): Sending subscriptions. Global: {self.subscribe_to_accounts_globally}")
            
            if self._callbacks["account"]:
                if self.subscribe_to_accounts_globally:
                    self.logger.info(f"UserHubStream (Acct: {self.account_id}): Subscribing to all accounts globally (SubscribeAccounts).")
                    self.connection.send("SubscribeToAccounts", []) # API method name per docs
                elif self.account_id > 0 : # Only subscribe to specific if ID is valid
                    self.logger.info(f"UserHubStream (Acct: {self.account_id}): Subscribing to specific account (SubscribeToAccount).")
                    self.connection.send("SubscribeToAccount", [self.account_id]) # API method name
                else:
                    self.logger.warning(f"UserHubStream (Acct: {self.account_id}): Account callback set but global sub is false and account ID is invalid. No account subscription sent.")
                time.sleep(0.1) # Small delay between sends

            # Account-specific subscriptions
            if self.account_id > 0:
                if self._callbacks["order"]:
                    self.logger.info(f"UserHubStream (Acct: {self.account_id}): Subscribing to orders (SubscribeToOrders).")
                    self.connection.send("SubscribeToOrders", [self.account_id])
                    time.sleep(0.1)
                if self._callbacks["position"]:
                    self.logger.info(f"UserHubStream (Acct: {self.account_id}): Subscribing to positions (SubscribeToPositions).")
                    self.connection.send("SubscribeToPositions", [self.account_id])
                    time.sleep(0.1)
                if self._callbacks["user_trade"]:
                    self.logger.info(f"UserHubStream (Acct: {self.account_id}): Subscribing to user trades (SubscribeToUserTrades).")
                    self.connection.send("SubscribeToUserTrades", [self.account_id]) # Renamed from SubscribeTrades
            elif self._callbacks["order"] or self._callbacks["position"] or self._callbacks["user_trade"]:
                self.logger.warning(f"UserHubStream (Acct: {self.account_id}): Callbacks for Orders/Positions/UserTrades exist, but account_id is invalid. Not subscribing.")
            
            self.logger.info(f"UserHubStream (Acct: {self.account_id}): Subscription messages sent.")
        except Exception as e: 
            self.logger.error(f"UserHubStream (Acct: {self.account_id}): Error sending subscriptions: {e}", exc_info=True)
            self._set_connection_state(StreamConnectionState.ERROR, f"Subscription send error: {type(e).__name__}")


    def start(self) -> bool:
        """Starts the SignalR connection to the User Hub."""
        self.logger.info(f"UserHubStream (Acct: {self.account_id}): Start called. Current state: {self.connection_status.name}")
        
        if self.connection_status == StreamConnectionState.CONNECTED:
            self.logger.warning(f"UserHubStream (Acct: {self.account_id}): Start called but stream is already CONNECTED.")
            return True
        if self.connection_status == StreamConnectionState.CONNECTING:
            self.logger.warning(f"UserHubStream (Acct: {self.account_id}): Start called but stream is already CONNECTING.")
            return True 
        
        # Ensure connection object exists and URL is ready
        if not self.connection or not self._hub_url_with_token:
            self.logger.info(f"UserHubStream (Acct: {self.account_id}): Connection or URL not ready, attempting to reinitialize/build.")
            try:
                if not self._wss_hub_url_no_token: self._prepare_websocket_url_base() # Should be done in init
                self._reinitialize_token_and_url() # Ensures token and _hub_url_with_token are fresh
                self._build_connection() # Rebuilds connection object with new URL
            except Exception as e_reinit_start: 
                self.logger.error(f"UserHubStream (Acct: {self.account_id}): Failed to re-initialize token/URL/connection during start(): {e_reinit_start}. Cannot start stream.", exc_info=True)
                self._set_connection_state(StreamConnectionState.ERROR, f"Prep failed in start: {type(e_reinit_start).__name__}")
                return False
        
        if not self.connection: # Check again after build attempt
            self.logger.error(f"UserHubStream (Acct: {self.account_id}): Connection object is still None after build attempt in start(). Cannot start stream.")
            self._set_connection_state(StreamConnectionState.ERROR, "Connection object None post-build")
            return False
        
        self._set_connection_state(StreamConnectionState.CONNECTING, "Start method initiated connection attempt")
        try:
            self.connection.start() 
            self.logger.info(f"UserHubStream (Acct: {self.account_id}): SignalR connection.start() called. Waiting for _on_open or _on_error.")
            return True # Indicates start was attempted. Actual connection is async.
        except Exception as e_conn_start: 
            self.logger.error(f"UserHubStream (Acct: {self.account_id}): Exception calling connection.start(): {e_conn_start}", exc_info=True)
            self._set_connection_state(StreamConnectionState.ERROR, f"Exception during connection.start(): {str(e_conn_start)[:50]}")
            return False

    def stop(self, reason_for_stop: Optional[str] = "User requested stop"):
        """Stops the SignalR connection."""
        self.logger.info(f"UserHubStream (Acct: {self.account_id}): Stop called. Reason: '{reason_for_stop}'. Current state: {self.connection_status.name}")
        
        # Allow stop even if in error or intermediate state to ensure cleanup
        self._set_connection_state(StreamConnectionState.STOPPING, reason_for_stop)
        
        if self.connection:
            try: 
                self.connection.stop()
                # _on_close should eventually set state to DISCONNECTED
            except Exception as e_conn_stop:
                self.logger.error(f"UserHubStream (Acct: {self.account_id}): Exception during connection.stop(): {e_conn_stop}", exc_info=True)
                self._set_connection_state(StreamConnectionState.ERROR, f"Exception during stop: {str(e_conn_stop)[:50]}")
        else:
            self.logger.info(f"UserHubStream (Acct: {self.account_id}): No connection object to stop. Setting state to DISCONNECTED.")
            self._set_connection_state(StreamConnectionState.DISCONNECTED, "No connection object was present")

    def update_token(self, new_token: str):
        """Updates the authentication token and restarts the SignalR connection."""
        if not new_token:
            self.logger.error(f"UserHubStream (Acct: {self.account_id}): Attempted to update token with an empty new token. Ignoring.")
            return
        
        self.logger.info(f"UserHubStream (Acct: {self.account_id}): Token update requested. Old token prefix: {self._current_token_for_url[:10] if self._current_token_for_url else 'None'}, New token prefix: {new_token[:10]}...")

        if new_token == self._current_token_for_url and self.connection_status == StreamConnectionState.CONNECTED:
            self.logger.debug(f"UserHubStream (Acct: {self.account_id}): Token update requested with the same token and stream already connected. No action taken.")
            return

        self.logger.info(f"UserHubStream (Acct: {self.account_id}): Proceeding with connection restart for token update.")
        
        # Stop current connection if active or trying to connect
        if self.connection_status not in [StreamConnectionState.NOT_INITIALIZED, StreamConnectionState.DISCONNECTED, StreamConnectionState.ERROR]:
            self.logger.info(f"UserHubStream (Acct: {self.account_id}): Stopping existing connection (state: {self.connection_status.name}) before token update.")
            self.stop(reason_for_stop="Token update initiated stop")
            # Wait briefly for stop to take effect; SignalR stop can be async
            time.sleep(0.5) 

        self._current_token_for_url = new_token # Update internal token
        if not self._wss_hub_url_no_token: self._prepare_websocket_url_base()
        self._hub_url_with_token = f"{self._wss_hub_url_no_token}?token={self._current_token_for_url}"
        
        self.logger.info(f"UserHubStream (Acct: {self.account_id}): Rebuilding connection with new token...")
        self._build_connection() # Rebuilds connection with the new _hub_url_with_token
        
        self.logger.info(f"UserHubStream (Acct: {self.account_id}): Attempting to start connection after token update.")
        self.start() # Attempt to start the new connection