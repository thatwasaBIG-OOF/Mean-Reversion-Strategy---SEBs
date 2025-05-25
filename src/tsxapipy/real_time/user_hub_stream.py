# tsxapipy/real_time/user_hub_stream.py
import logging
import time
# from enum import Enum, auto # No longer define StreamConnectionState here
from typing import Callable, Optional, Any, List, Dict

from signalrcore.hub_connection_builder import HubConnectionBuilder

from tsxapipy.config import USER_HUB_URL 
from tsxapipy.api import APIClient 
from .data_stream import StreamConnectionState # Import the shared Enum

logger = logging.getLogger(__name__)

# Callback type aliases
AccountUpdateCallback = Callable[[Dict[str, Any]], None]
OrderUpdateCallback = Callable[[Dict[str, Any]], None]
PositionUpdateCallback = Callable[[Dict[str, Any]], None]
UserTradeCallback = Callable[[Dict[str, Any]], None]
StreamErrorCallback = Callable[[Any], None]
StreamStateChangeCallback = Callable[[str], None] # Will pass state name as string

# StreamConnectionState Enum is now imported from .data_stream

class UserHubStream:
    """
    Manages a SignalR connection to the TopStep User Hub for real-time user-specific data.
    (Full docstring as previously provided)
    """
    def __init__(self,
                 api_client: APIClient,
                 account_id_to_watch: int,
                 on_order_update: Optional[OrderUpdateCallback] = None,
                 on_account_update: Optional[AccountUpdateCallback] = None,
                 on_position_update: Optional[PositionUpdateCallback] = None,
                 on_user_trade_update: Optional[UserTradeCallback] = None,
                 subscribe_to_accounts_globally: bool = True, 
                 on_error_callback: Optional[StreamErrorCallback] = None,
                 on_state_change_callback: Optional[StreamStateChangeCallback] = None):
        if not isinstance(api_client, APIClient):
            raise TypeError("api_client must be an instance of APIClient.")
        self.api_client = api_client

        if not isinstance(account_id_to_watch, int):
             raise ValueError(f"account_id_to_watch must be an integer, got {type(account_id_to_watch)}")
        
        if account_id_to_watch <= 0 and (on_order_update or on_position_update or on_user_trade_update):
            logger.warning(f"UserHubStream initialized with account_id_to_watch: {account_id_to_watch}. "
                           "Account-specific subscriptions (Orders, Positions, UserTrades) may not be sent "
                           "or may fail if a positive ID is required by the API for those subscriptions.")
        
        self.account_id: int = account_id_to_watch
        self._callbacks: Dict[str, Optional[Callable]] = {
            "order": on_order_update,
            "account": on_account_update,
            "position": on_position_update,
            "user_trade": on_user_trade_update,
            "error": on_error_callback,
            "state_change": on_state_change_callback
        }
        self.subscribe_to_accounts_globally: bool = subscribe_to_accounts_globally
        self.connection: Optional[Any] = None
        self._http_hub_base_url: str = USER_HUB_URL
        self._wss_hub_url_no_token: Optional[str] = None
        self._current_token_for_url: Optional[str] = None
        self._hub_url_with_token: Optional[str] = None
        
        self._current_state: StreamConnectionState = StreamConnectionState.DISCONNECTED # Use imported Enum

        logger.info(f"UserHubStream initialized for Account ID: {self.account_id}. Token will be embedded in URL.")
        self._prepare_websocket_url_base()
        try:
            self._reinitialize_token_and_url()
        except Exception as e:
            logger.error(f"UserHubStream (Acct: {self.account_id}): Failed to get initial token during __init__: {e}", exc_info=True)
            self._set_connection_state(StreamConnectionState.ERROR, f"Initial setup failed: {type(e).__name__}")
            return
            
        self._build_connection()

    @property
    def connection_status(self) -> StreamConnectionState: # Return Enum type
        """Current status of the SignalR connection."""
        return self._current_state

    def _set_connection_state(self, new_state: StreamConnectionState, reason: Optional[str] = None):
        """Internal method to update and log connection state changes."""
        if self._current_state != new_state:
            old_state_name = self._current_state.name
            self._current_state = new_state
            new_state_name = new_state.name
            log_msg = f"UserHubStream (Acct: {self.account_id}): Connection state changed from '{old_state_name}' to '{new_state_name}'."
            if reason:
                log_msg += f" Reason: {reason}"
            logger.info(log_msg) 
            if self._callbacks["state_change"]:
                try:
                    self._callbacks["state_change"](new_state_name)
                except Exception as e_cb_state:
                    logger.error(f"UserHubStream (Acct: {self.account_id}): Error in on_state_change_callback for '{new_state_name}': {e_cb_state}", exc_info=True)

    def _prepare_websocket_url_base(self):
        """Derives the base WebSocket URL (ws:// or wss://) from the HTTP/HTTPS hub URL."""
        if not self._http_hub_base_url or "://" not in self._http_hub_base_url:
            logger.error(f"UserHubStream (Acct: {self.account_id}): Invalid http_hub_base_url '{self._http_hub_base_url}'. Cannot derive WebSocket URL base.")
            self._wss_hub_url_no_token = None
            return
        try:
            scheme, rest_of_url = self._http_hub_base_url.split("://", 1)
            if scheme.lower() == "http": self._wss_hub_url_no_token = f"ws://{rest_of_url}"
            elif scheme.lower() == "https": self._wss_hub_url_no_token = f"wss://{rest_of_url}"
            else: 
                self._wss_hub_url_no_token = self._http_hub_base_url
                if not (scheme.lower().startswith("ws")):
                     logger.warning(f"UserHubStream (Acct: {self.account_id}): Unexpected scheme '{scheme}' in hub URL. Using URL directly for WebSocket: {self._wss_hub_url_no_token}")
        except ValueError:
            logger.error(f"UserHubStream (Acct: {self.account_id}): Malformed http_hub_base_url '{self._http_hub_base_url}'. Cannot derive WebSocket URL base.")
            self._wss_hub_url_no_token = None
        if self._wss_hub_url_no_token:
            logger.debug(f"UserHubStream (Acct: {self.account_id}): Prepared WebSocket base URL: {self._wss_hub_url_no_token}")

    def _reinitialize_token_and_url(self):
        """Fetches the current token from APIClient and constructs the full WebSocket URL."""
        logger.debug(f"UserHubStream (Acct: {self.account_id}): Re-initializing token and WebSocket URL.")
        if not self.api_client: raise RuntimeError(f"UserHubStream (Acct: {self.account_id}): APIClient instance not available.")
        if not self._wss_hub_url_no_token:
            self._prepare_websocket_url_base()
            if not self._wss_hub_url_no_token: raise RuntimeError(f"UserHubStream (Acct: {self.account_id}): Base WebSocket URL could not be prepared.")
        try:
            self._current_token_for_url = self.api_client.current_token
            self._hub_url_with_token = f"{self._wss_hub_url_no_token}?access_token={self._current_token_for_url}"
            log_token_preview = f"{self._current_token_for_url[:15]}..." if self._current_token_for_url and len(self._current_token_for_url) > 15 else "Token (short or None)"
            logger.info(f"UserHubStream (Acct: {self.account_id}): Token obtained/refreshed. Full WebSocket URL configured (token preview: {log_token_preview}).")
        except Exception as e_token_fetch:
            logger.error(f"UserHubStream (Acct: {self.account_id}): Failed to get token from APIClient for WebSocket URL construction: {e_token_fetch}", exc_info=True)
            self._hub_url_with_token = None
            raise

    def _build_connection(self):
        """Builds or rebuilds the SignalR connection object with current URL and handlers."""
        logger.debug(f"UserHubStream (Acct: {self.account_id}): Building new SignalR connection object.")
        if not self._hub_url_with_token:
            logger.critical(f"UserHubStream (Acct: {self.account_id}): Full WebSocket Hub URL (with token) is not prepared. Cannot build SignalR connection.")
            self._set_connection_state(StreamConnectionState.ERROR, "Hub URL with token not ready for build")
            return

        logger.info(f"UserHubStream (Acct: {self.account_id}): Configuring SignalR connection with skip_negotiation=True.")
        
        user_agent_header_val = "tsxapipy-UserHubStream/unknown-version"
        if self.api_client and self.api_client.session and self.api_client.session.headers:
            base_user_agent = self.api_client.session.headers.get('User-Agent', 'tsxapipy-unknown-version')
            user_agent_header_val = f"tsxapipy-UserHubStream/{base_user_agent}"

        connection_options = {
            "headers": {"User-Agent": user_agent_header_val},
            "skip_negotiation": True 
        }
        self.connection = HubConnectionBuilder() \
            .with_url(self._hub_url_with_token, options=connection_options) \
            .with_automatic_reconnect({"type": "interval", "keep_alive_interval": 10, "intervals": [0, 2000, 5000, 10000, 30000, 60000]}) \
            .build()
        
        self.connection.on_open(self._on_open)
        self.connection.on_close(self._on_close)
        self.connection.on_error(self._on_error)

        if self._callbacks["account"]: self.connection.on("GatewayUserAccount", self._handle_gateway_user_account_event)
        if self._callbacks["order"]: self.connection.on("GatewayUserOrder", self._handle_gateway_user_order_event)
        if self._callbacks["position"]: self.connection.on("GatewayUserPosition", self._handle_gateway_user_position_event)
        if self._callbacks["user_trade"]: self.connection.on("GatewayUserTrade", self._handle_gateway_user_trade_event)
        logger.debug(f"UserHubStream (Acct: {self.account_id}): SignalR connection object built and event handlers registered.")

    def _on_open(self):
        """Internal callback invoked by signalrcore when connection is successfully opened."""
        conn_id_str = self.connection.connection_id if self.connection and hasattr(self.connection, 'connection_id') else "N/A"
        logger.info(f"UserHubStream (Acct: {self.account_id}) CONNECTED to User Hub (Client Connection ID: {conn_id_str}).")
        self._set_connection_state(StreamConnectionState.CONNECTED)
        self._send_subscriptions()

    def _on_close(self):
        """Internal callback invoked by signalrcore when connection is closed."""
        log_message = (
            f"UserHubStream (Acct: {self.account_id}): SignalR connection _on_close CALLED. "
            f"Current state before this event: {self._current_state.name}"
        )
        logger.info(log_message)

        if self._current_state == StreamConnectionState.STOPPING:
            self._set_connection_state(StreamConnectionState.DISCONNECTED, "Connection intentionally closed")
        elif self._current_state == StreamConnectionState.CONNECTED or self._current_state == StreamConnectionState.CONNECTING :
            self._set_connection_state(StreamConnectionState.RECONNECTING_UNEXPECTED, "Connection closed unexpectedly; auto-reconnect will attempt")

    def _on_error(self, error: Any):
        """Internal callback invoked by signalrcore on connection errors."""
        log_message = (
            f"UserHubStream (Acct: {self.account_id}): SignalR connection _on_error CALLED. Error: '{error}', Type: {type(error)}. "
            f"Current state: {self._current_state.name}"
        )
        logger.error(log_message)
             
        if self._callbacks["error"]:
            try: self._callbacks["error"](error)
            except Exception as e_cb_err:
                logger.error(f"UserHubStream (Acct: {self.account_id}): Exception in user's on_error_callback: {e_cb_err}", exc_info=True)
        
        if self._current_state not in [StreamConnectionState.STOPPING, StreamConnectionState.DISCONNECTED, StreamConnectionState.ERROR]:
             self._set_connection_state(StreamConnectionState.RECONNECTING_UNEXPECTED, f"Stream error occurred: {str(error)[:50]}")


    def update_token(self, new_token_from_api_client: str):
        """
        Updates the authentication token used for the SignalR connection.
        This stops the current connection, rebuilds it with the new token, and restarts.
        """
        logger.info(f"UserHubStream (Acct: {self.account_id}): update_token called. Current state: {self.connection_status.name}. "
                    f"New token (first 15 chars): {new_token_from_api_client[:15] if new_token_from_api_client else 'None'}...")
        
        if not new_token_from_api_client:
            logger.error(f"UserHubStream (Acct: {self.account_id}): update_token called with an empty new token. Ignoring request."); return
        
        if new_token_from_api_client == self._current_token_for_url and self.connection_status == StreamConnectionState.CONNECTED:
            logger.debug(f"UserHubStream (Acct: {self.account_id}): update_token called with the current token while stream is connected. No forced restart needed.")
            return

        original_state_before_token_update = self._current_state
        self._set_connection_state(StreamConnectionState.RECONNECTING_TOKEN, "Token update procedure initiated")
        
        logger.info(f"UserHubStream (Acct: {self.account_id}): Token update in progress. Stream will be stopped, reconfigured, and restarted.")
        
        if self.connection and original_state_before_token_update not in [StreamConnectionState.DISCONNECTED, StreamConnectionState.ERROR]:
            logger.debug(f"UserHubStream (Acct: {self.account_id}): Stopping current connection (state: {original_state_before_token_update.name}) as part of token update.")
            self.stop(reason_for_stop="Token update procedure")
            time.sleep(0.5) 
        
        try:
            self._current_token_for_url = new_token_from_api_client 
            if not self._wss_hub_url_no_token: self._prepare_websocket_url_base()
            if self._wss_hub_url_no_token:
                self._hub_url_with_token = f"{self._wss_hub_url_no_token}?access_token={self._current_token_for_url}"
                logger.info(f"UserHubStream (Acct: {self.account_id}): New WebSocket URL with updated token configured for restart.")
            else:
                logger.error(f"UserHubStream (Acct: {self.account_id}): Critical error - cannot update WebSocket URL as base WSS URL is not set. Token update failed.")
                self._set_connection_state(StreamConnectionState.ERROR, "Base WSS URL not set during token update")
                return
            
            self._build_connection()
            if not self.start():
                logger.error(f"UserHubStream (Acct: {self.account_id}): Failed to restart stream after token update procedure.")
                if self._current_state not in [StreamConnectionState.CONNECTING, StreamConnectionState.ERROR]:
                     self._set_connection_state(StreamConnectionState.ERROR, "Failed to initiate start after token update")
        except Exception as e_token_update_seq:
            logger.error(f"UserHubStream (Acct: {self.account_id}): Exception during token update and stream restart sequence: {e_token_update_seq}", exc_info=True)
            self._set_connection_state(StreamConnectionState.ERROR, f"Exception during token update sequence: {type(e_token_update_seq).__name__}")

    def _parse_user_payload(self, args: List[Any], event_name: str) -> Optional[Dict[str, Any]]:
        """Internal helper to parse common SignalR payloads for User Hub."""
        # Log level changed from DEBUG to DEBUG for less noise on frequent events
        logger.debug(f"UserHubStream (Acct: {self.account_id}): Raw {event_name} event args received: {args}")
        if isinstance(args, list) and len(args) >= 1:
            if isinstance(args[0], dict):
                return args[0] 
            else:
                logger.warning(f"UserHubStream (Acct: {self.account_id}): {event_name} event's first argument in list is not a dict: {type(args[0])}")
        elif isinstance(args, dict):
            return args
        
        logger.warning(f"UserHubStream (Acct: {self.account_id}): Received {event_name} payload with an unhandled argument structure: {args}")
        return None

    def _handle_gateway_user_account_event(self, args: List[Any]):
        """Internal handler for "GatewayUserAccount" messages."""
        payload = self._parse_user_payload(args, "GatewayUserAccount")
        if payload and self._callbacks["account"]:
            try: self._callbacks["account"](payload)
            except Exception as e_cb_account:
                logger.error(f"UserHubStream (Acct: {self.account_id}): Unhandled error in on_account_update callback: {e_cb_account}", exc_info=True)

    def _handle_gateway_user_order_event(self, args: List[Any]):
        """Internal handler for "GatewayUserOrder" messages."""
        payload = self._parse_user_payload(args, "GatewayUserOrder")
        if payload and self._callbacks["order"]:
            try: self._callbacks["order"](payload)
            except Exception as e_cb_order:
                logger.error(f"UserHubStream (Acct: {self.account_id}): Unhandled error in on_order_update callback: {e_cb_order}", exc_info=True)

    def _handle_gateway_user_position_event(self, args: List[Any]):
        """Internal handler for "GatewayUserPosition" messages."""
        payload = self._parse_user_payload(args, "GatewayUserPosition")
        if payload and self._callbacks["position"]:
            try: self._callbacks["position"](payload)
            except Exception as e_cb_pos:
                logger.error(f"UserHubStream (Acct: {self.account_id}): Unhandled error in on_position_update callback: {e_cb_pos}", exc_info=True)
    
    def _handle_gateway_user_trade_event(self, args: List[Any]):
        """Internal handler for "GatewayUserTrade" messages."""
        payload = self._parse_user_payload(args, "GatewayUserTrade")
        if payload and self._callbacks["user_trade"]:
            try: self._callbacks["user_trade"](payload)
            except Exception as e_cb_utrade:
                logger.error(f"UserHubStream (Acct: {self.account_id}): Unhandled error in on_user_trade_update callback: {e_cb_utrade}", exc_info=True)

    def _send_subscriptions(self):
        """Sends configured subscriptions to the User Hub after connection."""
        if self.connection_status != StreamConnectionState.CONNECTED:
            logger.warning(f"UserHubStream (Acct: {self.account_id}): Cannot send subscriptions, stream not CONNECTED. Current state: '{self.connection_status.name}'")
            return
        if not self.connection:
            logger.error(f"UserHubStream (Acct: {self.account_id}): Critical error - cannot send subscriptions, connection object is None despite state being CONNECTED.")
            return
            
        try:
            logger.info(f"UserHubStream (Acct: {self.account_id}): Sending subscriptions to User Hub. "
                        f"Account ID for specific subs: {self.account_id if self.account_id > 0 else 'N/A (global subs only if any)'}.")
            
            time_between_sends = 0.2 
            subs_sent_count = 0
            
            if self.subscribe_to_accounts_globally and self._callbacks["account"]:
                logger.debug(f"UserHubStream (Acct: {self.account_id}): Attempting to subscribe to Accounts (global).")
                self.connection.send("SubscribeAccounts", []) 
                subs_sent_count+=1
                time.sleep(time_between_sends) 

            if self.account_id > 0:
                if self._callbacks["order"]:
                    logger.debug(f"UserHubStream (Acct: {self.account_id}): Attempting to subscribe to Orders for Account ID: {self.account_id}.")
                    self.connection.send("SubscribeOrders", [self.account_id]) 
                    subs_sent_count+=1
                    time.sleep(time_between_sends)
                if self._callbacks["position"]:
                    logger.debug(f"UserHubStream (Acct: {self.account_id}): Attempting to subscribe to Positions for Account ID: {self.account_id}.")
                    self.connection.send("SubscribePositions", [self.account_id])
                    subs_sent_count+=1
                    time.sleep(time_between_sends)
                if self._callbacks["user_trade"]:
                    logger.debug(f"UserHubStream (Acct: {self.account_id}): Attempting to subscribe to UserTrades for Account ID: {self.account_id}.")
                    self.connection.send("SubscribeTrades", [self.account_id]) 
                    subs_sent_count+=1
            elif self._callbacks["order"] or self._callbacks["position"] or self._callbacks["user_trade"]:
                 logger.warning(f"UserHubStream (Acct: {self.account_id}): Callbacks for Orders/Positions/UserTrades are set, but account_id ({self.account_id}) "
                                "is not a valid positive ID. These specific subscriptions will not be sent.")
            
            if subs_sent_count > 0:
                logger.info(f"UserHubStream (Acct: {self.account_id}): Finished sending {subs_sent_count} configured subscription request(s).")
            else:
                logger.info(f"UserHubStream (Acct: {self.account_id}): No subscriptions were sent (either not configured with callbacks, "
                            "or account_id was invalid for account-specific subscriptions).")
        except Exception as e_sub_send: 
            logger.error(f"UserHubStream (Acct: {self.account_id}): Exception occurred while sending subscriptions to User Hub: {e_sub_send}", exc_info=True)
            self._set_connection_state(StreamConnectionState.ERROR, f"Exception sending subscriptions: {type(e_sub_send).__name__}")


    def start(self) -> bool:
        """Starts the SignalR connection to the User Hub."""
        logger.info(f"UserHubStream (Acct: {self.account_id}): Start called. Current state: {self.connection_status.name}")
        
        if not self.connection:
             if not self._wss_hub_url_no_token: self._prepare_websocket_url_base()
             if not self._current_token_for_url:
                try:
                    self._reinitialize_token_and_url()
                except Exception as e_reinit_start: 
                    logger.error(f"UserHubStream (Acct: {self.account_id}): Failed to re-initialize token/URL during start(): {e_reinit_start}. Cannot start stream.", exc_info=True)
                    self._set_connection_state(StreamConnectionState.ERROR, f"Token fetch/URL prep failed in start: {type(e_reinit_start).__name__}")
                    return False
             self._build_connection()
             if not self.connection:
                  logger.error(f"UserHubStream (Acct: {self.account_id}): Connection object is still None after attempting build in start(). Cannot start stream."); return False
        
        if self._current_state == StreamConnectionState.CONNECTED:
            logger.warning(f"UserHubStream (Acct: {self.account_id}): Start called but stream is already CONNECTED.")
            return True
        if self._current_state == StreamConnectionState.CONNECTING:
            logger.warning(f"UserHubStream (Acct: {self.account_id}): Start called but stream is already CONNECTING.")
            return True 
        
        self._set_connection_state(StreamConnectionState.CONNECTING, "Start method initiated connection attempt")
        logger.info(f"Attempting to start UserHubStream SignalR connection for Account ID: {self.account_id}...")
        try:
            if not self._hub_url_with_token:
                logger.info(f"UserHubStream (Acct: {self.account_id}): Hub URL with token not ready, reinitializing before start.")
                self._reinitialize_token_and_url() 
                self._build_connection()
            if not self.connection:
                logger.error(f"UserHubStream (Acct: {self.account_id}): Connection object is None even after re-init attempt in start(). Cannot start."); return False
            
            self.connection.start() 
            logger.info(f"UserHubStream (Acct: {self.account_id}): SignalR connection process initiated. Waiting for _on_open.")
            return True
        except Exception as e_conn_start: 
            logger.error(f"UserHubStream (Acct: {self.account_id}): Failed to initiate SignalR connection start: {e_conn_start}", exc_info=True)
            self._set_connection_state(StreamConnectionState.ERROR, f"Exception during start: {str(e_conn_start)[:50]}")
            return False

    def stop(self, reason_for_stop: Optional[str] = "User requested stop"):
        """
        Stops the SignalR connection to the User Hub.

        Args:
            reason_for_stop (Optional[str], optional): A string describing why stop is called,
                used for logging and status updates. Defaults to "User requested stop".
        """
        logger.info(f"UserHubStream (Acct: {self.account_id}): Stop called. Reason: '{reason_for_stop}'. "
                    f"Current state: {self._current_state.name}")
        
        if self._current_state == StreamConnectionState.DISCONNECTED or self._current_state == StreamConnectionState.ERROR:
            logger.info(f"UserHubStream (Acct: {self.account_id}): Stop called but stream already {self._current_state.name}. No action taken.")
            return

        self._set_connection_state(StreamConnectionState.STOPPING, reason_for_stop)
        
        if self.connection:
            logger.debug(f"UserHubStream (Acct: {self.account_id}): Calling stop on underlying SignalR connection object...")
            try: self.connection.stop()
            except Exception as e_conn_stop:
                logger.error(f"Error stopping UserHubStream (Acct: {self.account_id}) connection: {e_conn_stop}", exc_info=True)
                self._set_connection_state(StreamConnectionState.ERROR, f"Exception during stop: {str(e_conn_stop)[:50]}")
        else:
            logger.info(f"UserHubStream (Acct: {self.account_id}): No active SignalR connection object to stop (was None). Setting state to DISCONNECTED.")
            self._set_connection_state(StreamConnectionState.DISCONNECTED, "No connection object to stop")