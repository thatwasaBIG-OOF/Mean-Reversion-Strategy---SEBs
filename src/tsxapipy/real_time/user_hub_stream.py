# tsxapipy/real_time/user_hub_stream.py
import logging
import time
from typing import Callable, Optional, Any, List, Dict

from signalrcore.hub_connection_builder import HubConnectionBuilder

from tsxapipy.config import USER_HUB_URL # Uses config var for User Hub URL
from tsxapipy.api import APIClient # For type hinting and usage

logger = logging.getLogger(__name__)

# Callback type aliases
AccountUpdateCallback = Callable[[Dict[str, Any]], None]
OrderUpdateCallback = Callable[[Dict[str, Any]], None]
PositionUpdateCallback = Callable[[Dict[str, Any]], None]
UserTradeCallback = Callable[[Dict[str, Any]], None]
StreamErrorCallback = Callable[[Any], None]
StreamStateChangeCallback = Callable[[str], None]

class UserHubStream:
    """
    Manages a SignalR connection to the TopStep User Hub for real-time user-specific data.

    This class handles connecting (using skip_negotiation=True), subscribing to various
    account-specific events (orders, account details, positions, user trades),
    and invoking user-provided callbacks. It requires an APIClient instance to obtain
    the initial authentication token. Token refresh for long-lived connections
    must be handled by the application by calling the `update_token()` method.

    Attributes:
        api_client (APIClient): The API client instance used for initially obtaining tokens.
        account_id (int): The account ID for specific event subscriptions.
        connection (Optional[Any]): The SignalR hub connection object.
        connection_status (str): Read-only property describing the current state of the connection.
    """
    def __init__(self,
                 api_client: APIClient,
                 account_id_to_watch: int,
                 on_order_update: Optional[OrderUpdateCallback] = None,
                 on_account_update: Optional[AccountUpdateCallback] = None,
                 on_position_update: Optional[PositionUpdateCallback] = None,
                 on_user_trade_update: Optional[UserTradeCallback] = None,
                 subscribe_to_accounts_globally: bool = True, # For "SubscribeAccounts"
                 on_error_callback: Optional[StreamErrorCallback] = None,
                 on_state_change_callback: Optional[StreamStateChangeCallback] = None):
        if not isinstance(api_client, APIClient):
            raise TypeError("api_client must be an instance of APIClient.")
        self.api_client = api_client

        if not isinstance(account_id_to_watch, int):
             raise ValueError(f"account_id_to_watch must be an integer, got {type(account_id_to_watch)}")
        # A non-positive account_id might be valid if only using subscribe_to_accounts_globally
        if account_id_to_watch <= 0 and (on_order_update or on_position_update or on_user_trade_update):
            logger.warning(f"UserHubStream initialized with account_id_to_watch: {account_id_to_watch}. "
                           "Account-specific subscriptions (Orders, Positions, UserTrades) may not be sent "
                           "or may fail if a positive ID is required by the API for those.")
        
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
        self._connection_status: str = "disconnected"
        self._is_explicitly_stopped: bool = False

        logger.info(f"UserHubStream initialized for Account ID: {self.account_id}. Token will be embedded in URL.")
        self._prepare_websocket_url_base()
        try:
            self._reinitialize_token_and_url()
        except Exception as e:
            logger.error(f"UserHubStream (Acct: {self.account_id}): Failed to get initial token during __init__: {e}", exc_info=True)
            self._set_connection_status("error")
            return
            
        self._build_connection()

    @property
    def connection_status(self) -> str:
        return self._connection_status

    def _set_connection_status(self, new_status: str):
        if self._connection_status != new_status:
            old_status = self._connection_status
            self._connection_status = new_status
            logger.info(f"UserHubStream (Acct: {self.account_id}): Connection status changed from '{old_status}' to '{new_status}'.")
            if self._callbacks["state_change"]:
                try:
                    self._callbacks["state_change"](new_status)
                except Exception as e:
                    logger.error(f"UserHubStream (Acct: {self.account_id}): Error in on_state_change_callback for '{new_status}': {e}", exc_info=True)

    def _prepare_websocket_url_base(self):
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
                     logger.warning(f"UserHubStream (Acct: {self.account_id}): Unexpected scheme '{scheme}'. Using URL directly for WebSocket: {self._wss_hub_url_no_token}")
        except ValueError:
            logger.error(f"UserHubStream (Acct: {self.account_id}): Malformed http_hub_base_url '{self._http_hub_base_url}'. Cannot derive WebSocket URL base.")
            self._wss_hub_url_no_token = None
        if self._wss_hub_url_no_token:
            logger.debug(f"UserHubStream (Acct: {self.account_id}): Prepared WebSocket base URL: {self._wss_hub_url_no_token}")

    def _reinitialize_token_and_url(self):
        logger.debug(f"UserHubStream (Acct: {self.account_id}): Re-initializing token and WebSocket URL.")
        if not self.api_client: raise RuntimeError(f"UserHubStream (Acct: {self.account_id}): APIClient not available.")
        if not self._wss_hub_url_no_token:
            self._prepare_websocket_url_base()
            if not self._wss_hub_url_no_token: raise RuntimeError(f"UserHubStream (Acct: {self.account_id}): Base WebSocket URL could not be prepared.")
        try:
            self._current_token_for_url = self.api_client.current_token
            self._hub_url_with_token = f"{self._wss_hub_url_no_token}?access_token={self._current_token_for_url}"
            log_token = f"{self._current_token_for_url[:15]}..." if self._current_token_for_url else "None"
            logger.info(f"UserHubStream (Acct: {self.account_id}): Token refreshed/set. Full WebSocket URL configured (token: {log_token}).")
        except Exception as e:
            logger.error(f"UserHubStream (Acct: {self.account_id}): Failed to get token from APIClient for URL: {e}", exc_info=True)
            self._hub_url_with_token = None
            raise

    def _build_connection(self):
        logger.debug(f"UserHubStream (Acct: {self.account_id}): Building new SignalR connection object.")
        if not self._hub_url_with_token:
            logger.critical(f"UserHubStream (Acct: {self.account_id}): Full WebSocket Hub URL with token is not prepared. Cannot build connection.")
            self._set_connection_status("error")
            return

        logger.info(f"UserHubStream (Acct: {self.account_id}): Configuring SignalR connection with skip_negotiation=True to URL (token embedded).")
        connection_options = {
            "headers": {"User-Agent": f"tsxapipy-UserHubStream/{self.api_client.session.headers.get('User-Agent', 'tsxapipy-unknown')}"},
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
        conn_id_str = self.connection.connection_id if self.connection and hasattr(self.connection, 'connection_id') else "N/A"
        logger.info(f"UserHubStream (Acct: {self.account_id}) CONNECTED to User Hub (Client Conn ID: {conn_id_str}).")
        self._is_explicitly_stopped = False
        self._set_connection_status("connected")
        self._send_subscriptions()

    def _on_close(self):
        logger.critical(
            f"UserHubStream (Acct: {self.account_id}): --- _on_close CALLED --- "
            f"Explicit stop: {self._is_explicitly_stopped}, Status: {self._connection_status}"
        )
        if not self._is_explicitly_stopped: self._set_connection_status("reconnecting") 
        else: self._set_connection_status("disconnected")

    def _on_error(self, error: Any):
        logger.critical(
            f"UserHubStream (Acct: {self.account_id}): --- _on_error CALLED --- Error: '{error}', Type: {type(error)}, "
            f"Explicit stop: {self._is_explicitly_stopped}, Status: {self._connection_status}"
        )
        if not self._is_explicitly_stopped:
            if self._connection_status not in ["reconnecting", "error", "disconnected_will_retry"]:
                self._set_connection_status("reconnecting")
            elif self._connection_status == "connected": self._set_connection_status("reconnecting")
        else: self._set_connection_status("error")
        if self._callbacks["error"]:
            try: self._callbacks["error"](error)
            except Exception as e_cb: logger.error(f"UserHubStream (Acct: {self.account_id}): Exc in on_error_callback: {e_cb}", exc_info=True)

    def update_token(self, new_token_from_api_client: str):
        if not new_token_from_api_client:
            logger.error(f"UserHubStream (Acct: {self.account_id}): update_token called with empty new token. Ignoring."); return
        if new_token_from_api_client == self._current_token_for_url and self.connection_status == "connected":
            logger.debug(f"UserHubStream (Acct: {self.account_id}): update_token called with current token and stream connected. No forced restart.")
            return
        logger.info(f"UserHubStream (Acct: {self.account_id}): Token update requested by application. New token (first 15): {new_token_from_api_client[:15]}...")
        self._current_token_for_url = new_token_from_api_client
        if not self._wss_hub_url_no_token: self._prepare_websocket_url_base()
        if self._wss_hub_url_no_token:
            self._hub_url_with_token = f"{self._wss_hub_url_no_token}?access_token={self._current_token_for_url}"
            logger.info(f"UserHubStream (Acct: {self.account_id}): New WebSocket URL with token configured. Forcing reconnect cycle.")
        else:
            logger.error(f"UserHubStream (Acct: {self.account_id}): Cannot update WebSocket URL, base WSS URL is not set. Token update failed.")
            self._set_connection_status("error"); return
        if self.connection: self.stop(is_internal_restart=True) 
        self._build_connection()
        self.start()

    def _parse_user_payload(self, args: List[Any], event_name: str) -> Optional[Dict[str, Any]]:
        logger.debug(f"UserHubStream (Acct: {self.account_id}): Raw {event_name} event args: {args}")
        if isinstance(args, list) and len(args) >= 1:
            if isinstance(args[0], dict): return args[0]
            else: logger.warning(f"UserHubStream (Acct: {self.account_id}): {event_name} arg[0] not dict: {type(args[0])}")
        elif isinstance(args, dict): return args
        logger.warning(f"UserHubStream (Acct: {self.account_id}): Received {event_name} with unhandled arg structure: {args}")
        return None

    def _handle_gateway_user_account_event(self, args: List[Any]):
        payload = self._parse_user_payload(args, "GatewayUserAccount")
        if payload and self._callbacks["account"]:
            try: self._callbacks["account"](payload)
            except Exception as e: logger.error(f"UserHubStream (Acct: {self.account_id}): Error in on_account_update: {e}", exc_info=True)

    def _handle_gateway_user_order_event(self, args: List[Any]):
        payload = self._parse_user_payload(args, "GatewayUserOrder")
        if payload and self._callbacks["order"]:
            try: self._callbacks["order"](payload)
            except Exception as e: logger.error(f"UserHubStream (Acct: {self.account_id}): Error in on_order_update: {e}", exc_info=True)

    def _handle_gateway_user_position_event(self, args: List[Any]):
        payload = self._parse_user_payload(args, "GatewayUserPosition")
        if payload and self._callbacks["position"]:
            try: self._callbacks["position"](payload)
            except Exception as e: logger.error(f"UserHubStream (Acct: {self.account_id}): Error in on_position_update: {e}", exc_info=True)

    def _handle_gateway_user_trade_event(self, args: List[Any]):
        payload = self._parse_user_payload(args, "GatewayUserTrade")
        if payload and self._callbacks["user_trade"]:
            try: self._callbacks["user_trade"](payload)
            except Exception as e: logger.error(f"UserHubStream (Acct: {self.account_id}): Error in on_user_trade_update: {e}", exc_info=True)

    def _send_subscriptions(self):
        if self.connection_status != "connected":
            logger.warning(f"UserHubStream (Acct: {self.account_id}): Cannot send subscriptions, not 'connected'. Status: '{self.connection_status}'")
            return
        if not self.connection:
             logger.error(f"UserHubStream (Acct: {self.account_id}): Cannot send subscriptions, connection object is None despite status 'connected'.")
             return
        try:
            logger.info(f"UserHubStream (Acct: {self.account_id}): Sending subscriptions to User Hub.")
            time_between_sends = 0.2
            subs_sent_count = 0
            if self.subscribe_to_accounts_globally and self._callbacks["account"]:
                logger.debug(f"UserHubStream (Acct: {self.account_id}): Subscribing to Accounts (global).")
                self.connection.send("SubscribeAccounts", []); subs_sent_count+=1; time.sleep(time_between_sends)
            if self.account_id > 0:
                if self._callbacks["order"]:
                    logger.debug(f"UserHubStream (Acct: {self.account_id}): Subscribing to Orders.")
                    self.connection.send("SubscribeOrders", [self.account_id]); subs_sent_count+=1; time.sleep(time_between_sends)
                if self._callbacks["position"]:
                    logger.debug(f"UserHubStream (Acct: {self.account_id}): Subscribing to Positions.")
                    self.connection.send("SubscribePositions", [self.account_id]); subs_sent_count+=1; time.sleep(time_between_sends)
                if self._callbacks["user_trade"]:
                    logger.debug(f"UserHubStream (Acct: {self.account_id}): Subscribing to UserTrades.")
                    self.connection.send("SubscribeTrades", [self.account_id]); subs_sent_count+=1
            elif self._callbacks["order"] or self._callbacks["position"] or self._callbacks["user_trade"]:
                 logger.warning(f"UserHubStream: Callbacks for account-specific events set, but account_id ({self.account_id}) is invalid. These subscriptions not sent.")
            if subs_sent_count > 0: logger.info(f"UserHubStream (Acct: {self.account_id}): Finished sending {subs_sent_count} subscription(s).")
            else: logger.info(f"UserHubStream (Acct: {self.account_id}): No subscriptions were sent (either not configured or invalid account_id for some).")
        except Exception as e:
            logger.error(f"UserHubStream (Acct: {self.account_id}): Exception sending subscriptions: {e}", exc_info=True)
            self._set_connection_status("error")

    def start(self) -> bool:
        if not self.connection:
             if not self._wss_hub_url_no_token: self._prepare_websocket_url_base()
             if not self._current_token_for_url:
                try: self._reinitialize_token_and_url()
                except Exception as e: logger.error(f"UserHubStream (Acct: {self.account_id}): Failed to get token during start(): {e}. Cannot start.", exc_info=True); self._set_connection_status("error"); return False
             self._build_connection()
             if not self.connection: logger.error(f"UserHubStream (Acct: {self.account_id}): Connection object still None after build in start(). Cannot start."); return False
        
        if self.connection_status in ["connected", "connecting", "reconnecting"]:
            logger.warning(f"UserHubStream (Acct: {self.account_id}): Start called but stream is already {self.connection_status}.")
            return self.connection_status == "connected" or self.connection_status == "connecting"
        
        self._is_explicitly_stopped = False
        self._set_connection_status("connecting")
        logger.info(f"Attempting to start UserHubStream connection for Account ID: {self.account_id}...")
        try:
            if not self._hub_url_with_token: # Should have been set by _reinitialize_token_and_url
                logger.info(f"UserHubStream (Acct: {self.account_id}): URL with token not ready, reinitializing before start.")
                self._reinitialize_token_and_url() 
                self._build_connection() # Rebuild with the fresh URL
            if not self.connection: logger.error(f"UserHubStream (Acct: {self.account_id}): Connection is None even after re-init in start(). Cannot start."); return False
            self.connection.start() 
            logger.info(f"UserHubStream (Acct: {self.account_id}): SignalR connection process initiated.")
            return True
        except Exception as e:
            logger.error(f"UserHubStream (Acct: {self.account_id}): Failed to initiate connection start: {e}", exc_info=True)
            self._set_connection_status("error")
            return False

    def stop(self, is_internal_restart: bool = False):
        if not is_internal_restart:
            logger.info(f"UserHubStream (Acct: {self.account_id}): User-initiated stop requested.")
            self._is_explicitly_stopped = True 
        if self.connection:
            logger.debug(f"UserHubStream (Acct: {self.account_id}): Calling stop on underlying SignalR connection object...")
            try: self.connection.stop()
            except Exception as e: logger.error(f"Error stopping UserHubStream (Acct: {self.account_id}) connection: {e}", exc_info=True)
        else: logger.info(f"UserHubStream (Acct: {self.account_id}): No active connection object to stop (was None).")
        if self._is_explicitly_stopped and not is_internal_restart:
            self._set_connection_status("disconnected")