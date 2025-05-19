# tsxapipy/real_time/data_stream.py
import logging
import time
from typing import Callable, Optional, Any, List, Dict

from signalrcore.hub_connection_builder import HubConnectionBuilder 

from tsxapipy.config import MARKET_HUB_URL
from tsxapipy.api import APIClient 

logger = logging.getLogger(__name__)

# Callback type aliases
QuoteCallback = Callable[[Dict[str, Any]], None]
TradeCallback = Callable[[Dict[str, Any]], None]
DepthCallback = Callable[[Dict[str, Any]], None]
StreamErrorCallback = Callable[[Any], None] 
StreamStateChangeCallback = Callable[[str], None] 

class DataStream:
    """
    Manages a SignalR connection to the TopStep Market Hub for real-time market data.

    This class handles connecting (using skip_negotiation=True), subscribing to market events,
    and invoking user-provided callbacks. It requires an APIClient instance to obtain
    the initial authentication token. Token refresh for long-lived connections
    must be handled by the application by calling the `update_token()` method,
    which will stop, reconfigure the URL with the new token, and restart the stream.

    Attributes:
        api_client (APIClient): The API client instance used for initially obtaining tokens.
        contract_id (str): The contract ID for which to subscribe to market data.
        connection (Optional[Any]): The SignalR hub connection object.
        connection_status (str): Read-only property describing the current state of the connection.
    """
    def __init__(self,
                 api_client: APIClient,
                 contract_id_to_subscribe: str,
                 on_quote_callback: Optional[QuoteCallback] = None,
                 on_trade_callback: Optional[TradeCallback] = None,
                 on_depth_callback: Optional[DepthCallback] = None,
                 on_error_callback: Optional[StreamErrorCallback] = None,
                 on_state_change_callback: Optional[StreamStateChangeCallback] = None):
        if not isinstance(api_client, APIClient):
            raise TypeError("api_client must be an instance of APIClient.")
        self.api_client = api_client

        if not contract_id_to_subscribe:
            raise ValueError("Contract ID to subscribe cannot be empty for DataStream.")
        
        self.contract_id: str = contract_id_to_subscribe
        self._callbacks: Dict[str, Optional[Callable]] = {
            "quote": on_quote_callback,
            "trade": on_trade_callback,
            "depth": on_depth_callback,
            "error": on_error_callback,
            "state_change": on_state_change_callback
        }
        self.connection: Optional[Any] = None 
        self._http_hub_base_url: str = MARKET_HUB_URL 
        self._wss_hub_url_no_token: Optional[str] = None # Base WSS URL without token
        self._current_token_for_url: Optional[str] = None # Token currently used in _hub_url_with_token
        self._hub_url_with_token: Optional[str] = None # Full WSS URL including token

        self._connection_status: str = "disconnected"
        self._is_explicitly_stopped: bool = False

        logger.info(f"DataStream initialized for Contract: {self.contract_id}.")
        self._prepare_websocket_url_base() # Prepare WSS base URL
        # Initial token fetch and URL construction will happen before the first _build_connection
        # or can be triggered by the first start() or update_token()
        try:
            self._reinitialize_token_and_url() # Get initial token and build full URL
        except Exception as e:
            logger.error(f"DataStream ({self.contract_id}): Failed to get initial token during __init__: {e}", exc_info=True)
            # Connection will likely fail to build or start if token is None
            self._set_connection_status("error") 
            # Don't build connection if token setup failed
            return
            
        self._build_connection()

    @property
    def connection_status(self) -> str:
        return self._connection_status

    def _set_connection_status(self, new_status: str):
        if self._connection_status != new_status:
            old_status = self._connection_status
            self._connection_status = new_status
            logger.info(f"DataStream ({self.contract_id}): Connection status changed from '{old_status}' to '{new_status}'.")
            if self._callbacks["state_change"]:
                try:
                    self._callbacks["state_change"](new_status)
                except Exception as e:
                    logger.error(f"DataStream ({self.contract_id}): Error in on_state_change_callback for status '{new_status}': {e}", exc_info=True)

    def _prepare_websocket_url_base(self):
        """Prepares the base ws:// or wss:// URL (without token)."""
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
            else: 
                self._wss_hub_url_no_token = self._http_hub_base_url
                if not (scheme.lower().startswith("ws")):
                     logger.warning(f"DataStream ({self.contract_id}): Unexpected scheme '{scheme}'. Using URL directly for WebSocket: {self._wss_hub_url_no_token}")
        except ValueError:
            logger.error(f"DataStream ({self.contract_id}): Malformed http_hub_base_url '{self._http_hub_base_url}'. Cannot derive WebSocket URL base.")
            self._wss_hub_url_no_token = None
        
        if self._wss_hub_url_no_token:
            logger.debug(f"DataStream ({self.contract_id}): Prepared WebSocket base URL: {self._wss_hub_url_no_token}")

    def _reinitialize_token_and_url(self):
        """Fetches a fresh token from APIClient and rebuilds the full WebSocket URL."""
        logger.debug(f"DataStream ({self.contract_id}): Re-initializing token and WebSocket URL.")
        if not self.api_client:
            raise RuntimeError(f"DataStream ({self.contract_id}): APIClient not available for token re-initialization.")
        if not self._wss_hub_url_no_token:
            self._prepare_websocket_url_base() # Attempt to prepare base URL if not already
            if not self._wss_hub_url_no_token:
                 raise RuntimeError(f"DataStream ({self.contract_id}): Base WebSocket URL could not be prepared. Cannot form connection URL.")

        try:
            self._current_token_for_url = self.api_client.current_token # This ensures APIClient's token is fresh
            self._hub_url_with_token = f"{self._wss_hub_url_no_token}?access_token={self._current_token_for_url}"
            log_token = f"{self._current_token_for_url[:15]}..." if self._current_token_for_url else "None"
            logger.info(f"DataStream ({self.contract_id}): Token refreshed/set. Full WebSocket URL configured (token: {log_token}).")
        except Exception as e:
            logger.error(f"DataStream ({self.contract_id}): Failed to get token from APIClient for URL: {e}", exc_info=True)
            self._hub_url_with_token = None # Ensure URL is None if token fetch fails
            raise # Re-raise the exception (e.g., AuthenticationError)

    def _build_connection(self):
        logger.debug(f"DataStream ({self.contract_id}): Building new SignalR connection object.")
        
        if not self._hub_url_with_token: # This URL now includes the token
            logger.critical(f"DataStream ({self.contract_id}): Full WebSocket Hub URL with token is not prepared. Cannot build connection. Ensure token was fetched.")
            self._set_connection_status("error")
            return

        logger.info(f"DataStream ({self.contract_id}): Configuring SignalR connection with skip_negotiation=True to URL (token embedded).")
        connection_options = {
            # No accessTokenFactory needed here, as token is in the URL
            "headers": {
                "User-Agent": f"tsxapipy-DataStream/{self.api_client.session.headers.get('User-Agent', 'tsxapipy-unknown')}"
            },
            "skip_negotiation": True 
        }
        
        self.connection = HubConnectionBuilder() \
            .with_url(self._hub_url_with_token, options=connection_options) \
            .with_automatic_reconnect({
                "type": "interval", 
                "keep_alive_interval": 10, 
                "intervals": [0, 2000, 5000, 10000, 15000, 30000, 60000, 120000, 300000]
            }) \
            .build()
        
        self.connection.on_open(self._on_open)
        self.connection.on_close(self._on_close)
        self.connection.on_error(self._on_error)

        if self._callbacks["quote"]: self.connection.on("GatewayQuote", self._handle_gateway_quote_event)
        if self._callbacks["trade"]: self.connection.on("GatewayTrade", self._handle_gateway_trade_event)
        if self._callbacks["depth"]: self.connection.on("GatewayDepth", self._handle_gateway_depth_event)
        logger.debug(f"DataStream ({self.contract_id}): SignalR connection object built and event handlers registered.")

    def _on_open(self):
        conn_id_str = self.connection.connection_id if self.connection and hasattr(self.connection, 'connection_id') else "N/A"
        logger.info(f"DataStream ({self.contract_id}) CONNECTED to Market Hub (Client Conn ID: {conn_id_str}).")
        self._is_explicitly_stopped = False
        self._set_connection_status("connected")
        self._send_subscriptions()

    def _on_close(self):
        logger.critical(
            f"DataStream ({self.contract_id}): --- _on_close CALLED --- "
            f"Current explicit_stop: {self._is_explicitly_stopped}, "
            f"Current status: {self._connection_status}"
        )
        if not self._is_explicitly_stopped:
            self._set_connection_status("reconnecting") # Assume signalrcore will try to reconnect
        else:
            self._set_connection_status("disconnected")

    def _on_error(self, error: Any):
        logger.critical(
            f"DataStream ({self.contract_id}): --- _on_error CALLED --- Error: '{error}', Type: {type(error)}, "
            f"Current explicit_stop: {self._is_explicitly_stopped}, "
            f"Current status: {self._connection_status}"
        )
        if not self._is_explicitly_stopped:
            if self._connection_status not in ["reconnecting", "error", "disconnected_will_retry"]:
                self._set_connection_status("reconnecting")
            elif self._connection_status == "connected":
                 self._set_connection_status("reconnecting")
        else: 
            self._set_connection_status("error")
             
        if self._callbacks["error"]:
            try: self._callbacks["error"](error)
            except Exception as e_cb:
                logger.error(f"DataStream ({self.contract_id}): Exception in user's on_error_callback: {e_cb}", exc_info=True)

    def update_token(self, new_token_from_api_client: str):
        """
        Updates the stream's token and forces a reconnection cycle.
        This method should be called by the application when it knows the APIClient's
        session token has been refreshed.

        Args:
            new_token_from_api_client (str): The new, fresh token obtained from APIClient.
        """
        if not new_token_from_api_client:
            logger.error(f"DataStream ({self.contract_id}): update_token called with empty new token. Ignoring."); return
        
        if new_token_from_api_client == self._current_token_for_url and self.connection_status == "connected":
            logger.debug(f"DataStream ({self.contract_id}): update_token called with current token and stream connected. No forced restart.")
            return

        logger.info(f"DataStream ({self.contract_id}): Token update requested by application. New token (first 15): {new_token_from_api_client[:15]}...")
        
        self._current_token_for_url = new_token_from_api_client # Store the new token
        if not self._wss_hub_url_no_token: self._prepare_websocket_url_base() # Ensure base is ready
        if self._wss_hub_url_no_token: # If base URL is valid
            self._hub_url_with_token = f"{self._wss_hub_url_no_token}?access_token={self._current_token_for_url}"
            logger.info(f"DataStream ({self.contract_id}): New WebSocket URL with token configured. Forcing reconnect cycle.")
        else:
            logger.error(f"DataStream ({self.contract_id}): Cannot update WebSocket URL, base WSS URL is not set. Token update failed.")
            self._set_connection_status("error")
            return
        
        if self.connection:
            self.stop(is_internal_restart=True) 
        
        self._build_connection() # Rebuilds connection with the new _hub_url_with_token
        self.start()

    def _parse_market_payload(self, args: List[Any], event_name: str) -> Optional[Dict[str, Any]]:
        logger.debug(f"DataStream ({self.contract_id}): Raw {event_name} event args: {args}")
        payload_data: Optional[Dict[str, Any]] = None
        if isinstance(args, list) and len(args) > 0:
            if len(args) == 2 and isinstance(args[0], str) and isinstance(args[1], dict):
                event_contract_id, payload_data = args[0], args[1]
                if event_contract_id != self.contract_id: return None
            elif len(args) == 1 and isinstance(args[0], dict):
                payload_data = args[0]
            else: logger.warning(f"DataStream ({self.contract_id}): {event_name} args list has unexpected structure: {args}")
        elif isinstance(args, dict): payload_data = args
        if payload_data is not None: return payload_data
        logger.warning(f"DataStream ({self.contract_id}): Received {event_name} with unhandled arg structure: {args}")
        return None

    def _handle_gateway_quote_event(self, args: List[Any]):
        payload = self._parse_market_payload(args, "GatewayQuote")
        if payload and self._callbacks["quote"]:
            try: self._callbacks["quote"](payload)
            except Exception as e: logger.error(f"DataStream ({self.contract_id}): Error in on_quote_callback: {e}", exc_info=True)

    def _handle_gateway_trade_event(self, args: List[Any]):
        payload = self._parse_market_payload(args, "GatewayTrade")
        if payload and self._callbacks["trade"]:
            try: self._callbacks["trade"](payload)
            except Exception as e: logger.error(f"DataStream ({self.contract_id}): Error in on_trade_callback: {e}", exc_info=True)

    def _handle_gateway_depth_event(self, args: List[Any]):
        payload = self._parse_market_payload(args, "GatewayDepth")
        if payload and self._callbacks["depth"]:
            try: self._callbacks["depth"](payload)
            except Exception as e: logger.error(f"DataStream ({self.contract_id}): Error in on_depth_callback: {e}", exc_info=True)

    def _send_subscriptions(self):
        if self.connection_status != "connected":
            logger.warning(f"DataStream ({self.contract_id}): Cannot send subscriptions, not 'connected'. Status: '{self.connection_status}'")
            return
        if not self.connection:
            logger.error(f"DataStream ({self.contract_id}): Cannot send subscriptions, connection object is None despite status 'connected'.")
            return
        try:
            logger.info(f"DataStream ({self.contract_id}): Sending subscriptions to Market Hub.")
            time_between_sends = 0.5 
            subscriptions_attempted_count = 0
            if self._callbacks["quote"]:
                logger.debug(f"DataStream ({self.contract_id}): Subscribing to ContractQuotes.")
                self.connection.send("SubscribeContractQuotes", [self.contract_id])
                subscriptions_attempted_count += 1
                time.sleep(time_between_sends) 
            if self._callbacks["trade"]:
                logger.debug(f"DataStream ({self.contract_id}): Subscribing to ContractTrades.")
                self.connection.send("SubscribeContractTrades", [self.contract_id])
                subscriptions_attempted_count += 1
                time.sleep(time_between_sends)
            if self._callbacks["depth"]:
                logger.debug(f"DataStream ({self.contract_id}): Subscribing to ContractMarketDepth.")
                self.connection.send("SubscribeContractMarketDepth", [self.contract_id])
                subscriptions_attempted_count += 1
            if subscriptions_attempted_count > 0:
                logger.info(f"DataStream ({self.contract_id}): Finished sending {subscriptions_attempted_count} configured subscription(s).")
            else:
                logger.info(f"DataStream ({self.contract_id}): No data subscriptions were configured.")
        except Exception as e: 
            logger.error(f"DataStream ({self.contract_id}): Exception occurred while sending subscriptions: {e}", exc_info=True)
            self._set_connection_status("error")

    def start(self) -> bool:
        if not self.connection: # Check if _build_connection failed (e.g. bad URL prep or initial token error)
             if not self._wss_hub_url_no_token: self._prepare_websocket_url_base()
             if not self._current_token_for_url: # Token not fetched in init (e.g. due to error)
                try: self._reinitialize_token_and_url()
                except Exception as e:
                    logger.error(f"DataStream ({self.contract_id}): Failed to get token during start(): {e}. Cannot start stream.", exc_info=True)
                    self._set_connection_status("error")
                    return False
             self._build_connection() # Try to build it now
             if not self.connection: # Still no connection object
                  logger.error(f"DataStream ({self.contract_id}): Connection object still None after attempting build in start(). Cannot start."); return False
        
        if self.connection_status in ["connected", "connecting", "reconnecting"]:
            logger.warning(f"DataStream ({self.contract_id}): Start called but stream is already {self.connection_status}.")
            return self.connection_status == "connected" or self.connection_status == "connecting"
        
        self._is_explicitly_stopped = False
        self._set_connection_status("connecting")
        logger.info(f"Attempting to start DataStream connection for Contract: {self.contract_id}...")
        try:
            # It's possible the connection object was built but token failed, so URL is None
            if not self._hub_url_with_token:
                logger.info(f"DataStream ({self.contract_id}): URL with token not ready, reinitializing before start.")
                self._reinitialize_token_and_url() # Ensure URL is ready
                self._build_connection() # Rebuild with the fresh URL if needed
            
            if not self.connection: # Final check after potential rebuild
                logger.error(f"DataStream ({self.contract_id}): Connection object is None even after re-init attempt in start(). Cannot start."); return False

            self.connection.start() 
            logger.info(f"DataStream ({self.contract_id}): SignalR connection process initiated. Waiting for _on_open callback.")
            return True
        except Exception as e:
            logger.error(f"DataStream ({self.contract_id}): Failed to initiate connection start: {e}", exc_info=True)
            self._set_connection_status("error")
            return False

    def stop(self, is_internal_restart: bool = False):
        if not is_internal_restart:
            logger.info(f"DataStream ({self.contract_id}): User-initiated stop requested.")
            self._is_explicitly_stopped = True 
        
        if self.connection:
            logger.debug(f"DataStream ({self.contract_id}): Calling stop on underlying SignalR connection object...")
            try:
                self.connection.stop()
                logger.info(f"DataStream ({self.contract_id}): SignalR connection stop request sent.")
            except Exception as e:
                logger.error(f"Error stopping DataStream ({self.contract_id}) connection: {e}", exc_info=True)
        else:
            logger.info(f"DataStream ({self.contract_id}): No active connection object to stop (was None).")
        
        if self._is_explicitly_stopped and not is_internal_restart: # If user explicitly stopped
            self._set_connection_status("disconnected")