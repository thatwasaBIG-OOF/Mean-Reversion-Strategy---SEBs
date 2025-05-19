# topstep_data_suite/src/tsxapipy/real_time/user_hub_stream.py
import logging
from typing import Callable, Optional, Any, List, Dict 
import time 

from signalrcore.hub_connection_builder import HubConnectionBuilder
from tsxapipy.config import USER_HUB_URL # Uses config var for User Hub URL

logger = logging.getLogger(__name__)

# Define callback type aliases for better readability and type checking
AccountUpdateCallback = Callable[[Dict[str, Any]], None]
"""Callable signature for account update events. Receives a dictionary of account data."""
OrderUpdateCallback = Callable[[Dict[str, Any]], None]
"""Callable signature for order update events. Receives a dictionary of order data."""
PositionUpdateCallback = Callable[[Dict[str, Any]], None]
"""Callable signature for position update events. Receives a dictionary of position data."""
UserTradeCallback = Callable[[Dict[str, Any]], None]
"""Callable signature for user's own trade execution events. Receives a dictionary of trade data."""

class UserHubStream:
    """
    Manages a SignalR connection to the TopStep User Hub for real-time user-specific data.

    This class handles connecting to the User Hub, subscribing to various
    account-specific events (orders, account details, positions, user trades based on API docs),
    and invoking user-provided callbacks when these events are received.
    It supports automatic reconnection and includes a method to update the
    authentication token, which necessitates restarting the stream if the token is part of the URL.

    Attributes:
        token (str): The authentication token used for the SignalR connection URL.
        account_id (int): The account ID for which specific event subscriptions are made.
            A value of 0 or less might be used for global subscriptions if supported by the API.
        connection (Optional[HubConnectionBuilder]): The SignalR hub connection object from `signalrcore`.
        subscribe_to_accounts_globally (bool): Flag indicating whether to attempt a global account subscription.
    """
    def __init__(self,
                 token: str,
                 account_id_to_watch: int, # Used for targeted subscriptions
                 on_order_update: Optional[OrderUpdateCallback] = None,
                 on_account_update: Optional[AccountUpdateCallback] = None,
                 on_position_update: Optional[PositionUpdateCallback] = None,
                 on_user_trade_update: Optional[UserTradeCallback] = None,
                 subscribe_to_accounts_globally: bool = True):
        """Initializes the UserHubStream.

        Args:
            token (str): The authenticated session token for the SignalR connection URL.
            account_id_to_watch (int): The specific account ID for which to subscribe
                to events like orders, positions, and user trades. If intending only global
                subscriptions (like SubscribeAccounts), this might be a placeholder if the API allows.
            on_order_update (Optional[OrderUpdateCallback]): Callback for `GatewayUserOrder` events.
            on_account_update (Optional[AccountUpdateCallback]): Callback for `GatewayUserAccount` events.
            on_position_update (Optional[PositionUpdateCallback]): Callback for `GatewayUserPosition` events.
            on_user_trade_update (Optional[UserTradeCallback]): Callback for `GatewayUserTrade` events
                (user's own trade executions).
            subscribe_to_accounts_globally (bool): If True and `on_account_update` is set,
                sends a global `SubscribeAccounts` message. API documentation (page 26)
                suggests `SubscribeAccounts` might not take an accountId. Defaults to True.

        Raises:
            ValueError: If `token` is empty.
        """
        if not token:
            raise ValueError("Authentication token cannot be empty for UserHubStream.")
        # account_id_to_watch can be non-positive if only global subscriptions are desired,
        # but specific subscriptions will require a valid ID.
        if not isinstance(account_id_to_watch, int): # Check type
             raise ValueError(f"account_id_to_watch must be an integer, got {type(account_id_to_watch)}")
        if account_id_to_watch <= 0 and (on_order_update or on_position_update or on_user_trade_update):
            logger.warning(f"UserHubStream initialized with account_id_to_watch: {account_id_to_watch}. "
                           "Account-specific subscriptions (Orders, Positions, UserTrades) will likely fail "
                           "or not be sent if a positive account ID is required by the API for those.")
        
        self.token: str = token
        self.account_id: int = account_id_to_watch
        
        self._callbacks: Dict[str, Optional[Callable[[Dict[str, Any]], None]]] = {
            "order": on_order_update,
            "account": on_account_update,
            "position": on_position_update,
            "user_trade": on_user_trade_update
        }
        self.subscribe_to_accounts_globally: bool = subscribe_to_accounts_globally

        self.connection: Optional[HubConnectionBuilder] = None
        self._hub_url_with_token: str = f"{USER_HUB_URL}?access_token={self.token}"
        logger.debug(f"UserHubStream: Hub URL configured: {USER_HUB_URL}?access_token=TOKEN_HIDDEN")
        
        self._build_connection()

    def _build_connection(self):
        """Builds or rebuilds the SignalR connection object and registers event handlers."""
        logger.debug("UserHubStream: Building new SignalR connection object.")
        options = {
            # 'skip_negotiation': True, # Per JS examples; test if needed for python client
            # 'transport': signalrcore.TransportType.WEBSOCKETS, # To force websockets if default isn't
            # 'logger': logger, # To pass this module's logger to signalrcore for its logs
        }
        self.connection = HubConnectionBuilder() \
            .with_url(self._hub_url_with_token, options=options) \
            .with_automatic_reconnect({
                "type": "interval", 
                "keep_alive_interval": 10, # Seconds
                "intervals": [0, 2, 5, 10, 15, 30, 60, 120, 300, 600] # Retry intervals (seconds)
            }) \
            .build()

        self.connection.on_open(self._on_open)
        self.connection.on_close(self._on_close)
        self.connection.on_error(self._on_error)
        self.connection.on_reconnecting(lambda e: logger.warning(f"UserHubStream attempting to reconnect... Last error: {e or 'N/A'}"))
        self.connection.on_reconnected(lambda connection_id: self._on_reconnected(connection_id))

        if self._callbacks["account"]:
            self.connection.on("GatewayUserAccount", self._handle_gateway_user_account_event)
            logger.debug("UserHubStream: Handler registered for 'GatewayUserAccount'.")
        if self._callbacks["order"]:
            self.connection.on("GatewayUserOrder", self._handle_gateway_user_order_event)
            logger.debug("UserHubStream: Handler registered for 'GatewayUserOrder'.")
        if self._callbacks["position"]:
            self.connection.on("GatewayUserPosition", self._handle_gateway_user_position_event)
            logger.debug("UserHubStream: Handler registered for 'GatewayUserPosition'.")
        if self._callbacks["user_trade"]:
            self.connection.on("GatewayUserTrade", self._handle_gateway_user_trade_event)
            logger.debug("UserHubStream: Handler registered for 'GatewayUserTrade'.")

    def _on_reconnected(self, connection_id: Optional[str]):
        """Internal callback invoked by signalrcore when connection is successfully re-established."""
        logger.info(f"UserHubStream reconnected (Connection ID: {connection_id or 'N/A'}). "
                    "Re-issuing subscriptions (via on_open if library calls it, or explicitly if needed).")
        # `signalrcore` should call `on_open` after `on_reconnected`. If testing shows it doesn't,
        # then `self._send_subscriptions()` might need to be called directly here.
        
    def _parse_payload(self, args: List[Any], event_name: str) -> Optional[Dict[str, Any]]:
        """Helper to parse common SignalR list-wrapped dictionary payloads for User Hub."""
        logger.debug(f"UserHubStream: Raw {event_name} event args: {args}")
        if isinstance(args, list) and len(args) >= 1:
            if isinstance(args[0], dict):
                return args[0]
            else:
                logger.warning(f"UserHubStream: {event_name} event's first argument is not a dict: {type(args[0])}")
        elif isinstance(args, dict): # If payload comes directly as dict
            return args
        logger.warning(f"UserHubStream: Received {event_name} payload with unexpected structure: {args}")
        return None

    def _handle_gateway_user_account_event(self, args: List[Any]):
        """Internal handler for GatewayUserAccount; invokes user callback if payload is valid."""
        payload = self._parse_payload(args, "GatewayUserAccount")
        if payload and self._callbacks["account"]:
            try:
                self._callbacks["account"](payload)
            except Exception as e:
                logger.error(f"UserHubStream: Unhandled error in on_account_update callback: {e}", exc_info=True)

    def _handle_gateway_user_order_event(self, args: List[Any]):
        """Internal handler for GatewayUserOrder; invokes user callback if payload is valid."""
        payload = self._parse_payload(args, "GatewayUserOrder")
        if payload and self._callbacks["order"]:
            try:
                self._callbacks["order"](payload)
            except Exception as e:
                logger.error(f"UserHubStream: Unhandled error in on_order_update callback: {e}", exc_info=True)

    def _handle_gateway_user_position_event(self, args: List[Any]):
        """Internal handler for GatewayUserPosition; invokes user callback if payload is valid."""
        payload = self._parse_payload(args, "GatewayUserPosition")
        if payload and self._callbacks["position"]:
            try:
                self._callbacks["position"](payload)
            except Exception as e:
                logger.error(f"UserHubStream: Unhandled error in on_position_update callback: {e}", exc_info=True)
    
    def _handle_gateway_user_trade_event(self, args: List[Any]):
        """Internal handler for GatewayUserTrade; invokes user callback if payload is valid."""
        payload = self._parse_payload(args, "GatewayUserTrade")
        if payload and self._callbacks["user_trade"]:
            try:
                self._callbacks["user_trade"](payload)
            except Exception as e:
                logger.error(f"UserHubStream: Unhandled error in on_user_trade_update callback: {e}", exc_info=True)

    def _send_subscriptions(self):
        """Sends configured subscriptions to the User Hub after connection is established."""
        if not self.connection or not self.connection.transport_connected:
            logger.warning("UserHubStream: Cannot send subscriptions, not connected.")
            return
        try:
            logger.info(f"UserHubStream sending subscriptions. Account ID for specific subs: {self.account_id if self.account_id > 0 else 'N/A (global only)'}.")
            
            # Global account subscription (API Doc page 26: SubscribeAccounts has no accountId parameter)
            if self.subscribe_to_accounts_globally and self._callbacks["account"]:
                logger.info("UserHubStream: Sending 'SubscribeAccounts'.")
                self.connection.send("SubscribeAccounts", []) 
                time.sleep(0.2) 

            # Account-specific subscriptions (only if a valid account_id is present)
            if self.account_id > 0:
                if self._callbacks["order"]:
                    logger.info(f"UserHubStream: Sending 'SubscribeOrders' for Account ID: {self.account_id}")
                    self.connection.send("SubscribeOrders", [self.account_id])
                    time.sleep(0.2)
                if self._callbacks["position"]:
                    logger.info(f"UserHubStream: Sending 'SubscribePositions' for Account ID: {self.account_id}")
                    self.connection.send("SubscribePositions", [self.account_id])
                    time.sleep(0.2)
                if self._callbacks["user_trade"]:
                    logger.info(f"UserHubStream: Sending 'SubscribeTrades' for Account ID: {self.account_id}")
                    self.connection.send("SubscribeTrades", [self.account_id])
            elif self._callbacks["order"] or self._callbacks["position"] or self._callbacks["user_trade"]:
                 logger.warning(f"UserHubStream: Callbacks for Orders/Positions/UserTrades are set, but account_id ({self.account_id}) is not valid for these subscriptions. They will not be sent.")
            
            logger.info("UserHubStream: All relevant and possible subscriptions sent.")
        except Exception as e: 
            logger.error(f"UserHubStream: Error sending subscriptions: {e}", exc_info=True)

    def _on_open(self):
        """Callback invoked when the SignalR connection is opened; sends subscriptions."""
        conn_id_str = self.connection.connection_id if self.connection else "N/A"
        logger.info(f"UserHubStream successfully connected to User Hub (Connection ID: {conn_id_str}).")
        self._send_subscriptions()

    def _on_close(self):
        """Callback invoked when the SignalR connection is closed."""
        logger.info("UserHubStream connection to User Hub closed.")

    def _on_error(self, error: Any):
        """Callback invoked on SignalR connection errors."""
        logger.error(f"UserHubStream connection error: {error}")
        if hasattr(error, 'error') and error.error: 
            logger.error(f"UserHubStream server error detail from CompletionMessage: {error.error}")
        if hasattr(error, 'invocation_id') and error.invocation_id:
            logger.error(f"UserHubStream error associated with invocation ID: {error.invocation_id}")

    def update_token(self, new_token: str):
        """Updates the authentication token and restarts the SignalR connection.

        This is necessary if the session token expires, as the token is part of
        the connection URL for SignalR. The stream will attempt to stop the current
        connection, rebuild with the new token, and then restart.

        Args:
            new_token (str): The new, valid session token.
        """
        if not new_token:
            logger.error("UserHubStream: Attempted to update token with an empty new token. Ignoring.")
            return
        if new_token == self.token and self.connection and self.connection.transport_connected:
            logger.debug("UserHubStream: Token update requested with the same token and stream already connected. No action taken.")
            return

        logger.info("UserHubStream: Token update requested. Restarting connection with new token...")
        self.token = new_token
        self._hub_url_with_token = f"{USER_HUB_URL}?access_token={self.token}"
        
        if self.connection and self.connection.transport_connected:
            logger.info("UserHubStream: Stopping existing connection for token update...")
            try:
                self.connection.stop() 
            except Exception as e:
                logger.error(f"UserHubStream: Error stopping existing connection during token update: {e}")
        
        logger.info("UserHubStream: Rebuilding connection with new token...")
        self._build_connection() 
        self.start() 

    def start(self) -> bool:
        """Starts the SignalR connection to the User Hub.

        Returns:
            bool: True if the connection attempt was initiated successfully (handshake started),
                  False if there was an immediate issue preventing the start.
                  Note: Successful initiation does not guarantee the connection will ultimately
                  establish or that subscriptions will succeed; these are asynchronous.
        """
        if not self.connection:
            logger.error("UserHubStream: Connection object not properly initialized. Cannot start.")
            return False
        if self.connection.transport_connected:
            logger.warning("UserHubStream: Attempted to start an already connected stream.")
            return True
        
        logger.info(f"Starting UserHubStream connection for Account ID: {self.account_id}...")
        try:
            self.connection.start() 
            logger.info("UserHubStream connection process initiated (handshake started).")
            return True
        except Exception as e:
            logger.error(f"Failed to start UserHubStream connection: {e}", exc_info=True)
            return False

    def stop(self):
        """Stops the SignalR connection to the User Hub.

        Attempts to gracefully close the connection.
        """
        if self.connection:
            if self.connection.transport_connected:
                logger.info("Stopping UserHubStream connection...")
                try:
                    # TODO: Consider sending Unsubscribe messages here before stopping if API recommends/supports
                    # E.g., self.connection.send("UnsubscribeOrders", [self.account_id])
                    self.connection.stop() 
                    logger.info("UserHubStream connection stop request sent.")
                except Exception as e:
                    logger.error(f"Error while stopping UserHubStream connection: {e}", exc_info=True)
            else:
                logger.info("UserHubStream connection was not actively connected when stop was called.")
        else:
            logger.info("UserHubStream has no active connection object to stop (it was None).")