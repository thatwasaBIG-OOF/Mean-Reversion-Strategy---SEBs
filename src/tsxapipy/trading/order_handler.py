# topstep_data_suite/src/tsxapipy/trading/order_handler.py
import logging
from typing import Dict, Any, Optional, List # Added List from TradingBot needs
from datetime import datetime, timedelta # For get_order_details search window

from tsxapipy.api.client import APIClient
from tsxapipy.api.exceptions import APIError, InvalidParameterError, OrderNotFoundError, OrderRejectedError
from tsxapipy.common.time_utils import UTC_TZ

logger = logging.getLogger(__name__)

# --- Standardized Order Type and Side Mappings (from API docs page 5 & 15) ---
ORDER_TYPES_MAP: Dict[str, int] = {
    "LIMIT": 1,
    "MARKET": 2,
    "STOP": 3, # Stop Market (API type 3 appears to be Stop Market based on common usage)
    # API Docs also mention: TRAILING_STOP: 5, JOIN_BID: 6, JOIN_ASK: 7
    # To support these, add them to the map and create specific placer methods if desired.
}
"""Maps user-friendly order type strings (e.g., "MARKET") to API integer codes."""

ORDER_SIDES_MAP: Dict[str, int] = {
    "BUY": 0,  # API: Bid (buy)
    "SELL": 1, # API: Ask (sell)
}
"""Maps user-friendly order side strings (e.g., "BUY") to API integer codes."""

# --- Order Status Constants ---
# CRUCIAL: These integer values are HYPOTHETICAL examples or based on limited documentation.
# You MUST verify these exact integer codes against actual API responses for all states.
ORDER_STATUS_PENDING_NEW = 0      # Hypothetical: Order sent, not yet acknowledged by exchange
ORDER_STATUS_NEW = 1              # Hypothetical: Order acknowledged, but not yet active (e.g. outside market hours)
ORDER_STATUS_WORKING = 2          # Based on API doc example (page 14 - likely active/open on book)
ORDER_STATUS_PARTIALLY_FILLED = 3 # Hypothetical - check if API uses a distinct status or just updates cumQuantity
ORDER_STATUS_FILLED = 4           # Hypothetical
ORDER_STATUS_PENDING_CANCEL = 5   # Hypothetical: Cancel request sent, not yet confirmed
ORDER_STATUS_CANCELLED = 6        # Hypothetical
ORDER_STATUS_REJECTED = 7         # Hypothetical
ORDER_STATUS_EXPIRED = 8          # Hypothetical
ORDER_STATUS_UNKNOWN = -1         # For cases where status isn't recognized or mapped

ORDER_STATUS_TO_STRING_MAP: Dict[int, str] = {
    v: k for k, v in locals().items() if k.startswith("ORDER_STATUS_") and isinstance(v, int)
}
"""Reverse mapping from order status integer codes to their string representations for logging/display."""


class OrderPlacer:
    """
    Handles placement and management of trading orders via the API.

    This class provides a higher-level interface for creating, cancelling,
    and modifying orders, using an underlying `APIClient` instance for
    communication. It also includes helpers for mapping user-friendly
    order parameters (like "MARKET", "BUY") to the integer codes required by the API.

    Attributes:
        api_client (APIClient): The API client instance used for all API interactions.
        account_id (int): The trading account ID for which orders will be managed.
        default_contract_id (Optional[str]): A default contract ID to use if not
            specified in individual order placement methods.
    """
    def __init__(self, api_client: APIClient,
                 account_id: int,
                 default_contract_id: Optional[str] = None):
        """Initializes the OrderPlacer.

        Args:
            api_client (APIClient): An initialized `APIClient` instance.
            account_id (int): The trading account ID to be used for all orders
                placed through this `OrderPlacer` instance.
            default_contract_id (Optional[str], optional): A contract ID to use
                by default if not provided in specific order methods. Defaults to None.

        Raises:
            TypeError: If `api_client` is not an instance of `APIClient`.
            ValueError: If `account_id` is not a positive integer.
        """
        if not isinstance(api_client, APIClient):
            raise TypeError("api_client must be an instance of APIClient.")
        if not isinstance(account_id, int) or account_id <= 0:
            raise ValueError("Account ID must be a positive integer for OrderPlacer.")

        self.api_client = api_client
        self.account_id = account_id
        self.default_contract_id = default_contract_id
        logger.info(f"OrderPlacer initialized for Account ID: {self.account_id} "
                    f"with default contract: {self.default_contract_id or 'Not set'}.")

    def _prepare_order_payload(self, contract_id: str, order_type_str: str, side_str: str, size: int,
                               limit_price: Optional[float] = None, 
                               stop_price: Optional[float] = None,
                               trail_price: Optional[float] = None,
                               custom_tag: Optional[str] = None,
                               linked_order_id: Optional[int] = None) -> Dict[str, Any]:
        """Prepares the JSON payload for an order placement API request.

        Validates and maps string order types/sides to API integer codes.
        Ensures numeric prices are floats or None.

        Args:
            contract_id (str): The contract ID for the order.
            order_type_str (str): User-friendly order type (e.g., "MARKET", "LIMIT", "STOP").
            side_str (str): User-friendly order side (e.g., "BUY", "SELL").
            size (int): Order quantity.
            limit_price (Optional[float]): Limit price for LIMIT orders.
            stop_price (Optional[float]): Stop price for STOP orders.
            trail_price (Optional[float]): Trail price/offset for TRAILING_STOP orders.
            custom_tag (Optional[str]): Optional custom tag for the order.
            linked_order_id (Optional[int]): Optional ID of a linked order (API key "linkedOrderld").

        Returns:
            Dict[str, Any]: The constructed payload dictionary for the API.

        Raises:
            ValueError: If `order_type_str` or `side_str` are not supported/mapped,
                        or if `size` is not a positive integer.
        """
        order_type_code = ORDER_TYPES_MAP.get(order_type_str.upper())
        if order_type_code is None:
            raise ValueError(f"Invalid order_type_str: '{order_type_str}'. Supported: {list(ORDER_TYPES_MAP.keys())}")
        
        side_code = ORDER_SIDES_MAP.get(side_str.upper())
        if side_code is None:
            raise ValueError(f"Invalid side_str: '{side_str}'. Supported: {list(ORDER_SIDES_MAP.keys())}")

        if not isinstance(size, int) or size <= 0:
            raise ValueError(f"Order size must be a positive integer, got {size}.")

        def _clean_price(price: Any) -> Optional[float]:
            if price is None:
                return None
            try:
                return float(price)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid price value '{price}', cannot convert to float.")

        payload: Dict[str, Any] = {
            "accountId": self.account_id,
            "contractId": contract_id,
            "type": order_type_code,
            "side": side_code,
            "size": size,
            "limitPrice": _clean_price(limit_price), # API expects null if not applicable
            "stopPrice": _clean_price(stop_price),   # API expects null if not applicable
            "trailPrice": _clean_price(trail_price), # API doc page 6 shows this
            "customTag": custom_tag if custom_tag else None, # Ensure null if empty
            "linkedOrderld": linked_order_id # API doc page 6 shows "linkedOrderld" (lowercase L, D)
        }
        return payload

    def place_order(self, contract_id: Optional[str], 
                    order_type: str, side: str, size: int,
                    limit_price: Optional[float] = None, stop_price: Optional[float] = None,
                    trail_price: Optional[float] = None,
                    custom_tag: Optional[str] = None, linked_order_id: Optional[int] = None) -> Optional[int]:
        """Places a generic order (Market, Limit, Stop, etc.) via the API.

        This method constructs the appropriate payload and calls the `place_order`
        method of the underlying `APIClient`.

        Args:
            contract_id (Optional[str]): The contract ID for the order. If None,
                `default_contract_id` of the `OrderPlacer` instance is used.
            order_type (str): The type of order (e.g., "MARKET", "LIMIT", "STOP").
                Must be a key in `ORDER_TYPES_MAP`.
            side (str): The side of the order ("BUY" or "SELL").
                Must be a key in `ORDER_SIDES_MAP`.
            size (int): The order quantity (number of contracts). Must be positive.
            limit_price (Optional[float]): The limit price, required for LIMIT orders.
            stop_price (Optional[float]): The stop price, required for STOP orders.
            trail_price (Optional[float]): The trail price/offset for TRAILING_STOP orders.
            custom_tag (Optional[str]): An optional custom string tag for the order.
            linked_order_id (Optional[int]): An optional ID of another order to link to
                (e.g., for OCO relationships if supported by the API).

        Returns:
            Optional[int]: The API-assigned `orderId` (integer) if the order placement
            request was successfully submitted and an ID was returned. Returns None if
            placement fails, if `contract_id` cannot be resolved, or if the API
            response does not indicate success or lacks an `orderId`.

        Raises:
            ValueError: If `order_type`, `side`, `size`, or price inputs are invalid during payload preparation.
            (Propagates exceptions from `self.api_client.place_order`, such as
             `InvalidParameterError`, `InsufficientFundsError`, `OrderRejectedError`,
             `RateLimitExceededError`, `APITimeoutError`, `APIHttpError`, `APIError`,
             `AuthenticationError`).
        """
        target_contract_id = contract_id if contract_id else self.default_contract_id
        if not target_contract_id:
            logger.error("OrderPlacer: Cannot place order. Contract ID is not specified and no default is set.")
            return None
        
        try:
            payload = self._prepare_order_payload(
                target_contract_id, order_type, side, size,
                limit_price, stop_price, trail_price, custom_tag, linked_order_id
            )
            
            logger.info(f"Placing {side} {order_type} order: {size} of {target_contract_id} on account {self.account_id}. Payload: {payload}")
            response_data = self.api_client.place_order(order_details=payload) # Uses APIClient
            
            order_id_from_response = response_data.get("orderId") # API doc page 16 shows "orderId"
            if response_data.get("success") and order_id_from_response is not None:
                try:
                    returned_order_id = int(order_id_from_response)
                    logger.info(f"[ORDER PLACED] {side} {order_type} {size} of {target_contract_id}. API Order ID: {returned_order_id}")
                    return returned_order_id
                except ValueError:
                    logger.error(f"[ORDER SUBMISSION FAILED] API returned non-integer orderId: '{order_id_from_response}' "
                                 f"for {side} {order_type} {size} of {target_contract_id}.")
                    return None
            else:
                err_msg = response_data.get('errorMessage', 'Order placement failed or orderId missing from API response.')
                logger.error(f"[ORDER SUBMISSION FAILED] {side} {order_type} {size} of {target_contract_id}. Reason: {err_msg}")
                return None
        except ValueError as ve: # From _prepare_order_payload or int(order_id_from_response)
            logger.error(f"[ORDER PREPARATION/RESPONSE ERROR] For {side} {order_type} order for {target_contract_id}: {ve}")
            # Re-raise if it's an input validation error to OrderPlacer itself.
            # If it's from int(order_id_from_response), it's an API response issue.
            if "order_type_str" in str(ve) or "side_str" in str(ve) or "Order size" in str(ve) or "Invalid price" in str(ve) :
                 raise
            return None 
        except APIError as apie: 
            logger.error(f"[ORDER API ERROR] Failed to place {side} {order_type} order for {target_contract_id}: {apie}")
            return None 
        except Exception as e: 
            logger.error(f"[ORDER UNEXPECTED ERROR] Failed to place {side} {order_type} order for {target_contract_id}: {e}", exc_info=True)
            return None

    def place_market_order(self, side: str, size: int = 1, contract_id: Optional[str] = None,
                           custom_tag: Optional[str] = None) -> Optional[int]:
        """Places a market order.

        Args:
            side (str): "BUY" or "SELL".
            size (int, optional): Order quantity. Defaults to 1. Must be positive.
            contract_id (Optional[str]): Contract ID. Uses instance default if None.
            custom_tag (Optional[str]): Optional custom tag.

        Returns:
            Optional[int]: API order ID if successful, else None.
        """
        return self.place_order(contract_id=contract_id, order_type="MARKET", 
                                side=side, size=size, custom_tag=custom_tag)

    def place_limit_order(self, side: str, size: int, limit_price: float, 
                          contract_id: Optional[str] = None,
                          custom_tag: Optional[str] = None, 
                          linked_order_id: Optional[int] = None) -> Optional[int]:
        """Places a limit order.

        Args:
            side (str): "BUY" or "SELL".
            size (int): Order quantity. Must be positive.
            limit_price (float): The limit price for the order. Must be positive.
            contract_id (Optional[str]): Contract ID. Uses instance default if None.
            custom_tag (Optional[str]): Optional custom tag.
            linked_order_id (Optional[int]): Optional linked order ID.

        Returns:
            Optional[int]: API order ID if successful, else None.
        """
        if not isinstance(limit_price, (int, float)) or limit_price <= 0:
            # This check is also in _prepare_order_payload, but good for early exit.
            logger.error(f"Invalid limit_price for limit order: {limit_price}. Must be a positive number.")
            return None # Or raise ValueError
        return self.place_order(contract_id=contract_id, order_type="LIMIT", 
                                side=side, size=size, limit_price=limit_price,
                                custom_tag=custom_tag, linked_order_id=linked_order_id)

    def place_stop_market_order(self, side: str, size: int, stop_price: float, 
                                contract_id: Optional[str] = None,
                                custom_tag: Optional[str] = None, 
                                linked_order_id: Optional[int] = None) -> Optional[int]:
        """Places a stop market order (API order type 3).

        A market order is triggered when the market price reaches the `stop_price`.

        Args:
            side (str): "BUY" (for buy-stop) or "SELL" (for sell-stop).
            size (int): Order quantity. Must be positive.
            stop_price (float): The stop price at which the market order will be triggered.
            contract_id (Optional[str]): Contract ID. Uses instance default if None.
            custom_tag (Optional[str]): Optional custom tag.
            linked_order_id (Optional[int]): Optional linked order ID.

        Returns:
            Optional[int]: API order ID if successful, else None.
        """
        if not isinstance(stop_price, (int, float)) or stop_price <= 0:
            logger.error(f"Invalid stop_price for stop order: {stop_price}. Must be positive.")
            return None # Or raise ValueError
        return self.place_order(contract_id=contract_id, order_type="STOP", 
                                side=side, size=size, stop_price=stop_price,
                                custom_tag=custom_tag, linked_order_id=linked_order_id)

    def cancel_order(self, order_id: int) -> bool:
        """Cancels an existing open order.

        Args:
            order_id (int): The ID of the order to cancel.

        Returns:
            bool: True if the API response indicates successful cancellation, False otherwise.
        
        Raises:
            (Propagates APIClient exceptions like OrderNotFoundError, APIError)
        """
        if not isinstance(order_id, int) or order_id <= 0:
            logger.error(f"Invalid order_id for cancellation: {order_id}")
            return False
        try:
            logger.info(f"Attempting to cancel order ID: {order_id} on account {self.account_id}")
            response_data = self.api_client.cancel_order(account_id=self.account_id, order_id=order_id)
            if response_data.get("success"):
                logger.info(f"[ORDER CANCELED] Order ID: {order_id} on account {self.account_id}.")
                return True
            else:
                err_msg = response_data.get('errorMessage', 'Order cancellation failed according to API.')
                logger.error(f"[CANCEL FAILED] Order ID: {order_id}. Reason: {err_msg}")
                return False
        except APIError as e: # Includes OrderNotFoundError if raised by APIClient
            logger.error(f"[CANCEL API ERROR] Failed to cancel order ID {order_id}: {e}")
            return False
        except Exception as e: 
            logger.error(f"[CANCEL UNEXPECTED ERROR] Failed for order ID {order_id}: {e}", exc_info=True)
            return False

    def modify_order(self, order_id: int,
                     new_size: Optional[int] = None,
                     new_limit_price: Optional[float] = None,
                     new_stop_price: Optional[float] = None,
                     new_trail_price: Optional[float] = None
                     ) -> bool:
        """Modifies parameters of an existing open order.

        Only non-None parameters will be included in the modification request.
        At least one modifiable parameter must be provided.

        Args:
            order_id (int): The ID of the order to modify.
            new_size (Optional[int]): The new quantity for the order. Must be positive if provided.
            new_limit_price (Optional[float]): The new limit price (for limit orders).
            new_stop_price (Optional[float]): The new stop price (for stop orders).
            new_trail_price (Optional[float]): The new trail price/offset (for trail stop orders, if supported).

        Returns:
            bool: True if the API response indicates successful modification, False otherwise.
        
        Raises:
            (Propagates APIClient exceptions)
        """
        if not isinstance(order_id, int) or order_id <= 0:
            logger.error(f"Invalid order_id for modification: {order_id}")
            return False

        modification_payload: Dict[str, Any] = {
            "accountId": self.account_id,
            "orderId": order_id # CHANGED: Use "orderId" instead of "orderld"
        }
        modified_fields_count = 0
        if new_size is not None:
            if not (isinstance(new_size, int) and new_size > 0):
                logger.error(f"Invalid new_size for modification: {new_size}. Must be positive integer.")
                return False 
            modification_payload["size"] = new_size
            modified_fields_count +=1
        if new_limit_price is not None:
            try: modification_payload["limitPrice"] = float(new_limit_price)
            except ValueError: logger.error(f"Invalid new_limit_price: {new_limit_price}"); return False
            modified_fields_count +=1
        if new_stop_price is not None:
            try: modification_payload["stopPrice"] = float(new_stop_price)
            except ValueError: logger.error(f"Invalid new_stop_price: {new_stop_price}"); return False
            modified_fields_count +=1
        if new_trail_price is not None:
            try: modification_payload["trailPrice"] = float(new_trail_price)
            except ValueError: logger.error(f"Invalid new_trail_price: {new_trail_price}"); return False
            modified_fields_count +=1

        if modified_fields_count == 0:
            logger.warning(f"No valid fields provided to modify for order ID: {order_id}. No action taken.")
            return False

        try:
            logger.info(f"Attempting to modify order ID: {order_id} on account {self.account_id} with payload: {modification_payload}")
            response_data = self.api_client.modify_order(modification_details=modification_payload)
            if response_data.get("success"):
                logger.info(f"[ORDER MODIFIED] Order ID: {order_id} on account {self.account_id}.")
                return True
            else:
                err_msg = response_data.get('errorMessage', 'Order modification failed according to API.')
                logger.error(f"[MODIFY FAILED] Order ID: {order_id}. Reason: {err_msg}")
                return False
        except APIError as e:
            logger.error(f"[MODIFY API ERROR] Failed to modify order ID {order_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"[MODIFY UNEXPECTED ERROR] Failed for order ID {order_id}: {e}", exc_info=True)
            return False

    def get_order_details(self, order_id_to_find: int, search_window_minutes: int = 60*24) -> Optional[Dict[str, Any]]:
        """Fetches details for a specific order by its ID by searching recent orders.

        Args:
            order_id_to_find (int): The ID of the order to find.
            search_window_minutes (int, optional): How far back (in minutes) from the
                current time to search for the order. Defaults to 1440 (24 hours).

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the order details if found,
            otherwise None.
        
        Raises:
            (Propagates APIClient exceptions)
        """
        if not isinstance(order_id_to_find, int) or order_id_to_find <= 0:
            logger.error(f"Invalid order_id_to_find: {order_id_to_find}")
            return None
            
        logger.debug(f"Attempting to fetch details for order ID: {order_id_to_find} for account {self.account_id}")
        
        end_time = datetime.now(UTC_TZ)
        start_time = end_time - timedelta(minutes=search_window_minutes)
        start_iso = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        try:
            orders_list = self.api_client.search_orders(
                account_id=self.account_id,
                start_timestamp_iso=start_iso,
                end_timestamp_iso=None # API default likely searches up to 'now'
            )
            for order in orders_list:
                if order.get("id") == order_id_to_find:
                    status_code = order.get("status")
                    status_str = ORDER_STATUS_TO_STRING_MAP.get(status_code, f"UNKNOWN({status_code})")
                    logger.info(f"Found details for order ID {order_id_to_find}: Status={status_str}")
                    return order
            logger.info(f"Order ID {order_id_to_find} not found in orders from the last {search_window_minutes} minutes for account {self.account_id}.")
            return None
        except APIError as e: 
            logger.error(f"API error while trying to get details for order ID {order_id_to_find}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching details for order ID {order_id_to_find}: {e}", exc_info=True)
            return None

def place_order_simulated(decision: str, contract_id: str, size: int = 1, account_id: Optional[int] = None):
    """Simulates placing an order and logs the action. Does not interact with an API.

    Args:
        decision (str): Typically "BUY" or "SELL".
        contract_id (str): The contract ID for the simulated order.
        size (int, optional): The size of the simulated order. Defaults to 1.
        account_id (Optional[int], optional): The account ID for simulation context. Defaults to None.
    """
    logger.info(f"[ORDER SIMULATED] Would place {decision} order for {size} of {contract_id} "
                f"{'on account ' + str(account_id) if account_id else ''}.")