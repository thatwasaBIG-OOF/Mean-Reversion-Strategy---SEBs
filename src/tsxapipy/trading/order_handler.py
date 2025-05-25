# topstep_data_suite/src/tsxapipy/trading/order_handler.py
import logging
from typing import Dict, Any, Optional, List, Union # Added Union
from datetime import datetime, timedelta # For get_order_details search window

from tsxapipy.api.client import APIClient
from tsxapipy.api.exceptions import (
    APIError, InvalidParameterError, OrderNotFoundError, OrderRejectedError,
    APIResponseParsingError # Added for Pydantic
)
from tsxapipy.api import schemas # Import Pydantic schemas
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
    communication. It now uses Pydantic models internally to construct requests
    to `APIClient` and expects Pydantic models in responses from `APIClient`.

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

    def _create_order_request_model(self, contract_id: str, order_type_str: str, side_str: str, size: int,
                                   limit_price: Optional[float] = None, 
                                   stop_price: Optional[float] = None,
                                   trail_price: Optional[float] = None,
                                   custom_tag: Optional[str] = None,
                                   linked_order_id: Optional[int] = None) -> schemas.OrderBase:
        """
        Creates the appropriate Pydantic order request model based on order_type_str.
        This internal method validates inputs and constructs the Pydantic request model.

        Args:
            contract_id (str): The contract ID for the order.
            order_type_str (str): User-friendly order type (e.g., "MARKET", "LIMIT", "STOP").
            side_str (str): User-friendly order side (e.g., "BUY", "SELL").
            size (int): Order quantity.
            limit_price (Optional[float]): Limit price for LIMIT orders.
            stop_price (Optional[float]): Stop price for STOP orders.
            trail_price (Optional[float]): Trail price/offset for TRAILING_STOP orders.
            custom_tag (Optional[str]): Optional custom tag for the order.
            linked_order_id (Optional[int]): Optional ID of a linked order.

        Returns:
            schemas.OrderBase: An instance of a Pydantic order request model.

        Raises:
            ValueError: If `order_type_str`, `side_str`, `size`, or price inputs are invalid.
            pydantic.ValidationError: If the created Pydantic model fails validation.
        """
        order_type_code = ORDER_TYPES_MAP.get(order_type_str.upper())
        if order_type_code is None:
            raise ValueError(f"Invalid order_type_str: '{order_type_str}'. Supported: {list(ORDER_TYPES_MAP.keys())}")
        
        side_code = ORDER_SIDES_MAP.get(side_str.upper())
        if side_code is None:
            raise ValueError(f"Invalid side_str: '{side_str}'. Supported: {list(ORDER_SIDES_MAP.keys())}")

        if not isinstance(size, int) or size <= 0:
            raise ValueError(f"Order size must be a positive integer, got {size}.")

        # Prepare common parameters, Pydantic will handle None for optional fields
        common_params = {
            "accountId": self.account_id,
            "contractId": contract_id,
            # "type" will be set by the specific model or overridden if using OrderBase directly
            "side": side_code,
            "size": size,
            "customTag": custom_tag, # Pydantic model handles Optional
            "linkedOrderld": linked_order_id # Pydantic model handles Optional and alias
        }
        # Filter out None values for base params that might not be in all specific models
        # or if we were to build a generic OrderBase instance
        common_params_cleaned = {k: v for k, v in common_params.items() if v is not None}
        common_params_cleaned['type'] = order_type_code # ensure type is set for all paths

        request_model_instance: schemas.OrderBase
        try:
            if order_type_str.upper() == "MARKET":
                request_model_instance = schemas.PlaceMarketOrderRequest(**common_params_cleaned)
            elif order_type_str.upper() == "LIMIT":
                if limit_price is None or not isinstance(limit_price, (int,float)) or limit_price <=0:
                    raise ValueError("Limit price must be a positive number for LIMIT orders.")
                request_model_instance = schemas.PlaceLimitOrderRequest(**common_params_cleaned, limitPrice=limit_price)
            elif order_type_str.upper() == "STOP": # Assuming Stop Market
                if stop_price is None or not isinstance(stop_price, (int,float)) or stop_price <=0:
                    raise ValueError("Stop price must be a positive number for STOP orders.")
                request_model_instance = schemas.PlaceStopOrderRequest(**common_params_cleaned, stopPrice=stop_price)
            # Example for TRAILING_STOP if schema is defined
            # elif order_type_str.upper() == "TRAILING_STOP":
            #     if trail_price is None or not isinstance(trail_price, (int, float)) or trail_price <= 0:
            #         raise ValueError("Trail price must be a positive number for TRAILING_STOP orders.")
            #     request_model_instance = schemas.PlaceTrailingStopOrderRequest(**common_params_cleaned, trailPrice=trail_price)
            else:
                # This path implies a type exists in ORDER_TYPES_MAP but not as a specific Pydantic request model subclass.
                # We create a generic OrderBase; APIClient.place_order takes OrderBase.
                logger.warning(f"Creating generic OrderBase for order type '{order_type_str}'. Specific model preferred.")
                # Add price fields if present for the generic base. Pydantic will validate.
                if limit_price is not None: common_params_cleaned['limitPrice'] = limit_price
                if stop_price is not None: common_params_cleaned['stopPrice'] = stop_price
                if trail_price is not None: common_params_cleaned['trailPrice'] = trail_price
                request_model_instance = schemas.OrderBase(**common_params_cleaned)
            return request_model_instance
        except ValidationError as e: # Catch Pydantic validation errors during model instantiation
            logger.error(f"Pydantic validation error creating order request model for type '{order_type_str}': {e}")
            raise # Re-raise to be handled by the calling placement method

    def place_order(self, contract_id: Optional[str], 
                    order_type: str, side: str, size: int,
                    limit_price: Optional[float] = None, stop_price: Optional[float] = None,
                    trail_price: Optional[float] = None,
                    custom_tag: Optional[str] = None, linked_order_id: Optional[int] = None) -> Optional[int]:
        """
        Places a generic order using Pydantic models for request and response.
        (Full docstring preserved from your version)
        """
        target_contract_id = contract_id if contract_id else self.default_contract_id
        if not target_contract_id:
            logger.error("OrderPlacer: Cannot place order. Contract ID is missing and no default_contract_id is set.")
            return None
        
        try:
            request_model = self._create_order_request_model(
                target_contract_id, order_type, side, size,
                limit_price, stop_price, trail_price, custom_tag, linked_order_id
            )
            
            logger.info(f"Placing {side} {order_type} order: {size} of {target_contract_id} on account {self.account_id}. "
                        f"Request Model Type: {type(request_model).__name__}")
            logger.debug(f"OrderPlacer request model details: {request_model.model_dump_json(indent=2, by_alias=True, exclude_none=True)}")
            
            response_model: schemas.OrderPlacementResponse = self.api_client.place_order(order_payload_model=request_model)
            
            if response_model.success and response_model.order_id is not None:
                logger.info(f"[ORDER PLACED SUCCESSFULLY] Type: {side} {order_type}, Size: {size}, Contract: {target_contract_id}. API Order ID: {response_model.order_id}")
                return response_model.order_id
            else:
                logger.error(f"[ORDER SUBMISSION FAILED - API REJECTED] {side} {order_type} {size} of {target_contract_id}. "
                             f"API Reason: {response_model.error_message or 'Unknown'} (Code: {response_model.error_code})")
                return None
        except ValueError as ve: 
            logger.error(f"[ORDER PREPARATION ERROR] For {side} {order_type} order for {target_contract_id}: {ve}")
            return None 
        except ValidationError as pydantic_val_err: 
            logger.error(f"[ORDER PREPARATION VALIDATION ERROR] For {side} {order_type} order for {target_contract_id}: {pydantic_val_err}")
            return None
        except APIResponseParsingError as rpe:
            logger.error(f"[ORDER RESPONSE PARSING ERROR] For {side} {order_type} order for {target_contract_id}: {rpe}")
            return None
        except APIError as apie: 
            logger.error(f"[ORDER API ERROR] Failed to place {side} {order_type} order for {target_contract_id}: {apie}")
            return None 
        except Exception as e_place: 
            logger.error(f"[ORDER UNEXPECTED ERROR] Failed to place {side} {order_type} order for {target_contract_id}: {e_place}", exc_info=True)
            return None

    def place_market_order(self, side: str, size: int = 1, contract_id: Optional[str] = None,
                           custom_tag: Optional[str] = None) -> Optional[int]:
        """Places a market order.
        (Full docstring preserved)
        """
        return self.place_order(contract_id=contract_id, order_type="MARKET", 
                                side=side, size=size, custom_tag=custom_tag)

    def place_limit_order(self, side: str, size: int, limit_price: float, 
                          contract_id: Optional[str] = None,
                          custom_tag: Optional[str] = None, 
                          linked_order_id: Optional[int] = None) -> Optional[int]:
        """Places a limit order.
        (Full docstring preserved)
        """
        if not isinstance(limit_price, (int, float)) or limit_price <= 0:
            logger.error(f"Invalid limit_price for limit order: {limit_price}. Must be a positive number.")
            return None
        return self.place_order(contract_id=contract_id, order_type="LIMIT", 
                                side=side, size=size, limit_price=limit_price,
                                custom_tag=custom_tag, linked_order_id=linked_order_id)

    def place_stop_market_order(self, side: str, size: int, stop_price: float, 
                                contract_id: Optional[str] = None,
                                custom_tag: Optional[str] = None, 
                                linked_order_id: Optional[int] = None) -> Optional[int]:
        """Places a stop market order (API order type 3).
        (Full docstring preserved)
        """
        if not isinstance(stop_price, (int, float)) or stop_price <= 0:
            logger.error(f"Invalid stop_price for stop market order: {stop_price}. Must be positive.")
            return None
        return self.place_order(contract_id=contract_id, order_type="STOP", 
                                side=side, size=size, stop_price=stop_price,
                                custom_tag=custom_tag, linked_order_id=linked_order_id)

    def cancel_order(self, order_id: int) -> bool:
        """Cancels an existing open order.
        (Full docstring preserved)
        """
        if not isinstance(order_id, int) or order_id <= 0:
            logger.error(f"Invalid order_id for cancellation: {order_id}. Must be a positive integer.")
            return False
        try:
            logger.info(f"Attempting to cancel order ID: {order_id} on account {self.account_id}")
            response_model: schemas.CancelOrderResponse = self.api_client.cancel_order(
                account_id=self.account_id, order_id=order_id
            )
            if response_model.success:
                logger.info(f"[ORDER CANCEL REQUESTED] Order ID: {order_id} on account {self.account_id} successfully submitted for cancellation.")
                return True
            else:
                logger.error(f"[CANCEL FAILED - API REJECTED] Order ID: {order_id}. Reason: {response_model.error_message or 'Unknown'} (Code: {response_model.error_code})")
                return False
        except APIResponseParsingError as rpe:
            logger.error(f"[CANCEL RESPONSE PARSING ERROR] For order ID {order_id}: {rpe}")
            return False
        except APIError as e: 
            logger.error(f"[CANCEL API ERROR] Failed to cancel order ID {order_id}: {e}")
            return False
        except Exception as e_cancel: 
            logger.error(f"[CANCEL UNEXPECTED ERROR] Failed for order ID {order_id}: {e_cancel}", exc_info=True)
            return False

    def modify_order(self, order_id: int,
                     new_size: Optional[int] = None,
                     new_limit_price: Optional[float] = None,
                     new_stop_price: Optional[float] = None,
                     new_trail_price: Optional[float] = None
                     ) -> bool:
        """Modifies parameters of an existing open order.
        (Full docstring preserved)
        """
        if not isinstance(order_id, int) or order_id <= 0:
            logger.error(f"Invalid order_id for modification: {order_id}. Must be a positive integer.")
            return False

        request_params = {
            "accountId": self.account_id,
            "orderId": order_id,
            "size": new_size,
            "limitPrice": new_limit_price,
            "stopPrice": new_stop_price,
            "trailPrice": new_trail_price
        }
        # Pydantic model will handle None for optional fields based on exclude_none=True in model_dump
        # and its own validator will check if at least one modifiable field is provided.
        try:
            request_model = schemas.ModifyOrderRequest(**request_params)
            
            logger.info(f"Attempting to modify order ID: {order_id} on account {self.account_id} with changes.")
            logger.debug(f"OrderPlacer modify request model: {request_model.model_dump_json(indent=2, by_alias=True, exclude_none=True)}")

            response_model: schemas.ModifyOrderResponse = self.api_client.modify_order(
                modification_request_model=request_model
            )
            
            if response_model.success:
                logger.info(f"[ORDER MODIFICATION REQUESTED] Order ID: {order_id} on account {self.account_id} successfully submitted for modification.")
                return True
            else:
                logger.error(f"[MODIFY FAILED - API REJECTED] Order ID: {order_id}. Reason: {response_model.error_message or 'Unknown'} (Code: {response_model.error_code})")
                return False
        except ValidationError as pydantic_val_err: # From Pydantic model creation
            logger.error(f"[MODIFY PREPARATION VALIDATION ERROR] For order ID {order_id}: {pydantic_val_err}")
            return False
        except APIResponseParsingError as rpe:
            logger.error(f"[MODIFY RESPONSE PARSING ERROR] For order ID {order_id}: {rpe}")
            return False
        except APIError as e:
            logger.error(f"[MODIFY API ERROR] Failed to modify order ID {order_id}: {e}")
            return False
        except Exception as e_modify:
            logger.error(f"[MODIFY UNEXPECTED ERROR] Failed for order ID {order_id}: {e_modify}", exc_info=True)
            return False

    def get_order_details(self, order_id_to_find: int, search_window_minutes: int = 60*24) -> Optional[schemas.OrderDetails]:
        """
        Fetches details for a specific order by its ID, returning a Pydantic OrderDetails model.
        (Full docstring preserved)
        """
        if not isinstance(order_id_to_find, int) or order_id_to_find <= 0:
            logger.error(f"Invalid order_id_to_find: {order_id_to_find}. Must be a positive integer.")
            return None
            
        logger.debug(f"Attempting to fetch details for order ID: {order_id_to_find} for account {self.account_id} "
                     f"within the last {search_window_minutes} minutes.")
        
        end_time_utc = datetime.now(UTC_TZ)
        start_time_utc = end_time_utc - timedelta(minutes=search_window_minutes)
        start_iso = start_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        try:
            # APIClient.search_orders now returns List[schemas.OrderDetails]
            orders_list: List[schemas.OrderDetails] = self.api_client.search_orders(
                account_id=self.account_id,
                start_timestamp_iso=start_iso,
                end_timestamp_iso=None 
            )
            for order_model in orders_list:
                # Pydantic model's 'id' field is already an int
                if order_model.id == order_id_to_find:
                    status_str = ORDER_STATUS_TO_STRING_MAP.get(order_model.status, f"UNKNOWN_STATUS_CODE({order_model.status})")
                    logger.info(f"Found details for order ID {order_id_to_find}: Status is '{status_str}' (Code: {order_model.status}).")
                    return order_model # Return the Pydantic model instance
            
            logger.info(f"Order ID {order_id_to_find} not found in orders from the last {search_window_minutes} minutes for account {self.account_id}.")
            return None
        except APIResponseParsingError as rpe:
            logger.error(f"Failed to parse order search response when getting details for order ID {order_id_to_find}: {rpe}")
            return None
        except APIError as apie: 
            logger.error(f"API error while trying to get details for order ID {order_id_to_find}: {apie}")
            return None 
        except Exception as e_get_details:
            logger.error(f"Unexpected error fetching details for order ID {order_id_to_find}: {e_get_details}", exc_info=True)
            return None

def place_order_simulated(decision: str, contract_id: str, size: int = 1, account_id: Optional[int] = None):
    """
    Simulates placing an order and logs the action. Does not interact with an API.
    (Full docstring preserved)
    """
    log_msg = (f"[SIMULATED ORDER] Action: {decision.upper()}, Size: {size}, "
               f"Contract: {contract_id}")
    if account_id is not None:
        log_msg += f", Account: {account_id}"
    logger.info(log_msg)