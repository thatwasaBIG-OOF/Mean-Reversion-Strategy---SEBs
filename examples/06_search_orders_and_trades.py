"""
Example 06: Search Historical Orders and Trades.

This script demonstrates:
1. Authenticating and initializing APIClient.
2. Defining a time range (e.g., last 24 hours).
3. Searching for orders within that time range for a specified account, using Pydantic models.
4. Searching for trades within that time range for the same account, using Pydantic models.
"""
# pylint: disable=invalid-name # Allow filename for example script
import sys
import os
import logging
from datetime import datetime, timedelta
from typing import Optional, List # Added List for type hinting

# ---- sys.path modification ----
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ---- End sys.path modification ----

from tsxapipy.auth import authenticate
from tsxapipy.api import APIClient, APIError, AuthenticationError
from tsxapipy.common import UTC_TZ
from tsxapipy import (
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH as ACCOUNT_ID_STR, # Keep as STR
    api_schemas # Import Pydantic schemas
)
from tsxapipy.api.exceptions import ConfigurationError, LibraryError, APIResponseParsingError
from tsxapipy.trading import ORDER_STATUS_TO_STRING_MAP, ORDER_SIDES_MAP, ORDER_TYPES_MAP # For display

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s'
)
logger = logging.getLogger(__name__)

def get_order_type_str(type_code: Optional[int]) -> str:
    if type_code is None: return "N/A"
    for k, v in ORDER_TYPES_MAP.items():
        if v == type_code: return k
    return f"UNKNOWN_TYPE({type_code})"

def get_order_side_str(side_code: Optional[int]) -> str:
    if side_code is None: return "N/A"
    for k, v in ORDER_SIDES_MAP.items():
        if v == side_code: return k
    return f"UNKNOWN_SIDE({side_code})"

def run_example():
    """Runs the example for searching orders and trades."""
    logger.info("--- Example 6: Search Historical Orders and Trades (Using Pydantic Models) ---")

    if not ACCOUNT_ID_STR:
        logger.error("ACCOUNT_ID_TO_WATCH not set in .env. Cannot search.")
        return
        
    try:
        account_id_int = int(ACCOUNT_ID_STR)
        if account_id_int <= 0:
            raise ValueError("ACCOUNT_ID_TO_WATCH must be a positive integer.")
    except ValueError:
        logger.error(f"Invalid ACCOUNT_ID_TO_WATCH in .env: '{ACCOUNT_ID_STR}'. Must be a positive integer.")
        return

    api_client: Optional[APIClient] = None
    try:
        initial_token, token_acquired_at = authenticate()
        api_client = APIClient(initial_token=initial_token,
                               token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")

        # --- Define Time Range (e.g., last 24 hours) ---
        end_time_utc = datetime.now(UTC_TZ)
        start_time_utc = end_time_utc - timedelta(days=1) # Search last 1 day

        start_iso = start_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = end_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        log_search_orders = ("\nSearching orders for Account ID: %s from %s to %s",
                             account_id_int, start_iso, end_iso)
        logger.info(*log_search_orders)

        # api_client.search_orders now returns List[api_schemas.OrderDetails]
        orders_found: List[api_schemas.OrderDetails] = api_client.search_orders(
            account_id=account_id_int,
            start_timestamp_iso=start_iso,
            end_timestamp_iso=end_iso # Pass end_iso, though APIClient sends exclude_none
        )

        if orders_found:
            logger.info("Found %d order(s) in the specified period:",
                        len(orders_found))
            for order_model in orders_found[:5]: # Display first 5
                status_str = ORDER_STATUS_TO_STRING_MAP.get(order_model.status, f"UNKNOWN({order_model.status})") if order_model.status is not None else "N/A"
                type_str = get_order_type_str(order_model.type)
                side_str = get_order_side_str(order_model.side)
                created_ts_str = order_model.creation_timestamp.isoformat() if order_model.creation_timestamp else "N/A"

                log_order_details = (
                    "  Order ID: %s, Contract: %s, Status: %s, Type: %s, "
                    "Side: %s, Size: %s, Created: %s",
                    order_model.id,
                    order_model.contract_id or "N/A",
                    status_str,
                    type_str,
                    side_str,
                    order_model.size if order_model.size is not None else "N/A",
                    created_ts_str
                )
                logger.info(*log_order_details)
        else:
            logger.info("No orders found in the specified period for this account.")

        # --- Search for Trades ---
        log_search_trades = ("\nSearching trades for Account ID: %s from %s to %s",
                             account_id_int, start_iso, end_iso)
        logger.info(*log_search_trades)
        
        # api_client.search_trades now returns List[api_schemas.Trade]
        trades_found: List[api_schemas.Trade] = api_client.search_trades(
            account_id=account_id_int,
            start_timestamp_iso=start_iso,
            end_timestamp_iso=end_iso
        )

        if trades_found:
            logger.info("Found %d trade(s) in the specified period:",
                        len(trades_found))
            for trade_model in trades_found[:5]: # Display first 5
                side_str = get_order_side_str(trade_model.side)
                created_ts_str = trade_model.creation_timestamp.isoformat() if trade_model.creation_timestamp else "N/A"
                
                log_trade_details = (
                    "  Trade ID: %s, Order ID: %s, Contract: %s, Time: %s, "
                    "Price: %.2f, Size: %s, Side: %s, PnL: %.2f",
                    trade_model.id,
                    trade_model.order_id if trade_model.order_id is not None else "N/A",
                    trade_model.contract_id or "N/A",
                    created_ts_str,
                    trade_model.price if trade_model.price is not None else float('nan'),
                    trade_model.size if trade_model.size is not None else "N/A",
                    side_str,
                    trade_model.profit_and_loss if trade_model.profit_and_loss is not None else float('nan')
                )
                logger.info(*log_trade_details)
        else:
            logger.info("No trades found in the specified period for this account.")

    except ConfigurationError as e_conf:
        logger.error("CONFIGURATION ERROR: %s", e_conf)
    except AuthenticationError as e_auth:
        logger.error("Auth failed: %s", e_auth)
    except APIResponseParsingError as e_parse:
        logger.error("API RESPONSE PARSING ERROR: %s", e_parse)
        if e_parse.raw_response_text:
            logger.error("Raw problematic response text (preview): %s", e_parse.raw_response_text[:500])
    except APIError as e_api:
        logger.error("API Error: %s", e_api)
    except ValueError as e_val: 
        logger.error("Value/Config Error: %s", e_val)
    except LibraryError as e_lib:
        logger.error("LIBRARY ERROR: %s", e_lib)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("Unexpected error: %s", e_gen, exc_info=True)

if __name__ == "__main__":
    run_example()