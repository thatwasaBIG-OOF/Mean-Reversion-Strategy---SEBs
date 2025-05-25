"""
Example 04: Place Market Order.

This script demonstrates:
1. Authenticating and initializing APIClient and OrderPlacer.
2. Placing a market BUY order for a default contract and account.
3. Optionally polling for the order status shortly after placement,
   handling Pydantic models for order details.

WARNING: This script will place a LIVE market order if configured for a live account.
Ensure you are using a DEMO account or understand the risks.
"""
# pylint: disable=invalid-name # Allow filename for example script
import sys
import os
import logging
import time
from typing import Optional 

# ---- sys.path modification ----
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ---- End sys.path modification ----

from tsxapipy.auth import authenticate
from tsxapipy.api import APIClient
from tsxapipy.trading import OrderPlacer, ORDER_STATUS_TO_STRING_MAP # Import status map
from tsxapipy import (
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH as ACCOUNT_ID_STR, # Keep as STR for int conversion check
    DEFAULT_CONFIG_CONTRACT_ID as CONTRACT_ID,
    api_schemas # Import Pydantic schemas module
)
from tsxapipy.api.exceptions import (
    APIError, AuthenticationError, ConfigurationError, LibraryError,
    APIResponseParsingError # Added
)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s'
)
logger = logging.getLogger(__name__)

def run_example():
    """Runs the example for placing a market order."""
    logger.warning("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    logger.warning("!!! WARNING: THIS SCRIPT WILL PLACE A LIVE MARKET ORDER!     !!!")
    logger.warning("!!! ENSURE YOU ARE USING A DEMO ACCOUNT OR UNDERSTAND RISK!  !!!")
    logger.warning("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    confirm = input("Type 'YES_PLACE_ORDER' to continue: ")
    if confirm != "YES_PLACE_ORDER":
        logger.info("Order placement cancelled by user.")
        return

    if not ACCOUNT_ID_STR or not CONTRACT_ID:
        logger.error("ACCOUNT_ID_TO_WATCH or CONTRACT_ID not set in .env. Cannot place order.")
        return

    try:
        # Ensure ACCOUNT_ID can be converted to int
        account_id_int = int(ACCOUNT_ID_STR)
        if account_id_int <= 0:
            raise ValueError("ACCOUNT_ID_TO_WATCH must be a positive integer.")
    except ValueError:
        logger.error(f"Invalid ACCOUNT_ID_TO_WATCH in .env: '{ACCOUNT_ID_STR}'. Must be a positive integer.")
        return

    log_msg_start = ("--- Example 4: Place Market Order for Account %s on %s (Using Pydantic Models) ---",
                     account_id_int, CONTRACT_ID)
    logger.info(*log_msg_start)

    api_client: Optional[APIClient] = None
    try:
        initial_token, token_acquired_at = authenticate()
        api_client = APIClient(initial_token=initial_token,
                               token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")

        order_placer = OrderPlacer(
            api_client=api_client,
            account_id=account_id_int, 
            default_contract_id=CONTRACT_ID
        )

        # --- Place a Market BUY order ---
        logger.info("Attempting to place MARKET BUY order, 1 lot of %s...",
                    CONTRACT_ID)
        buy_order_id = order_placer.place_market_order(side="BUY", size=1, contract_id=CONTRACT_ID) # Explicitly pass contract_id

        if buy_order_id:
            logger.info("MARKET BUY order submitted successfully! Order ID: %s",
                        buy_order_id)
            logger.info("Monitor this order via your trading platform or User Hub stream.")

            # Example: Wait a bit and try to get its status (optional)
            time.sleep(5) # Allow time for processing
            logger.info("Polling for status of order %s...", buy_order_id)
            
            # OrderPlacer.get_order_details now returns Optional[api_schemas.OrderDetails]
            order_details_model: Optional[api_schemas.OrderDetails] = order_placer.get_order_details(
                order_id_to_find=buy_order_id,
                search_window_minutes=5
            )
            
            if order_details_model:
                status_str = ORDER_STATUS_TO_STRING_MAP.get(
                    order_details_model.status, 
                    f"UNKNOWN_STATUS({order_details_model.status})"
                ) if order_details_model.status is not None else "Status Not Available"
                
                logger.info("Polled Order Details for %s:", buy_order_id)
                logger.info("  ID: %s", order_details_model.id)
                logger.info("  Status: %s (Code: %s)", status_str, order_details_model.status)
                logger.info("  Contract ID: %s", order_details_model.contract_id or "N/A")
                logger.info("  Size: %s, CumQty: %s, LeavesQty: %s",
                            order_details_model.size if order_details_model.size is not None else "N/A",
                            order_details_model.cum_quantity if order_details_model.cum_quantity is not None else "N/A",
                            order_details_model.leaves_quantity if order_details_model.leaves_quantity is not None else "N/A")
                logger.info("  AvgPx: %s", f"{order_details_model.avg_px:.2f}" if order_details_model.avg_px is not None else "N/A")
                # For full details, you can dump the model:
                # logger.info("  Full Model JSON: %s", order_details_model.model_dump_json(indent=2, by_alias=True))
            else:
                log_msg_details_fail = ("Could not retrieve details for order %s "
                                        "via polling shortly after placement.",
                                        buy_order_id)
                logger.info(*log_msg_details_fail)
        else:
            logger.error("Failed to submit MARKET BUY order for %s.", CONTRACT_ID)

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
    except ValueError as e_val: # Could be from int(ACCOUNT_ID) or OrderPlacer init
        logger.error("Value/Config Error: %s", e_val)
    except LibraryError as e_lib:
        logger.error("LIBRARY ERROR: %s", e_lib)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("Unexpected error: %s", e_gen, exc_info=True)

if __name__ == "__main__":
    run_example()