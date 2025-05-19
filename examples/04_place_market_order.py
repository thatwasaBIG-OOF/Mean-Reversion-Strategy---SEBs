"""
Example 04: Place Market Order.

This script demonstrates:
1. Authenticating and initializing APIClient and OrderPlacer.
2. Placing a market BUY order for a default contract and account.
3. Optionally polling for the order status shortly after placement.

WARNING: This script will place a LIVE market order if configured for a live account.
Ensure you are using a DEMO account or understand the risks.
"""
# pylint: disable=invalid-name # Allow filename for example script
import sys
import os
import logging
import time
from typing import Optional # For APIClient type hint
# from datetime import datetime # Not directly used

# ---- sys.path modification ----
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ---- End sys.path modification ----

from tsxapipy.auth import authenticate
from tsxapipy.api import APIClient
from tsxapipy.trading import OrderPlacer
from tsxapipy import (
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH as ACCOUNT_ID,
    DEFAULT_CONFIG_CONTRACT_ID as CONTRACT_ID
)
from tsxapipy.api.exceptions import APIError, AuthenticationError

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

    if not ACCOUNT_ID or not CONTRACT_ID:
        logger.error("ACCOUNT_ID or CONTRACT_ID not set. Cannot place order.")
        return

    log_msg_start = ("--- Example 4: Place Market Order for Account %s on %s ---",
                     ACCOUNT_ID, CONTRACT_ID)
    logger.info(*log_msg_start)

    api_client: Optional[APIClient] = None
    try:
        initial_token, token_acquired_at = authenticate()
        api_client = APIClient(initial_token=initial_token,
                               token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")

        order_placer = OrderPlacer(
            api_client=api_client,
            account_id=int(ACCOUNT_ID) if ACCOUNT_ID else 0, # Ensure int or handle None
            default_contract_id=CONTRACT_ID
        )

        # --- Place a Market BUY order ---
        logger.info("Attempting to place MARKET BUY order, 1 lot of %s...",
                    CONTRACT_ID)
        buy_order_id = order_placer.place_market_order(side="BUY", size=1)

        if buy_order_id:
            logger.info("MARKET BUY order submitted successfully! Order ID: %s",
                        buy_order_id)
            logger.info("Monitor this order via your trading platform or User Hub stream.")

            # Example: Wait a bit and try to get its status (optional)
            time.sleep(5) # Allow time for processing
            logger.info("Polling for status of order %s...", buy_order_id)
            order_details = order_placer.get_order_details(buy_order_id,
                                                           search_window_minutes=5)
            if order_details:
                logger.info("Polled Order Details for %s: %s",
                            buy_order_id, order_details)
            else:
                log_msg_details_fail = ("Could not retrieve details for order %s "
                                        "via polling shortly after placement.",
                                        buy_order_id)
                logger.info(*log_msg_details_fail)
        else:
            logger.error("Failed to submit MARKET BUY order for %s.", CONTRACT_ID)

    except AuthenticationError as e:
        logger.error("Auth failed: %s", e)
    except APIError as e:
        logger.error("API Error: %s", e)
    except ValueError as e: # Could be from int(ACCOUNT_ID) or OrderPlacer init
        logger.error("Value/Config Error: %s", e)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("Unexpected error: %s", e_gen, exc_info=True)

if __name__ == "__main__":
    run_example()