"""
Example 05: Place, Modify, and Cancel Limit Order.

This script demonstrates:
1. Placing a limit BUY order.
2. Polling for its status.
3. Modifying the order's limit price.
4. Polling again.
5. Cancelling the order.
6. Polling for final status.

WARNING: This script will place, modify, and cancel LIVE orders.
Ensure you are using a DEMO account or understand the risks.
"""
# pylint: disable=invalid-name # Allow filename for example script
# pylint: disable=too-many-locals, too-many-branches, too-many-statements
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
from tsxapipy.trading import OrderPlacer, ORDER_STATUS_TO_STRING_MAP
from tsxapipy.trading.order_handler import ( # Import constants directly
    ORDER_STATUS_WORKING, ORDER_STATUS_PENDING_NEW, ORDER_STATUS_NEW
)
from tsxapipy import (
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH as ACCOUNT_ID,
    DEFAULT_CONFIG_CONTRACT_ID as CONTRACT_ID
)
from tsxapipy.api.exceptions import APIError, AuthenticationError # OrderNotFoundError removed (not directly used)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s'
)
logger = logging.getLogger(__name__)

# !!! ADJUST THIS PRICE TO BE REALISTIC FOR YOUR CONTRACT !!!
# For NQ futures, this would be a high number like 18000.00.
# Using a placeholder that is likely too low for NQ.
EXAMPLE_LIMIT_PRICE = 100.00
EXAMPLE_MODIFIED_PRICE = EXAMPLE_LIMIT_PRICE - 1.00

def run_example():
    """Runs the example for placing, modifying, and cancelling a limit order."""
    logger.warning("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    logger.warning("!!! WARNING: THIS SCRIPT WILL PLACE, MODIFY, AND CANCEL LIVE ORDERS!  !!!")
    logger.warning("!!! ENSURE YOU ARE USING A DEMO ACCOUNT OR UNDERSTAND THE RISK!       !!!")
    logger.warning("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    confirm = input("Type 'YES_MANAGE_ORDERS' to continue: ")
    if confirm != "YES_MANAGE_ORDERS":
        logger.info("Order management example cancelled by user.")
        return

    if not ACCOUNT_ID or not CONTRACT_ID:
        logger.error("ACCOUNT_ID or CONTRACT_ID not set. Cannot manage orders.")
        return

    log_msg_start = ("--- Example 5: Place, Modify, and Cancel Limit Order for "
                     "Acct %s on %s ---", ACCOUNT_ID, CONTRACT_ID)
    logger.info(*log_msg_start)

    api_client: Optional[APIClient] = None
    placed_order_id: Optional[int] = None

    try:
        initial_token, token_acquired_at = authenticate()
        api_client = APIClient(initial_token=initial_token,
                               token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")

        order_placer = OrderPlacer(
            api_client=api_client,
            account_id=int(ACCOUNT_ID) if ACCOUNT_ID else 0,
            default_contract_id=CONTRACT_ID
        )

        # --- 1. Place a Limit BUY Order ---
        log_place_attempt = ("Attempting to place LIMIT BUY order, 1 lot of %s "
                             "at price %.2f...", CONTRACT_ID, EXAMPLE_LIMIT_PRICE)
        logger.info(*log_place_attempt)
        placed_order_id = order_placer.place_limit_order(
            side="BUY",
            size=1,
            limit_price=EXAMPLE_LIMIT_PRICE,
            contract_id=CONTRACT_ID
        )

        if not placed_order_id:
            logger.error("Failed to submit LIMIT BUY order for %s. Exiting.",
                         CONTRACT_ID)
            return

        logger.info("LIMIT BUY order submitted successfully! Order ID: %s",
                    placed_order_id)
        logger.info("Waiting a few seconds for order to appear on book...")
        time.sleep(5)

        # --- 2. Check Order Status (Polling) ---
        logger.info("\nPolling for status of order %s...", placed_order_id)
        order_details = order_placer.get_order_details(placed_order_id,
                                                       search_window_minutes=5)
        if order_details:
            status_code = order_details.get('status')
            status_str = ORDER_STATUS_TO_STRING_MAP.get(status_code,
                                                        f"UNKNOWN({status_code})")
            logger.info("Polled Order Details for %s: Status is %s",
                        placed_order_id, status_str)

            if status_code not in [ORDER_STATUS_WORKING, ORDER_STATUS_PENDING_NEW,
                                   ORDER_STATUS_NEW]:
                logger.warning("Order %s is not in a modifiable state (%s). "
                               "Skipping modify/cancel.", placed_order_id, status_str)
                return
        else:
            logger.warning("Could not retrieve details for order %s via polling. "
                           "Skipping modify/cancel.", placed_order_id)
            return

        # --- 3. Modify the Limit Order ---
        log_modify_attempt = ("\nAttempting to modify order %s to new limit price %.2f...",
                              placed_order_id, EXAMPLE_MODIFIED_PRICE)
        logger.info(*log_modify_attempt)
        modified_success = order_placer.modify_order(
            order_id=placed_order_id,
            new_limit_price=EXAMPLE_MODIFIED_PRICE
        )

        if modified_success:
            logger.info("Order %s modify request submitted successfully.",
                        placed_order_id)
            logger.info("Waiting a few seconds for modification to process...")
            time.sleep(3)

            logger.info("\nPolling for status of modified order %s...",
                        placed_order_id)
            modified_details = order_placer.get_order_details(placed_order_id,
                                                              search_window_minutes=5)
            if modified_details:
                logger.info("Polled Modified Order Details: %s", modified_details)
            else:
                logger.info("Could not retrieve details for modified order %s.",
                            placed_order_id)
        else:
            logger.error("Failed to submit modification request for order %s.",
                         placed_order_id)

        # --- 4. Cancel the Order ---
        logger.info("\nAttempting to cancel order %s...", placed_order_id)
        cancelled_success = order_placer.cancel_order(order_id=placed_order_id)

        if cancelled_success:
            logger.info("Order %s cancel request submitted successfully.",
                        placed_order_id)
            time.sleep(3)
            logger.info("\nPolling for final status of order %s "
                        "after cancel request...", placed_order_id)
            final_details = order_placer.get_order_details(placed_order_id,
                                                           search_window_minutes=5)
            if final_details:
                status_code = final_details.get('status')
                status_str = ORDER_STATUS_TO_STRING_MAP.get(status_code,
                                                           f"UNKNOWN({status_code})")
                logger.info("Final Polled Order Details for %s: Status is %s",
                            placed_order_id, status_str)
            else:
                logger.info("Could not retrieve final details for order %s "
                            "(might be fully processed and old).", placed_order_id)
        else:
            logger.error("Failed to submit cancel request for order %s. "
                         "It might have already been filled/rejected/cancelled.",
                         placed_order_id)

    except AuthenticationError as e:
        logger.error("Auth failed: %s", e)
    except APIError as e:
        logger.error("API Error: %s", e)
    except ValueError as e:
        logger.error("Value/Config Error: %s", e)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("Unexpected error: %s", e_gen, exc_info=True)

if __name__ == "__main__":
    run_example()