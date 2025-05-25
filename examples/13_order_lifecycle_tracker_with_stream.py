# examples/13_order_lifecycle_tracker_with_stream.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

import logging
import time
import pprint
import argparse
from typing import Any, Optional, Dict

from tsxapipy import (
    APIClient,
    UserHubStream,
    OrderPlacer,
    authenticate,
    AuthenticationError,
    ConfigurationError,
    APIError,
    LibraryError, # Added
    # StreamConnectionState, # Not directly used in this script's logic but good to be aware of
    DEFAULT_CONFIG_CONTRACT_ID,
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH,
    ORDER_STATUS_TO_STRING_MAP, 
    ORDER_STATUS_FILLED,
    ORDER_STATUS_CANCELLED,
    ORDER_STATUS_REJECTED,
    ORDER_STATUS_WORKING
)
from tsxapipy.api.exceptions import APIResponseParsingError # Added

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("OrderLifecycleTrackerExample")

# --- State for the tracked order ---
tracked_order_id: Optional[int] = None
tracked_order_status_from_stream: Optional[int] = None
tracked_order_details_from_stream: Optional[Dict[str, Any]] = None
is_order_terminal: bool = False 

def handle_tracked_order_update(order_data: Any): # order_data is Dict from stream
    global tracked_order_id, tracked_order_status_from_stream, tracked_order_details_from_stream, is_order_terminal
    
    order_id_from_event = order_data.get("id")
    
    if order_id_from_event == tracked_order_id:
        logger.info(f"--- TRACKED ORDER UPDATE (ID: {tracked_order_id}) ---")
        
        status_code = order_data.get("status")
        status_str = ORDER_STATUS_TO_STRING_MAP.get(status_code, f"UNKNOWN_STATUS({status_code})")
        
        logger.info(f"  New Status: {status_str} (Code: {status_code})")
        logger.info(f"  CumQty: {order_data.get('cumQuantity')}, LeavesQty: {order_data.get('leavesQuantity')}")
        logger.info(f"  AvgPx: {order_data.get('avgPx')}")
        logger.debug(f"  Full data: {pprint.pformat(order_data)}")

        tracked_order_status_from_stream = status_code
        tracked_order_details_from_stream = order_data

        if status_code in [ORDER_STATUS_FILLED, ORDER_STATUS_CANCELLED, ORDER_STATUS_REJECTED]:
            logger.info(f"Tracked order {tracked_order_id} reached terminal state: {status_str}. Loop will end if not already.")
            is_order_terminal = True
    else:
        logger.debug(f"Received update for non-tracked order ID: {order_id_from_event}")


def handle_user_stream_state(state_str: str): # state_str is the name of StreamConnectionState enum
    logger.info(f"UserHubStream state changed to: {state_str}")

def handle_user_stream_error(error: Any):
    logger.error(f"UserHubStream error: {error}")


def run_order_lifecycle_example(contract_id: str, account_id: int, limit_price_offset: float = -10.0):
    global tracked_order_id, is_order_terminal # Allow modification of globals
    
    # Reset global state for multiple runs in the same process (if any)
    tracked_order_id = None
    is_order_terminal = False
    # tracked_order_status_from_stream = None # Not strictly needed to reset if logic handles None
    # tracked_order_details_from_stream = None

    logger.info(f"--- Example: Order Lifecycle Tracker via UserHubStream ---")
    logger.info(f"Contract: {contract_id}, Account: {account_id}, Price Offset for Limit: {limit_price_offset}")

    api_client: Optional[APIClient] = None
    order_placer: Optional[OrderPlacer] = None
    user_stream: Optional[UserHubStream] = None

    base_price_for_limit = 18000.0 
    if "CL" in contract_id.upper(): base_price_for_limit = 70.0
    elif "GC" in contract_id.upper(): base_price_for_limit = 2000.0
    elif "ES" in contract_id.upper(): base_price_for_limit = 5000.0

    target_limit_price = base_price_for_limit + limit_price_offset
    logger.info(f"Calculated target limit price for BUY order: {target_limit_price}")

    try:
        logger.info("Authenticating...")
        initial_token, token_acquired_at = authenticate()
        
        api_client = APIClient(initial_token=initial_token, token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")

        order_placer = OrderPlacer(api_client, account_id, default_contract_id=contract_id)
        logger.info("OrderPlacer initialized.")

        logger.info(f"Initializing UserHubStream for account: {account_id}")
        user_stream = UserHubStream(
            api_client=api_client,
            account_id_to_watch=account_id,
            on_order_update=handle_tracked_order_update,
            subscribe_to_accounts_globally=False, 
            on_state_change_callback=handle_user_stream_state,
            on_error_callback=handle_user_stream_error
        )
        if not user_stream.start():
            logger.error(f"Failed to start UserHubStream (current status: {user_stream.connection_status.name if user_stream else 'N/A'}). Exiting.")
            return
        logger.info("UserHubStream start initiated.")
        time.sleep(2) 

        logger.info(f"Placing LIMIT BUY order: 1 lot of {contract_id} at {target_limit_price} on account {account_id}...")
        # OrderPlacer.place_limit_order signature is the same
        current_placed_order_id = order_placer.place_limit_order(
            side="BUY", 
            size=1, 
            limit_price=target_limit_price,
            contract_id=contract_id
        )

        if not current_placed_order_id:
            logger.error("Failed to place the initial test order. Exiting example.")
            if user_stream: user_stream.stop(reason_for_stop="Order placement failed")
            return
        
        tracked_order_id = current_placed_order_id # Set global for callback
        logger.info(f"Test order placed successfully! Order ID to track: {tracked_order_id}. Monitoring stream for updates...")

        monitoring_duration_before_cancel = 20 
        cancel_attempt_timeout = 15 
        
        start_monitor_time = time.monotonic()
        while time.monotonic() - start_monitor_time < monitoring_duration_before_cancel:
            if is_order_terminal:
                logger.info(f"Order {tracked_order_id} became terminal before cancel attempt. Loop ending.")
                break
            logger.debug(f"Monitoring order {tracked_order_id}... Current status from stream: "
                         f"{ORDER_STATUS_TO_STRING_MAP.get(tracked_order_status_from_stream, 'Not Yet Seen')}")
            time.sleep(1)
        
        if not is_order_terminal:
            logger.info(f"\nAttempting to cancel order {tracked_order_id}...")
            # OrderPlacer.cancel_order signature is the same
            cancel_success = order_placer.cancel_order(order_id=tracked_order_id)
            if cancel_success:
                logger.info(f"Cancel request for order {tracked_order_id} submitted. Waiting for stream confirmation...")
                
                cancel_confirm_start_time = time.monotonic()
                while time.monotonic() - cancel_confirm_start_time < cancel_attempt_timeout:
                    if is_order_terminal and tracked_order_status_from_stream == ORDER_STATUS_CANCELLED:
                        logger.info(f"Order {tracked_order_id} successfully confirmed as CANCELLED via stream.")
                        break
                    elif is_order_terminal: 
                        logger.warning(f"Order {tracked_order_id} became terminal with status "
                                       f"{ORDER_STATUS_TO_STRING_MAP.get(tracked_order_status_from_stream, 'N/A')} "
                                       f"during/after cancel attempt.")
                        break
                    time.sleep(0.5)
                else: 
                    logger.warning(f"Timed out waiting for cancellation confirmation for order {tracked_order_id} via stream. "
                                   f"Last known stream status: {ORDER_STATUS_TO_STRING_MAP.get(tracked_order_status_from_stream, 'N/A')}")
            else:
                logger.error(f"Failed to submit cancel request for order {tracked_order_id} via OrderPlacer.")
                if is_order_terminal:
                    logger.info(f"  Order was already terminal with status: {ORDER_STATUS_TO_STRING_MAP.get(tracked_order_status_from_stream, 'N/A')}")

        logger.info("\nFinal details of tracked order from stream events:")
        if tracked_order_details_from_stream:
            logger.info(pprint.pformat(tracked_order_details_from_stream))
        else:
            logger.info("No stream updates were captured for the tracked order ID.")

    except ConfigurationError as e_conf:
        logger.error(f"CONFIGURATION ERROR: {e_conf}")
    except AuthenticationError as e_auth:
        logger.error(f"AUTHENTICATION FAILED: {e_auth}")
    except APIResponseParsingError as e_parse: 
        logger.error(f"API RESPONSE PARSING ERROR (likely during auth or order placement): {e_parse}")
        if e_parse.raw_response_text:
            logger.error("Raw problematic response text (preview): %s", e_parse.raw_response_text[:500])
    except APIError as e_api:
        logger.error(f"API ERROR: {e_api}")
    except LibraryError as e_lib:
        logger.error(f"LIBRARY ERROR: {e_lib}")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down...")
        if tracked_order_id and not is_order_terminal and order_placer: # Check if order_placer was initialized
            logger.warning(f"Order {tracked_order_id} might still be active. Attempting to cancel due to interrupt...")
            if order_placer.cancel_order(tracked_order_id):
                logger.info("Cancel request for lingering order sent.")
            else:
                logger.error("Failed to send cancel for lingering order.")
    except Exception as e_generic:
        logger.error(f"AN UNEXPECTED ERROR OCCURRED: {e_generic}", exc_info=True)
    finally:
        if user_stream:
            logger.info(f"Stopping UserHubStream (current status: {user_stream.connection_status.name})...")
            user_stream.stop(reason_for_stop="Example script finishing")
        logger.info("Order lifecycle tracker example finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Track an order's lifecycle using UserHubStream.")
    parser.add_argument(
        "--contract_id", 
        type=str, 
        default=DEFAULT_CONFIG_CONTRACT_ID,
        help="Contract ID for the test order."
    )
    parser.add_argument(
        "--account_id", 
        type=int, 
        default=int(DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH) if DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH else 0,
        help="Account ID to place the order on."
    )
    parser.add_argument(
        "--price_offset",
        type=float,
        default=-20.0, 
        help="Price offset from a base price to set the limit order (e.g., -10 for 10 points below base)."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG level logging."
    )
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.DEBUG)
        logger.info("DEBUG logging enabled.")

    if not args.contract_id or not args.account_id or args.account_id <= 0:
        logger.error("A valid --contract_id and positive --account_id must be provided or set in .env.")
        sys.exit(1)

    run_order_lifecycle_example(
        contract_id=args.contract_id,
        account_id=args.account_id,
        limit_price_offset=args.price_offset
    )