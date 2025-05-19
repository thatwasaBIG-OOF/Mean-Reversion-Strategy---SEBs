# examples/test_idempotency/test_custom_tag_idempotency.py
import sys
import os
import uuid
import time
import logging
from typing import Optional

# Adjust path to find the tsxapipy library
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from tsxapipy.auth import authenticate, AuthenticationError
from tsxapipy.api import APIClient, APIError
from tsxapipy.trading.order_handler import OrderPlacer, OrderPlacementResult # Assuming OrderPlacementResult is defined
from tsxapipy import DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH, DEFAULT_CONFIG_CONTRACT_ID, TRADING_ENV_SETTING
from tsxapipy.api.exceptions import ConfigurationError, LibraryError

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("IDEMPOTENCY_TESTER")

# --- Test Configuration ---
# Ensure these are valid for your DEMO account and an active market
# These will be pulled from your .env via tsxapipy.config
TEST_ACCOUNT_ID = DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH
TEST_CONTRACT_ID = DEFAULT_CONFIG_CONTRACT_ID # e.g., "CON.F.US.MNQ.M25" (Micro Nasdaq)
ORDER_SIZE = 1

def run_idempotency_test():
    logger.info(f"--- CustomTag Idempotency Test ---")
    logger.info(f"Target Environment: {TRADING_ENV_SETTING}")
    logger.info(f"Test Account ID: {TEST_ACCOUNT_ID}")
    logger.info(f"Test Contract ID: {TEST_CONTRACT_ID}")

    if TRADING_ENV_SETTING != "DEMO":
        logger.warning("WARNING: This test is designed for the DEMO environment. Running against LIVE is risky.")
        # confirmation = input("Are you sure you want to proceed against a non-DEMO environment? (yes/no): ")
        # if confirmation.lower() != 'yes':
        #     logger.info("Test aborted by user.")
        #     return
        # For safety, let's just abort if not DEMO for this automated script
        logger.error("Test will only run if TRADING_ENVIRONMENT is set to DEMO in .env. Aborting.")
        return

    if not TEST_ACCOUNT_ID or not TEST_CONTRACT_ID:
        logger.error("TEST_ACCOUNT_ID or TEST_CONTRACT_ID is not set in config. Aborting.")
        return

    api_client: Optional[APIClient] = None
    order_placer: Optional[OrderPlacer] = None

    try:
        logger.info("Authenticating...")
        initial_token, token_acquired_at = authenticate()
        api_client = APIClient(initial_token=initial_token, token_acquired_at=token_acquired_at)
        order_placer = OrderPlacer(api_client, TEST_ACCOUNT_ID, default_contract_id=TEST_CONTRACT_ID)
        logger.info("Authentication and OrderPlacer setup successful.")

    except (ConfigurationError, AuthenticationError, LibraryError) as e:
        logger.error(f"Setup failed: {e}")
        return
    except Exception as e:
        logger.error(f"Unexpected setup error: {e}", exc_info=True)
        return

    # Generate a unique client order ID (customTag) for this test sequence
    client_order_id_for_test = str(uuid.uuid4())
    logger.info(f"Generated Client Order ID (customTag) for test: {client_order_id_for_test}")

    # --- Attempt 1: Place the order ---
    logger.info("\n--- Attempt 1: Placing initial market order ---")
    # Using the OrderPlacer that generates customTag if not provided, or we can pass it
    # For this test, we explicitly pass our client_order_id_for_test as client_order_id_override
    # which should then be used as customTag by the modified OrderPlacer.
    
    # Assuming OrderPlacer.place_market_order is updated to use the new client_order_id_override
    # and returns OrderPlacementResult
    result1: OrderPlacementResult = order_placer.place_market_order(
        side="BUY",
        size=ORDER_SIZE,
        client_order_id_override=client_order_id_for_test # This becomes the customTag
    )

    logger.info(f"Attempt 1 - API Response: {result1.api_response}")
    logger.info(f"Attempt 1 - Client Tag Sent: {result1.client_order_id}")
    logger.info(f"Attempt 1 - Server Order ID: {result1.server_order_id}")
    logger.info(f"Attempt 1 - API Accepted: {result1.is_accepted_by_api}")

    original_server_order_id = result1.server_order_id

    if not result1.is_accepted_by_api or original_server_order_id is None:
        logger.error("Initial order placement (Attempt 1) was not accepted by the API or no server ID returned. Cannot proceed with idempotency check.")
        # Potentially log result1.api_response.get("errorMessage") or errorCode
        return

    logger.info(f"Initial order seems accepted with Server Order ID: {original_server_order_id}. Waiting a moment...")
    time.sleep(5) # Brief pause to ensure the first order is processed by the matching engine

    # --- Attempt 2: Place the EXACT SAME order with the SAME customTag ---
    logger.info("\n--- Attempt 2: Placing IDENTICAL market order with the SAME customTag ---")
    result2: OrderPlacementResult = order_placer.place_market_order(
        side="BUY", # Identical parameters
        size=ORDER_SIZE,
        client_order_id_override=client_order_id_for_test # Crucially, use the SAME client_order_id
    )

    logger.info(f"Attempt 2 - API Response: {result2.api_response}")
    logger.info(f"Attempt 2 - Client Tag Sent: {result2.client_order_id}") # Should be same as attempt 1
    logger.info(f"Attempt 2 - Server Order ID: {result2.server_order_id}")
    logger.info(f"Attempt 2 - API Accepted: {result2.is_accepted_by_api}")

    # --- Analysis of Results ---
    logger.info("\n--- Idempotency Test Analysis ---")
    if result2.is_accepted_by_api:
        if result2.server_order_id == original_server_order_id:
            logger.info("RESULT: POTENTIALLY IDEMPOTENT! Second request accepted and returned the SAME Server Order ID.")
            logger.info("This suggests the API might be de-duplicating based on customTag and returning the original order.")
        elif result2.server_order_id is not None and result2.server_order_id != original_server_order_id:
            logger.warning("RESULT: NOT IDEMPOTENT (Duplicate Order Created). Second request accepted and returned a NEW Server Order ID.")
            logger.warning(f"  Original Order ID: {original_server_order_id}, Duplicate Order ID: {result2.server_order_id}")
            logger.warning("  This means customTag is NOT acting as an idempotency key to prevent duplicates.")
            logger.warning("  ACTION: You will need to manually cancel one of these orders in your DEMO account.")
        else: # Accepted but no server order ID
            logger.warning(f"RESULT: AMBIGUOUS. Second request accepted (is_accepted_by_api=True) but Server Order ID is None. Investigate API response: {result2.api_response}")
    else: # Not result2.is_accepted_by_api
        api_err_code2 = result2.api_response.get("errorCode")
        api_err_msg2 = result2.api_response.get("errorMessage")
        logger.info(f"RESULT: Second order attempt was REJECTED by API.")
        logger.info(f"  API ErrorCode: {api_err_code2}, ErrorMessage: '{api_err_msg2}'")
        if api_err_code2 == YOUR_HYPOTHETICAL_IDEMPOTENCY_ERROR_CODE: # Replace with actual code if discovered
            logger.info("  This rejection with a specific error code might indicate IDEMPOTENT behavior (duplicate detected and rejected).")
            logger.info("  Check if the response includes the original order ID.")
        else:
            logger.info("  This rejection might be due to other reasons, or it's how the API handles duplicate customTags (by rejecting the new one).")

    # --- Cleanup (Optional but Recommended for Demo) ---
    # If a second order was created, you'd want to cancel it.
    # This part requires knowing the server IDs.
    if original_server_order_id:
        logger.info(f"\nAttempting to cancel original test order: {original_server_order_id} (if still open)")
        try:
            cancel_res1 = order_placer.cancel_order(original_server_order_id)
            logger.info(f"Cancel response for order {original_server_order_id}: {cancel_res1}")
        except APIError as e:
            logger.error(f"Error cancelling order {original_server_order_id}: {e}")

    if result2.server_order_id and result2.server_order_id != original_server_order_id:
        logger.info(f"Attempting to cancel duplicate test order: {result2.server_order_id} (if still open)")
        try:
            cancel_res2 = order_placer.cancel_order(result2.server_order_id)
            logger.info(f"Cancel response for order {result2.server_order_id}: {cancel_res2}")
        except APIError as e:
            logger.error(f"Error cancelling order {result2.server_order_id}: {e}")

    logger.info("--- Idempotency Test Finished ---")


if __name__ == "__main__":
    # Ensure your .env has TRADING_ENVIRONMENT="DEMO"
    # Ensure ACCOUNT_ID_TO_WATCH and CONTRACT_ID are set for a DEMO account and a liquid contract
    run_idempotency_test()