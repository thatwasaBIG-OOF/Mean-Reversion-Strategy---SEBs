# examples/test_error_codes/test_modify_order_errors.py
import time
import json
from typing import Optional

from base_error_test_setup import (
    get_authenticated_client, log_api_call_attempt, log_client_exception,
    TEST_ACCOUNT_ID, TEST_CONTRACT_ID, direct_post, logger # direct_post might still be useful for ad-hoc discovery
)
from tsxapipy.api.exceptions import (
    InvalidParameterError, OrderNotFoundError, OrderRejectedError, APIError, MarketClosedError
)
from tsxapipy.trading.order_handler import ORDER_TYPES_MAP, ORDER_SIDES_MAP

# Helper to place a test order and return its ID
def place_test_limit_order(api_client, account_id, contract_id, side="BUY", price=1.0, size=1) -> Optional[int]:
    if not api_client or not account_id or not contract_id:
        logger.error("Cannot place test order: missing api_client, account_id, or contract_id.")
        return None

    order_side_code = ORDER_SIDES_MAP.get(side.upper())
    if order_side_code is None:
        logger.error(f"Invalid side '{side}' for test order.")
        return None

    payload = {
        "accountId": account_id,
        "contractId": contract_id,
        "type": ORDER_TYPES_MAP["LIMIT"],
        "side": order_side_code,
        "size": size,
        "limitPrice": price
    }
    log_api_call_attempt("Placing Test Limit Order", "/api/Order/place", payload)
    try:
        response = api_client.place_order(payload) # Calls APIClient.place_order
        if response.get("success") and response.get("orderId"):
            order_id = int(response["orderId"])
            logger.info(f"Test order placed successfully. Order ID: {order_id}")
            return order_id
        else:
            api_err_code = response.get("errorCode")
            api_err_msg = response.get("errorMessage")
            logger.error(f"Failed to place test order. API Response: success={response.get('success')}, errorCode={api_err_code}, errorMessage='{api_err_msg}'")
            # This part relies on APIClient.place_order raising the correct exception for market closed
            # If it doesn't, the helper won't know. This is handled by the except APIError below.
            return None
    except MarketClosedError as mce: # Specifically catch if place_order says market is closed
        logger.warning(f"Test order placement failed because market is closed: {mce}")
        return None
    except APIError as e:
        logger.error(f"APIError placing test order: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error placing test order: {e}", exc_info=True)
        return None

def run_modify_order_error_tests():
    api_client = get_authenticated_client()
    if not api_client:
        logger.error("Failed to get authenticated API client. Aborting modify_order tests.")
        return

    logger.info("====== Testing Modify Order Error Codes ======")

    non_existent_order_id_for_modify = 777777777 # An ID unlikely to exist
    valid_test_account_id = TEST_ACCOUNT_ID

    if not valid_test_account_id:
        logger.error("TEST_ACCOUNT_ID is not set. Cannot proceed with modify_order tests that require it.")
        return

    # --- Step 1: Confirm 'orderId' is the correct key (based on previous output from this script) ---
    # Your previous run confirmed 'orderId' did not cause an HTTP 400 and resulted in
    # errorCode: 2, errorMessage: null for a non-existent order, which is expected behavior
    # if 'orderId' is the correct key.
    determined_order_key = "orderId"
    logger.info(f"Using determined order identifier key for modify payloads: '{determined_order_key}'")
    # Ensure APIClient.modify_order now uses "orderId" consistently.

    # Scenario 1: Modify non-existent order ID
    test_name_non_existent_client = f"Modify Order (Client) - Non-existent order ({determined_order_key})"
    payload_non_existent_client = {
        "accountId": valid_test_account_id,
        determined_order_key: non_existent_order_id_for_modify,
        "size": 3
    }
    log_api_call_attempt(test_name_non_existent_client, "api_client.modify_order (non-existent)")
    try:
        api_client.modify_order(payload_non_existent_client)
        logger.error(f"{test_name_non_existent_client}: FAILED - Expected OrderNotFoundError, but no exception was raised.")
    except OrderNotFoundError as e:
        logger.info(f"{test_name_non_existent_client}: SUCCESS - Caught OrderNotFoundError: {e}")
        if e.error_code == 2 and e.raw_response and e.raw_response.get("errorMessage") is None:
            logger.info(f"  Correctly mapped from API errorCode 2 and null errorMessage for modify (non-existent order).")
        else:
            logger.warning(f"  Caught OrderNotFoundError, but details mismatch: Code={e.error_code}, Msg='{str(e)}', RawResp={e.raw_response}")
    except Exception as e:
        log_client_exception(test_name_non_existent_client, e)


    # --- Scenarios for modifying an EXISTING OPEN order ---
    live_test_order_id: Optional[int] = None
    if valid_test_account_id and TEST_CONTRACT_ID:
        logger.info("\n--- Placing a live test order to attempt modifications ---")
        # Adjust price based on typical range of TEST_CONTRACT_ID to ensure it doesn't fill
        # For ES/NQ futures, a very low buy limit or very high sell limit.
        market_price_assumption = 5000 # Example, adjust if you know a typical price for TEST_CONTRACT_ID
        test_limit_price = market_price_assumption / 10 if TEST_CONTRACT_ID and (TEST_CONTRACT_ID.count(".MES.") > 0 or TEST_CONTRACT_ID.count(".MNQ.") > 0 or TEST_CONTRACT_ID.count(".EP.") > 0 or TEST_CONTRACT_ID.count(".ENQ.") > 0) else 1.00
        if TEST_CONTRACT_ID.startswith("CON.F.US.CL"): # Crude oil
            test_limit_price = 10.0

        live_test_order_id = place_test_limit_order(
            api_client,
            valid_test_account_id,
            TEST_CONTRACT_ID,
            side="BUY", # Or "SELL" with a very high price
            price=test_limit_price,
            size=1
        )
        if live_test_order_id:
            logger.info(f"Test order {live_test_order_id} placed. Waiting a few seconds for it to be active...")
            time.sleep(5) # Give a moment for the order to hit the books / be acknowledged
        else:
            logger.error("Failed to place live test order. Subsequent modification tests on live order will be skipped.")
            logger.info("This often happens if the market for TEST_CONTRACT_ID is closed when the test is run.")
    else:
        logger.warning("Skipping live order modification tests due to missing TEST_ACCOUNT_ID or TEST_CONTRACT_ID from config.")

    if live_test_order_id:
        # Scenario 2a: Modify an EXISTING OPEN order with an invalid size (e.g., 0)
        test_name_modify_existing_invalid_size = f"Modify Order (Client) - Existing order {live_test_order_id} with invalid size (0)"
        modify_payload_invalid_size = {
            "accountId": valid_test_account_id,
            determined_order_key: live_test_order_id,
            "size": 0 # Invalid size
        }
        log_api_call_attempt(test_name_modify_existing_invalid_size, "api_client.modify_order (existing order, invalid size)")
        try:
            api_client.modify_order(modify_payload_invalid_size)
            logger.error(f"{test_name_modify_existing_invalid_size}: FAILED - Expected InvalidParameterError or OrderRejectedError, but no exception raised.")
        except InvalidParameterError as e:
            logger.info(f"{test_name_modify_existing_invalid_size}: SUCCESS/OBSERVED - Caught InvalidParameterError: {e}")
            logger.info(f"  >> OBSERVE: For invalid size on existing order, API returned: errorCode={e.error_code}, message='{str(e)}', raw={e.raw_response}")
            # TODO: Based on observation, update APIClient._post_request for this specific errorCode/message
            #       Then, refine this assertion to be more specific.
        except OrderRejectedError as e:
            logger.info(f"{test_name_modify_existing_invalid_size}: SUCCESS/OBSERVED - Caught OrderRejectedError: {e}")
            logger.info(f"  >> OBSERVE: For invalid size on existing order, API returned: errorCode={e.error_code}, message='{str(e)}', raw={e.raw_response}")
            # TODO: Based on observation, update APIClient._post_request and refine this assertion.
        except Exception as e:
            log_client_exception(test_name_modify_existing_invalid_size, e)

        # Scenario 2b: Modify an EXISTING OPEN order with an invalid price (e.g., negative)
        test_name_modify_existing_invalid_price = f"Modify Order (Client) - Existing order {live_test_order_id} with invalid price (-10)"
        modify_payload_invalid_price = {
            "accountId": valid_test_account_id,
            determined_order_key: live_test_order_id,
            "limitPrice": -10.0 # Invalid price
            # "size": 1 # Keep original size if only changing price
        }
        log_api_call_attempt(test_name_modify_existing_invalid_price, "api_client.modify_order (existing order, invalid price)")
        try:
            api_client.modify_order(modify_payload_invalid_price)
            logger.error(f"{test_name_modify_existing_invalid_price}: FAILED - Expected InvalidParameterError or OrderRejectedError, but no exception raised.")
        except InvalidParameterError as e:
            logger.info(f"{test_name_modify_existing_invalid_price}: SUCCESS/OBSERVED - Caught InvalidParameterError: {e}")
            logger.info(f"  >> OBSERVE: For invalid price on existing order, API returned: errorCode={e.error_code}, message='{str(e)}', raw={e.raw_response}")
            # TODO: Update APIClient and refine assertion.
        except OrderRejectedError as e:
            logger.info(f"{test_name_modify_existing_invalid_price}: SUCCESS/OBSERVED - Caught OrderRejectedError: {e}")
            logger.info(f"  >> OBSERVE: For invalid price on existing order, API returned: errorCode={e.error_code}, message='{str(e)}', raw={e.raw_response}")
            # TODO: Update APIClient and refine assertion.
        except Exception as e:
            log_client_exception(test_name_modify_existing_invalid_price, e)

        # Cleanup: Cancel the test order
        logger.info(f"\n--- Attempting to cancel test order {live_test_order_id} after modification attempts ---")
        try:
            # Ensure cancel_order uses the correct key if modify and cancel differ, though unlikely.
            # APIClient.cancel_order should use "orderId" based on previous tests.
            cancel_response = api_client.cancel_order(valid_test_account_id, live_test_order_id)
            if cancel_response.get("success"):
                logger.info(f"Test order {live_test_order_id} cancelled successfully.")
            else:
                logger.error(f"Failed to cancel test order {live_test_order_id}. Response: {cancel_response}")
        except APIError as e: # Catch APIError from client.cancel_order
            logger.error(f"APIError cancelling test order {live_test_order_id}: {e}")
    
    logger.info("\nTODO: Add tests for modifying an already filled order.")
    logger.info("TODO: Add tests for modifying an already cancelled order.")
    logger.info("====== Finished Testing Modify Order Error Codes ======")

if __name__ == "__main__":
    if not TEST_ACCOUNT_ID:
        print("ERROR: TEST_ACCOUNT_ID is not set. Check your .env and tsxapipy.config.py")
    if not TEST_CONTRACT_ID:
        print("ERROR: TEST_CONTRACT_ID is not set for placing test orders. Some modify tests will be limited or fail.")
    run_modify_order_error_tests()