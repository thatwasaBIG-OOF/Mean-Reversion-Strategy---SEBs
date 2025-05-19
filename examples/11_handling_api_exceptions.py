# examples/11_handling_api_exceptions.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

import logging
from datetime import datetime, timedelta

from tsxapipy import (
    APIClient,
    authenticate,
    AuthenticationError,
    ConfigurationError,
    APIError,
    ContractNotFoundError,
    InvalidParameterError,
    OrderNotFoundError,
    OrderRejectedError,
    MarketClosedError, # Assuming place_order can raise this
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH,
    DEFAULT_CONFIG_CONTRACT_ID
)
from tsxapipy.trading.order_handler import ORDER_TYPES_MAP, ORDER_SIDES_MAP # For constructing payloads

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("APIExceptionHandlingExample")

def run_exception_handling_example():
    logger.info(f"--- Example: Handling APIClient Exceptions ---")

    # Use configured account and contract if available, otherwise use placeholders for failure tests
    test_account_id = DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH
    test_contract_id = DEFAULT_CONFIG_CONTRACT_ID

    if not test_account_id:
        logger.warning("DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH not set. Using a placeholder (0) for some tests, which will likely fail.")
        test_account_id = 0 # Placeholder for tests designed to fail on account ID
    
    if not test_contract_id:
        logger.warning("DEFAULT_CONFIG_CONTRACT_ID not set. Using a placeholder for some tests.")
        # test_contract_id will remain None, and tests will use specific invalid IDs.


    api_client: Optional[APIClient] = None
    try:
        logger.info("Authenticating...")
        initial_token, token_acquired_at = authenticate()
        api_client = APIClient(initial_token=initial_token, token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")
    except (ConfigurationError, AuthenticationError, APIError) as e:
        logger.error(f"CRITICAL SETUP FAILURE: Could not authenticate or initialize APIClient: {e}")
        return
    except Exception as e_setup:
        logger.error(f"UNEXPECTED SETUP ERROR: {e_setup}", exc_info=True)
        return

    # --- Test 1: Searching for a non-existent contract ---
    logger.info("\n--- Test 1: Search for Non-Existent Contract ---")
    non_existent_contract_id = "CON.F.XX.DOESNOTEXIST.Z99"
    try:
        logger.info(f"Attempting to search for contract by ID: '{non_existent_contract_id}'")
        contracts = api_client.search_contract_by_id(contract_id=non_existent_contract_id)
        if not contracts:
            # This path is taken if the API returns success:true, contracts:[]
            logger.info(f"SUCCESS (as expected): No contract found for ID '{non_existent_contract_id}', API returned empty list.")
        else:
            # This path might be taken if the error mapping is not perfect and an error wasn't raised
            logger.warning(f"UNEXPECTED: Contracts found for '{non_existent_contract_id}': {contracts}")
    except ContractNotFoundError as e:
        logger.info(f"SUCCESS (as expected): Caught ContractNotFoundError for '{non_existent_contract_id}'.")
        logger.info(f"  Error details: Code={e.error_code}, Message='{str(e)}'")
        logger.debug(f"  Raw response from exception: {e.raw_response}")
    except APIError as e:
        logger.error(f"APIError during non-existent contract search: {type(e).__name__} - {e}")
    except Exception as e:
        logger.error(f"Unexpected error during non-existent contract search: {e}", exc_info=True)

    # --- Test 2: Placing an order with invalid parameters (e.g., zero size) ---
    logger.info("\n--- Test 2: Place Order with Zero Size ---")
    if test_account_id > 0 and test_contract_id: # Need valid account/contract for this to reach deeper API validation
        payload_zero_size = {
            "accountId": test_account_id,
            "contractId": test_contract_id,
            "type": ORDER_TYPES_MAP["MARKET"],
            "side": ORDER_SIDES_MAP["BUY"],
            "size": 0 # Invalid size
        }
        try:
            logger.info(f"Attempting to place order with zero size for contract '{test_contract_id}' on account {test_account_id}.")
            api_client.place_order(order_details=payload_zero_size)
            logger.warning("UNEXPECTED: Zero-size order placement did not raise an exception.")
        except InvalidParameterError as e:
            logger.info(f"SUCCESS (as expected): Caught InvalidParameterError for zero-size order.")
            logger.info(f"  Error details: Code={e.error_code}, Message='{str(e)}'")
            if e.error_code == 2 and "invalid order size" in str(e).lower(): # Check specific mapping
                logger.info("  Error message matches expected for invalid size.")
            logger.debug(f"  Raw response from exception: {e.raw_response}")
        except OrderRejectedError as e: # Could also be rejected for other reasons
            logger.info(f"OBSERVED: Caught OrderRejectedError for zero-size order (might also be valid response): {e}")
        except MarketClosedError as e: # If market is closed, this might be the primary error
            logger.warning(f"NOTE: MarketClosedError caught instead, market for {test_contract_id} might be closed: {e}")
        except APIError as e:
            logger.error(f"APIError during zero-size order placement: {type(e).__name__} - {e}")
        except Exception as e:
            logger.error(f"Unexpected error during zero-size order placement: {e}", exc_info=True)
    else:
        logger.warning("Skipping zero-size order test due to missing valid TEST_ACCOUNT_ID or TEST_CONTRACT_ID.")


    # --- Test 3: Placing an order for a non-existent contract ID ---
    logger.info("\n--- Test 3: Place Order for Non-Existent Contract ---")
    if test_account_id > 0 : # Need a valid account
        payload_non_existent_contract_order = {
            "accountId": test_account_id,
            "contractId": non_existent_contract_id, # Use the same non-existent ID
            "type": ORDER_TYPES_MAP["MARKET"],
            "side": ORDER_SIDES_MAP["BUY"],
            "size": 1
        }
        try:
            logger.info(f"Attempting to place order for non-existent contract '{non_existent_contract_id}'.")
            api_client.place_order(order_details=payload_non_existent_contract_order)
            logger.warning("UNEXPECTED: Order placement for non-existent contract did not raise an exception.")
        except ContractNotFoundError as e:
            logger.info(f"SUCCESS (as expected): Caught ContractNotFoundError for order on non-existent contract.")
            logger.info(f"  Error details: Code={e.error_code}, Message='{str(e)}'")
            if e.error_code == 8: # Check specific mapping for place_order with bad contract
                 logger.info("  Error code matches expected for invalid contract on order placement.")
            logger.debug(f"  Raw response from exception: {e.raw_response}")
        except APIError as e:
            logger.error(f"APIError during order on non-existent contract: {type(e).__name__} - {e}")
        except Exception as e:
            logger.error(f"Unexpected error during order on non-existent contract: {e}", exc_info=True)
    else:
        logger.warning("Skipping order placement for non-existent contract test due to missing valid TEST_ACCOUNT_ID.")

    # --- Test 4: Cancelling a non-existent order ---
    logger.info("\n--- Test 4: Cancel Non-Existent Order ---")
    if test_account_id > 0:
        non_existent_order_id_to_cancel = 999999999 # Unlikely to exist
        try:
            logger.info(f"Attempting to cancel non-existent order ID: {non_existent_order_id_to_cancel}")
            api_client.cancel_order(account_id=test_account_id, order_id=non_existent_order_id_to_cancel)
            logger.warning("UNEXPECTED: Cancellation of non-existent order did not raise an exception.")
        except OrderNotFoundError as e:
            logger.info(f"SUCCESS (as expected): Caught OrderNotFoundError for cancelling non-existent order.")
            logger.info(f"  Error details: Code={e.error_code}, Message='{str(e)}'")
            if e.error_code == 2 and e.raw_response and e.raw_response.get("errorMessage") is None: # Check specific mapping for cancel
                logger.info("  Error signature matches expected for non-existent order cancellation.")
            logger.debug(f"  Raw response from exception: {e.raw_response}")
        except APIError as e:
            logger.error(f"APIError during non-existent order cancellation: {type(e).__name__} - {e}")
        except Exception as e:
            logger.error(f"Unexpected error during non-existent order cancellation: {e}", exc_info=True)
    else:
        logger.warning("Skipping cancellation of non-existent order test due to missing valid TEST_ACCOUNT_ID.")
        
    # --- Test 5: General APIError Catch-All (e.g., if token becomes invalid mid-session) ---
    # This is harder to reliably trigger without manipulating the client's token directly
    # or having an endpoint known to cause an unmapped error.
    # For demonstration, we could simulate a bad token IF APIClient allowed easy token override for a single call
    # (it doesn't directly, it manages its own token).
    # Instead, we'll just show the structure.
    logger.info("\n--- Test 5: Demonstrating General APIError Catch ---")
    try:
        # Imagine an operation that might fail with an unmapped API error:
        # api_client._post_request("/api/Some/UnusualEndpoint", {"param": "value"})
        # For now, we'll re-attempt a known failing call that might not have a super-specific exception
        # if the error_mapper is not exhaustive.
        if test_account_id > 0:
            logger.info("Re-attempting cancel of non-existent order to show generic APIError catch.")
            api_client.cancel_order(account_id=test_account_id, order_id=999999998) # Different ID
    except OrderNotFoundError: # Already handled specifically
        pass
    except InvalidParameterError: # Already handled
        pass
    except APIError as e: # This would catch other API-originated errors
        logger.info(f"CAUGHT GENERIC APIError: {type(e).__name__} - {str(e)}")
        if hasattr(e, 'error_code'): logger.info(f"  Generic APIError has error_code: {e.error_code}")
        if hasattr(e, 'status_code'): logger.info(f"  Generic APIError has status_code: {e.status_code}")
    except Exception as e:
        logger.error(f"Unexpected non-APIError during generic error test: {e}", exc_info=True)


    logger.info("\n--- API Exception Handling Example Finished ---")

if __name__ == "__main__":
    run_exception_handling_example()