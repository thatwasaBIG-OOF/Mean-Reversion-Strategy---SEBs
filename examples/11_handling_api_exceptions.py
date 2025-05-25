# examples/11_handling_api_exceptions.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

import logging
# from datetime import datetime, timedelta # Not used directly

from pydantic import ValidationError # For catching Pydantic errors

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
    MarketClosedError,
    LibraryError, # Added for completeness
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH as ACCOUNT_ID_STR, # Keep as STR
    DEFAULT_CONFIG_CONTRACT_ID as DEFAULT_CONTRACT_ID_STR,
    api_schemas # Import Pydantic schemas
)
from tsxapipy.api.exceptions import APIResponseParsingError # Added
from tsxapipy.trading.order_handler import ORDER_TYPES_MAP, ORDER_SIDES_MAP # For constructing payloads

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("APIExceptionHandlingExample")

def run_exception_handling_example():
    logger.info(f"--- Example: Handling APIClient Exceptions (with Pydantic) ---")

    test_account_id_int: Optional[int] = None
    if ACCOUNT_ID_STR:
        try:
            val = int(ACCOUNT_ID_STR)
            if val > 0:
                test_account_id_int = val
            else:
                logger.warning("DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH in .env is not positive. Using placeholder 0 for some tests.")
                test_account_id_int = 0 # Placeholder for tests designed to fail
        except ValueError:
            logger.warning(f"DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH ('{ACCOUNT_ID_STR}') in .env is not a valid integer. Using placeholder 0.")
            test_account_id_int = 0
    else:
        logger.warning("DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH not set. Using placeholder 0 for some tests.")
        test_account_id_int = 0

    test_contract_id_str = DEFAULT_CONTRACT_ID_STR
    if not test_contract_id_str:
        logger.warning("DEFAULT_CONFIG_CONTRACT_ID not set in .env. Some tests requiring a valid contract might behave unexpectedly or be skipped.")
        # test_contract_id_str will remain None

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
        # search_contract_by_id returns List[api_schemas.Contract]
        contracts: List[api_schemas.Contract] = api_client.search_contract_by_id(contract_id=non_existent_contract_id)
        if not contracts:
            logger.info(f"SUCCESS (as expected): No contract found for ID '{non_existent_contract_id}', API returned empty list.")
        else:
            logger.warning(f"UNEXPECTED: Contracts found for '{non_existent_contract_id}': {[c.id for c in contracts]}")
    except ContractNotFoundError as e: # This exception is mapped by error_mapper
        logger.info(f"SUCCESS (as expected): Caught ContractNotFoundError for '{non_existent_contract_id}'.")
        logger.info(f"  Error details: Code={e.error_code}, Message='{str(e)}'")
        logger.debug(f"  Raw response from exception: {e.raw_response}")
    except APIResponseParsingError as e_parse:
        logger.error(f"API RESPONSE PARSING ERROR during non-existent contract search: {e_parse}")
    except APIError as e:
        logger.error(f"APIError during non-existent contract search: {type(e).__name__} - {e}")
    except Exception as e_exc:
        logger.error(f"Unexpected error during non-existent contract search: {e_exc}", exc_info=True)

    # --- Test 2: Placing an order with invalid parameters (e.g., zero size) ---
    logger.info("\n--- Test 2: Place Order with Zero Size ---")
    if test_account_id_int is not None and test_account_id_int > 0 and test_contract_id_str:
        try:
            logger.info(f"Attempting to create Pydantic model for order with zero size for contract '{test_contract_id_str}' on account {test_account_id_int}.")
            # Pydantic model `schemas.PlaceMarketOrderRequest` has `size: int = Field(gt=0)`
            # This will raise ValidationError locally before hitting the API.
            order_request_model = api_schemas.PlaceMarketOrderRequest(
                accountId=test_account_id_int,
                contractId=test_contract_id_str,
                # type is const=2 in model
                side=ORDER_SIDES_MAP["BUY"],
                size=0 # Invalid size according to Pydantic model's gt=0
            )
            # The following line will not be reached if size=0 due to Pydantic validation
            # response = api_client.place_order(order_payload_model=order_request_model)
            logger.warning("UNEXPECTED: Pydantic model creation for zero-size order did not raise ValidationError.")
        except ValidationError as e_val: # Catch Pydantic's validation error
            logger.info(f"SUCCESS (as expected): Caught pydantic.ValidationError creating zero-size order model: {e_val}")
            # Example of how to inspect Pydantic errors:
            # for error in e_val.errors():
            #     logger.info(f"  Field: {error['loc']}, Message: {error['msg']}, Type: {error['type']}")
        # The API's InvalidParameterError for size=0 might not be hit due to client-side Pydantic validation.
        # If Pydantic's `size` constraint was looser (e.g., `ge=0`), then we'd test API's rejection.
        except APIError as e_api: # Catch if somehow it passed Pydantic and API errored
            logger.error(f"APIError during zero-size order attempt (should have been caught by Pydantic): {type(e_api).__name__} - {e_api}")
        except Exception as e_exc:
            logger.error(f"Unexpected error during zero-size order attempt: {e_exc}", exc_info=True)
    else:
        logger.warning("Skipping zero-size order test due to missing valid positive TEST_ACCOUNT_ID or TEST_CONTRACT_ID.")


    # --- Test 3: Placing an order for a non-existent contract ID ---
    logger.info("\n--- Test 3: Place Order for Non-Existent Contract ---")
    if test_account_id_int is not None and test_account_id_int > 0 :
        try:
            order_request_model = api_schemas.PlaceMarketOrderRequest(
                accountId=test_account_id_int,
                contractId=non_existent_contract_id, # Use the same non-existent ID
                # type is const=2
                side=ORDER_SIDES_MAP["BUY"],
                size=1
            )
            logger.info(f"Attempting to place order for non-existent contract '{non_existent_contract_id}'.")
            # APIClient.place_order now takes a Pydantic model
            response = api_client.place_order(order_payload_model=order_request_model)
            # If API returns success=true for a non-existent contract order (unlikely), this is an issue.
            if response.success:
                logger.warning(f"UNEXPECTED: Order placement for non-existent contract '{non_existent_contract_id}' "
                               f"reported success by API. Order ID: {response.order_id}")
            else: # API returns success=false
                 logger.info(f"API indicated failure for order on non-existent contract: {response.error_message} (Code: {response.error_code})")
                 # This path suggests ContractNotFoundError was not raised by error_mapper for this specific case
                 # or that the error code/message didn't match the mapping.
        except ContractNotFoundError as e_cnf: # Expected if error_mapper works for place_order
            logger.info(f"SUCCESS (as expected): Caught ContractNotFoundError for order on non-existent contract '{non_existent_contract_id}'.")
            logger.info(f"  Error details: Code={e_cnf.error_code}, Message='{str(e_cnf)}'")
            if e_cnf.error_code == 8: 
                 logger.info("  Error code matches expected for invalid contract on order placement.")
            logger.debug(f"  Raw response from exception: {e_cnf.raw_response}")
        except APIResponseParsingError as e_parse:
            logger.error(f"API RESPONSE PARSING ERROR for order on non-existent contract: {e_parse}")
        except APIError as e_api: # Other API errors
            logger.error(f"APIError during order on non-existent contract: {type(e_api).__name__} - {e_api}")
        except Exception as e_exc:
            logger.error(f"Unexpected error during order on non-existent contract: {e_exc}", exc_info=True)
    else:
        logger.warning("Skipping order placement for non-existent contract test due to missing valid positive TEST_ACCOUNT_ID.")

    # --- Test 4: Cancelling a non-existent order ---
    logger.info("\n--- Test 4: Cancel Non-Existent Order ---")
    if test_account_id_int is not None and test_account_id_int > 0:
        non_existent_order_id_to_cancel = 999999999 
        try:
            logger.info(f"Attempting to cancel non-existent order ID: {non_existent_order_id_to_cancel}")
            # APIClient.cancel_order returns schemas.CancelOrderResponse
            response_model = api_client.cancel_order(account_id=test_account_id_int, order_id=non_existent_order_id_to_cancel)
            if response_model.success: # API might return success even if order not found but request is valid
                logger.warning(f"UNEXPECTED: Cancellation of non-existent order {non_existent_order_id_to_cancel} reported success by API.")
            else: # Expected path if API returns success=false because order not found
                logger.info(f"API indicated failure for cancel of non-existent order {non_existent_order_id_to_cancel}: {response_model.error_message} (Code: {response_model.error_code})")
                # Check if it should have been an OrderNotFoundError
                if response_model.error_code == 2 and response_model.error_message is None: # Matching specific conditions
                     logger.info("  Error signature matches expected for non-existent order cancellation (potentially mapped to OrderNotFoundError by API client).")

        except OrderNotFoundError as e_onf: # Expected exception
            logger.info(f"SUCCESS (as expected): Caught OrderNotFoundError for cancelling non-existent order {non_existent_order_id_to_cancel}.")
            logger.info(f"  Error details: Code={e_onf.error_code}, Message='{str(e_onf)}'")
            if e_onf.error_code == 2 and e_onf.raw_response and e_onf.raw_response.get("errorMessage") is None:
                logger.info("  Error signature matches expected for non-existent order cancellation.")
            logger.debug(f"  Raw response from exception: {e_onf.raw_response}")
        except APIResponseParsingError as e_parse:
            logger.error(f"API RESPONSE PARSING ERROR during non-existent order cancellation: {e_parse}")
        except APIError as e_api:
            logger.error(f"APIError during non-existent order cancellation: {type(e_api).__name__} - {e_api}")
        except Exception as e_exc:
            logger.error(f"Unexpected error during non-existent order cancellation: {e_exc}", exc_info=True)
    else:
        logger.warning("Skipping cancellation of non-existent order test due to missing valid positive TEST_ACCOUNT_ID.")
        
    # --- Test 5: General APIError Catch-All ---
    logger.info("\n--- Test 5: Demonstrating General APIError Catch ---")
    if test_account_id_int is not None and test_account_id_int > 0:
        try:
            logger.info("Re-attempting cancel of another non-existent order to show generic APIError catch if not specifically mapped.")
            api_client.cancel_order(account_id=test_account_id_int, order_id=999999998) # Different ID
        except OrderNotFoundError: 
            pass # Already handled specifically, this is for testing the general catch
        except InvalidParameterError: 
            pass 
        except APIResponseParsingError:
            pass # If response parsing failed
        except APIError as e_api: 
            logger.info(f"CAUGHT GENERIC APIError (as expected if specific mapping didn't trigger): {type(e_api).__name__} - {str(e_api)}")
            if hasattr(e_api, 'error_code') and e_api.error_code is not None: logger.info(f"  Generic APIError has error_code: {e_api.error_code}")
            if hasattr(e_api, 'status_code') and e_api.status_code is not None: logger.info(f"  Generic APIError has status_code: {e_api.status_code}") # If it's an APIHttpError subclass
        except Exception as e_exc:
            logger.error(f"Unexpected non-APIError during generic error test: {e_exc}", exc_info=True)
    else:
        logger.warning("Skipping generic APIError catch test due to missing valid positive TEST_ACCOUNT_ID.")

    logger.info("\n--- API Exception Handling Example Finished ---")

if __name__ == "__main__":
    run_exception_handling_example()