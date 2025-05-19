# examples/test_error_codes/test_cancel_order_errors.py
from base_error_test_setup import (
    get_authenticated_client, log_api_call_attempt, log_client_exception,
    TEST_ACCOUNT_ID, logger # Removed direct_post as we test client
)
from tsxapipy.api.exceptions import ( # Import exceptions you expect to catch
    OrderNotFoundError, InvalidParameterError, APIError
)

def run_cancel_order_error_tests():
    api_client = get_authenticated_client()
    if not api_client:
        logger.error("Failed to get authenticated API client. Aborting cancel_order tests.")
        return

    logger.info("====== Testing Cancel Order Error Codes (via APIClient methods) ======")

    # Scenario 1: Non-existent order ID (API returns errorCode 2, errorMessage null for cancel)
    if TEST_ACCOUNT_ID:
        test_name_non_existent_client = "Cancel Order (Client) - Non-existent orderId"
        non_existent_order_id = 999999999 # A very unlikely real order ID
        log_api_call_attempt(test_name_non_existent_client, f"api_client.cancel_order(accountId={TEST_ACCOUNT_ID}, orderId={non_existent_order_id})")
        try:
            # APIClient.cancel_order now uses the corrected "orderId" key internally
            api_client.cancel_order(account_id=TEST_ACCOUNT_ID, order_id=non_existent_order_id)
            logger.error(f"{test_name_non_existent_client}: FAILED - Expected OrderNotFoundError, but no exception was raised.")
        except OrderNotFoundError as e:
            logger.info(f"{test_name_non_existent_client}: SUCCESS - Caught OrderNotFoundError: {e}")
            if e.error_code == 2 and e.raw_response and e.raw_response.get("errorMessage") is None:
                logger.info("  Correctly mapped from API errorCode 2 and null errorMessage.")
            else:
                logger.warning(f"  Caught OrderNotFoundError, but API error code/message mismatch: Code={e.error_code}, Msg='{str(e)}', Raw={e.raw_response}")
        except Exception as e:
            log_client_exception(test_name_non_existent_client, e)
    else:
        logger.warning("Skipping 'Cancel Order (Client) - Non-existent orderId' due to missing TEST_ACCOUNT_ID.")


    # Scenario 2: Invalid accountId (API returns errorCode 1, errorMessage null for cancel)
    test_name_invalid_account_client = "Cancel Order (Client) - Invalid accountId"
    invalid_account_id = 0 # Or another demonstrably invalid account ID
    some_order_id_for_test = 888888888 # Order ID doesn't matter as much if account is invalid first
    log_api_call_attempt(test_name_invalid_account_client, f"api_client.cancel_order(accountId={invalid_account_id}, orderId={some_order_id_for_test})")
    try:
        api_client.cancel_order(account_id=invalid_account_id, order_id=some_order_id_for_test)
        logger.error(f"{test_name_invalid_account_client}: FAILED - Expected InvalidParameterError, but no exception was raised.")
    except InvalidParameterError as e:
        logger.info(f"{test_name_invalid_account_client}: SUCCESS - Caught InvalidParameterError: {e}")
        if e.error_code == 1 and e.raw_response and e.raw_response.get("errorMessage") is None:
            logger.info("  Correctly mapped from API errorCode 1 and null errorMessage.")
        else:
            logger.warning(f"  Caught InvalidParameterError, but API error code/message mismatch: Code={e.error_code}, Msg='{str(e)}', Raw={e.raw_response}")
    except Exception as e:
        log_client_exception(test_name_invalid_account_client, e)

    # TODO: Add more scenarios for cancel_order:
    # - Attempt to cancel an already filled order (requires placing one first).
    #   - What errorCode/errorMessage does the API return?
    #   - Map this to OrderRejectedError or a similar specific exception in APIClient.
    # - Attempt to cancel an already cancelled order.
    #   - What errorCode/errorMessage does the API return?
    #   - Map this appropriately.

    logger.info("====== Finished Testing Cancel Order Error Codes (via APIClient methods) ======")


if __name__ == "__main__":
    if not TEST_ACCOUNT_ID:
        print("ERROR: TEST_ACCOUNT_ID is not set. Check your .env and tsxapipy.config.py (via base_error_test_setup.py)")
        print("Aborting tests due to missing configuration.")
    else:
        run_cancel_order_error_tests()