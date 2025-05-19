# examples/test_error_codes/test_place_order_errors.py
from base_error_test_setup import (
    get_authenticated_client, log_api_call_attempt, log_client_exception,
    TEST_ACCOUNT_ID, TEST_CONTRACT_ID, logger
)
from tsxapipy.trading.order_handler import ORDER_TYPES_MAP, ORDER_SIDES_MAP
from tsxapipy.api.exceptions import (
    InvalidParameterError, ContractNotFoundError, MarketClosedError, APIHttpError, APIError
)

def run_place_order_error_tests():
    api_client = get_authenticated_client()
    if not api_client:
        logger.error("Failed to get authenticated API client. Aborting tests.")
        return

    logger.info("====== Testing Place Order Error Codes (via APIClient methods) ======")

    # Scenario 1: Payload missing accountId (should result in HTTP 400, parsed as InvalidParameterError by client)
    test_name_http400 = "Place Order (Client) - HTTP 400 for missing accountId"
    payload_missing_account = {
        "contractId": TEST_CONTRACT_ID if TEST_CONTRACT_ID else "DUMMY.CONTRACT.ID",
        "type": ORDER_TYPES_MAP["MARKET"],
        "side": ORDER_SIDES_MAP["BUY"],
        "size": 1
    }
    log_api_call_attempt(test_name_http400, "api_client.place_order (payload missing accountId)")
    try:
        api_client.place_order(payload_missing_account)
        logger.error(f"{test_name_http400}: FAILED - Expected InvalidParameterError (from HTTP 400), but no exception was raised.")
    except InvalidParameterError as e:
        logger.info(f"{test_name_http400}: SUCCESS - Caught InvalidParameterError: {e}")
        # The error_code for this parsed HTTP 400 will be 400 (the status code)
        if e.error_code == 400 and e.raw_response and "errors" in e.raw_response and "accountId" in str(e.raw_response.get("errors")):
             logger.info(f"  Error details correctly indicate missing accountId: {e.raw_response.get('errors')}")
        else:
             logger.warning(f"  Caught InvalidParameterError, but details differ from expected ASP.NET 400 for missing accountId: Code={e.error_code}, RawResp={e.raw_response}")
    except APIHttpError as e:
        if e.status_code == 400:
            logger.info(f"{test_name_http400}: CAUGHT APIHttpError with status 400 as expected: {e}")
        else:
            log_client_exception(test_name_http400, e)
    except Exception as e:
        log_client_exception(test_name_http400, e)


    # Scenario 2: Invalid contractId (API returns errorCode 8)
    if TEST_ACCOUNT_ID:
        test_name_invalid_contract = "Place Order (Client) - Invalid contractId (errorCode 8)"
        invalid_contract = "INVALID.CONTRACT.ID.XYZ"
        payload_invalid_contract = {
            "accountId": TEST_ACCOUNT_ID,
            "contractId": invalid_contract,
            "type": ORDER_TYPES_MAP["MARKET"],
            "side": ORDER_SIDES_MAP["BUY"],
            "size": 1
        }
        log_api_call_attempt(test_name_invalid_contract, "api_client.place_order (invalid contractId)")
        try:
            api_client.place_order(payload_invalid_contract)
            logger.error(f"{test_name_invalid_contract}: FAILED - Expected ContractNotFoundError, but no exception was raised.")
        except ContractNotFoundError as e:
            logger.info(f"{test_name_invalid_contract}: SUCCESS - Caught ContractNotFoundError: {e}")
            if e.error_code == 8:
                logger.info("  Correctly mapped from API errorCode 8.")
            else:
                logger.warning(f"  Caught ContractNotFoundError, but API errorCode was {e.error_code} (expected 8).")
        except Exception as e:
            log_client_exception(test_name_invalid_contract, e)
    else:
        logger.warning(f"Skipping '{test_name_invalid_contract}' due to missing TEST_ACCOUNT_ID.")


    # Scenario 3: Invalid order type (API returns errorCode 2, specific message)
    if TEST_ACCOUNT_ID and TEST_CONTRACT_ID:
        test_name_invalid_type = "Place Order (Client) - Invalid order type (errorCode 2)"
        payload_invalid_type = {
            "accountId": TEST_ACCOUNT_ID,
            "contractId": TEST_CONTRACT_ID,
            "type": 999,
            "side": ORDER_SIDES_MAP["BUY"],
            "size": 1
        }
        log_api_call_attempt(test_name_invalid_type, "api_client.place_order (invalid type)")
        try:
            api_client.place_order(payload_invalid_type)
            logger.error(f"{test_name_invalid_type}: FAILED - Expected InvalidParameterError, but no exception was raised.")
        except InvalidParameterError as e:
            logger.info(f"{test_name_invalid_type}: SUCCESS - Caught InvalidParameterError: {e}")
            # Use str(e) to access the exception's primary message
            if e.error_code == 2 and str(e) and "invalid order type specified" in str(e).lower():
                 logger.info("  Correctly mapped from API errorCode 2 and specific message.")
            else:
                logger.warning(f"  Caught InvalidParameterError, but API error code/message mismatch: Code={e.error_code}, Msg='{str(e)}'")
        except Exception as e:
            log_client_exception(test_name_invalid_type, e)
    else:
        logger.warning(f"Skipping '{test_name_invalid_type}' due to missing TEST_ACCOUNT_ID or TEST_CONTRACT_ID.")


    # Scenario 4: Zero size (API returns errorCode 2, specific message)
    if TEST_ACCOUNT_ID and TEST_CONTRACT_ID:
        test_name_zero_size = "Place Order (Client) - Zero size (errorCode 2)"
        payload_zero_size = {
            "accountId": TEST_ACCOUNT_ID,
            "contractId": TEST_CONTRACT_ID,
            "type": ORDER_TYPES_MAP["MARKET"],
            "side": ORDER_SIDES_MAP["BUY"],
            "size": 0
        }
        log_api_call_attempt(test_name_zero_size, "api_client.place_order (zero size)")
        try:
            api_client.place_order(payload_zero_size)
            logger.error(f"{test_name_zero_size}: FAILED - Expected InvalidParameterError, but no exception was raised.")
        except InvalidParameterError as e:
            logger.info(f"{test_name_zero_size}: SUCCESS - Caught InvalidParameterError: {e}")
            # Use str(e) to access the exception's primary message
            if e.error_code == 2 and str(e) and "invalid order size specified" in str(e).lower():
                 logger.info("  Correctly mapped from API errorCode 2 and specific message.")
            else:
                logger.warning(f"  Caught InvalidParameterError, but API error code/message mismatch: Code={e.error_code}, Msg='{str(e)}'")
        except Exception as e:
            log_client_exception(test_name_zero_size, e)
    else:
        logger.warning(f"Skipping '{test_name_zero_size}' due to missing TEST_ACCOUNT_ID or TEST_CONTRACT_ID.")


    # Scenario 5: Limit order missing limitPrice (API returns errorCode 2, "Outside of trading hours." if market closed)
    if TEST_ACCOUNT_ID and TEST_CONTRACT_ID:
        test_name_limit_no_price_market_closed = "Place Order (Client) - Limit no price / Market Closed (errorCode 2)"
        payload_limit_no_price = {
            "accountId": TEST_ACCOUNT_ID,
            "contractId": TEST_CONTRACT_ID,
            "type": ORDER_TYPES_MAP["LIMIT"],
            "side": ORDER_SIDES_MAP["BUY"],
            "size": 1
        }
        log_api_call_attempt(test_name_limit_no_price_market_closed, "api_client.place_order (limit no price / market closed)")
        try:
            api_client.place_order(payload_limit_no_price)
            logger.error(f"{test_name_limit_no_price_market_closed}: FAILED - Expected MarketClosedError (or InvalidParameterError if market open/different error), but no exception raised.")
        except MarketClosedError as e:
            logger.info(f"{test_name_limit_no_price_market_closed}: SUCCESS - Caught MarketClosedError: {e}")
            # Use str(e) to access the exception's primary message
            if e.error_code == 2 and str(e) and "outside of trading hours" in str(e).lower():
                 logger.info("  Correctly mapped from API errorCode 2 and market closed message.")
            else:
                logger.warning(f"  Caught MarketClosedError, but API error code/message mismatch: Code={e.error_code}, Msg='{str(e)}'")
        except InvalidParameterError as e:
            logger.info(f"{test_name_limit_no_price_market_closed}: CAUGHT InvalidParameterError (market might be open OR API treats missing limitPrice as this): {e}")
        except Exception as e:
            log_client_exception(test_name_limit_no_price_market_closed, e)
    else:
        logger.warning(f"Skipping '{test_name_limit_no_price_market_closed}' due to missing TEST_ACCOUNT_ID or TEST_CONTRACT_ID.")

    logger.info("====== Finished Testing Place Order Error Codes (via APIClient methods) ======")

if __name__ == "__main__":
    if not TEST_ACCOUNT_ID:
        print("ERROR: TEST_ACCOUNT_ID is not set. Check your .env and tsxapipy.config.py")
    if not TEST_CONTRACT_ID:
        print("ERROR: TEST_CONTRACT_ID is not set. Check your .env and tsxapipy.config.py")
    run_place_order_error_tests()