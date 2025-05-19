# examples/test_error_codes/test_order_search_errors.py
from datetime import datetime, timedelta
from base_error_test_setup import (
    get_authenticated_client, log_api_call_attempt, log_client_exception,
    TEST_ACCOUNT_ID, direct_post, logger # Using direct_post for initial discovery
)
from tsxapipy.api.exceptions import InvalidParameterError, APIHttpError, APIError

def run_order_search_error_tests():
    api_client = get_authenticated_client()
    if not api_client: return
    logger.info("====== Testing Order Search Error Codes ======")

    valid_start_time = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    earlier_end_time = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Scenario 1: Invalid accountId
    if TEST_ACCOUNT_ID: # Ensure we have a valid one for other tests
        test_name = "Order Search (Direct) - Invalid accountId"
        payload = {"accountId": 0, "startTimestamp": valid_start_time}
        log_api_call_attempt(test_name, "/api/Order/search", payload)
        direct_post(api_client, "/api/Order/search", payload)
        # TODO: Observe API response, update APIClient, then test client mapping:
        # try:
        #     api_client.search_orders(account_id=0, start_timestamp_iso=valid_start_time)
        # except InvalidParameterError as e: logger.info(f"{test_name}: SUCCESS - Caught {type(e).__name__}: {e}")
        # except Exception as e: log_client_exception(test_name, e)

    # Scenario 2: Invalid startTimestamp format
    if TEST_ACCOUNT_ID:
        test_name = "Order Search (Direct) - Invalid startTimestamp format"
        payload = {"accountId": TEST_ACCOUNT_ID, "startTimestamp": "BAD-DATE-FORMAT"}
        log_api_call_attempt(test_name, "/api/Order/search", payload)
        direct_post(api_client, "/api/Order/search", payload)
        # TODO: Observe, update client, test client mapping

    # Scenario 3: endTimestamp BEFORE startTimestamp
    if TEST_ACCOUNT_ID:
        test_name = "Order Search (Direct) - endTimestamp before startTimestamp"
        payload = {"accountId": TEST_ACCOUNT_ID, "startTimestamp": valid_start_time, "endTimestamp": earlier_end_time}
        log_api_call_attempt(test_name, "/api/Order/search", payload)
        direct_post(api_client, "/api/Order/search", payload)
        # TODO: Observe, update client, test client mapping

    logger.info("====== Finished Testing Order Search Error Codes ======")

if __name__ == "__main__":
    run_order_search_error_tests()