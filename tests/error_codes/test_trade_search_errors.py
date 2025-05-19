# examples/test_error_codes/test_trade_search_errors.py
from datetime import datetime, timedelta
from base_error_test_setup import (
    get_authenticated_client, log_api_call_attempt, log_client_exception,
    TEST_ACCOUNT_ID, direct_post, logger
)
from tsxapipy.api.exceptions import InvalidParameterError, APIHttpError, APIError

def run_trade_search_error_tests():
    api_client = get_authenticated_client()
    if not api_client: return
    logger.info("====== Testing Trade Search Error Codes ======")

    valid_start_time = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    earlier_end_time = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Scenario 1: Invalid accountId
    if TEST_ACCOUNT_ID:
        test_name = "Trade Search (Direct) - Invalid accountId"
        payload = {"accountId": 0, "startTimestamp": valid_start_time}
        log_api_call_attempt(test_name, "/api/Trade/search", payload)
        direct_post(api_client, "/api/Trade/search", payload)
        # TODO: Observe, update client, test client mapping for api_client.search_trades

    # Scenario 2: Invalid startTimestamp format
    if TEST_ACCOUNT_ID:
        test_name = "Trade Search (Direct) - Invalid startTimestamp format"
        payload = {"accountId": TEST_ACCOUNT_ID, "startTimestamp": "BAD-DATE-FORMAT"}
        log_api_call_attempt(test_name, "/api/Trade/search", payload)
        direct_post(api_client, "/api/Trade/search", payload)
        # TODO: Observe, update client, test client mapping

    # Scenario 3: endTimestamp BEFORE startTimestamp
    if TEST_ACCOUNT_ID:
        test_name = "Trade Search (Direct) - endTimestamp before startTimestamp"
        payload = {"accountId": TEST_ACCOUNT_ID, "startTimestamp": valid_start_time, "endTimestamp": earlier_end_time}
        log_api_call_attempt(test_name, "/api/Trade/search", payload)
        direct_post(api_client, "/api/Trade/search", payload)
        # TODO: Observe, update client, test client mapping

    logger.info("====== Finished Testing Trade Search Error Codes ======")

if __name__ == "__main__":
    run_trade_search_error_tests()