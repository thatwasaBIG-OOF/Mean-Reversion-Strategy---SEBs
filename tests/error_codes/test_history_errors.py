# examples/test_error_codes/test_history_errors.py
from datetime import datetime, timedelta
from base_error_test_setup import (
    get_authenticated_client, log_api_call_attempt, log_client_exception,
    TEST_CONTRACT_ID, direct_post, logger # TEST_CONTRACT_ID useful if it's an INT
)
from tsxapipy.api.exceptions import InvalidParameterError, ContractNotFoundError, APIHttpError

def run_history_error_tests():
    api_client = get_authenticated_client()
    if not api_client: return
    logger.info("====== Testing History Retrieval Error Codes ======")

    # IMPORTANT: Determine if TEST_CONTRACT_ID for history should be an INT or STRING
    # The OCR docs suggested INT for this endpoint. For now, assuming it might be string too.
    # If it's int, replace TEST_CONTRACT_ID with a known valid int ID or discover one.
    valid_contract_for_history = TEST_CONTRACT_ID or "CON.F.US.EP.M25" # Fallback
    # If API expects int, use something like:
    # valid_contract_for_history_int = 12345 # Replace with a real int ID if needed

    now = datetime.utcnow()
    start_time_iso = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    earlier_end_time_iso = (now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Scenario 1: Invalid contractId type (if it expects int, send string)
    test_name = "History (Direct) - Invalid contractId type"
    payload_invalid_type = {"contractId": "THIS_IS_A_STRING_IF_INT_EXPECTED", "startTime": start_time_iso, "endTime": end_time_iso, "unit": 2, "unitNumber": 1, "limit": 10}
    log_api_call_attempt(test_name, "/api/History/retrieveBars", payload_invalid_type)
    direct_post(api_client, "/api/History/retrieveBars", payload_invalid_type)
    # TODO: Observe, update client, test client mapping.

    # Scenario 2: Non-existent (but valid type) contractId
    test_name = "History (Direct) - Non-existent contractId"
    payload_non_existent = {"contractId": "CON.F.XX.NONEXISTENT.Z99", "startTime": start_time_iso, "endTime": end_time_iso, "unit": 2, "unitNumber": 1, "limit": 10}
    # If int: payload_non_existent = {"contractId": 99999999, ...}
    log_api_call_attempt(test_name, "/api/History/retrieveBars", payload_non_existent)
    direct_post(api_client, "/api/History/retrieveBars", payload_non_existent)
    # TODO: Observe (empty list or error code?), update client, test client mapping.

    # Scenario 3: Invalid startTime format
    test_name = "History (Direct) - Invalid startTime format"
    payload_bad_start = {"contractId": valid_contract_for_history, "startTime": "BAD-DATE", "endTime": end_time_iso, "unit": 2, "unitNumber": 1, "limit": 10}
    log_api_call_attempt(test_name, "/api/History/retrieveBars", payload_bad_start)
    direct_post(api_client, "/api/History/retrieveBars", payload_bad_start)
    # TODO: Observe, update client, test client mapping.

    # Scenario 4: endTime BEFORE startTime
    test_name = "History (Direct) - endTime before startTime"
    payload_bad_window = {"contractId": valid_contract_for_history, "startTime": start_time_iso, "endTime": earlier_end_time_iso, "unit": 2, "unitNumber": 1, "limit": 10}
    log_api_call_attempt(test_name, "/api/History/retrieveBars", payload_bad_window)
    direct_post(api_client, "/api/History/retrieveBars", payload_bad_window)
    # TODO: Observe, update client, test client mapping.

    # Scenario 5: Invalid unit
    test_name = "History (Direct) - Invalid unit"
    payload_bad_unit = {"contractId": valid_contract_for_history, "startTime": start_time_iso, "endTime": end_time_iso, "unit": 99, "unitNumber": 1, "limit": 10}
    log_api_call_attempt(test_name, "/api/History/retrieveBars", payload_bad_unit)
    direct_post(api_client, "/api/History/retrieveBars", payload_bad_unit)
    # TODO: Observe, update client, test client mapping.

    logger.info("====== Finished Testing History Retrieval Error Codes ======")

if __name__ == "__main__":
    run_history_error_tests()