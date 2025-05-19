# examples/test_error_codes/test_contract_search_by_id_errors.py
from base_error_test_setup import (
    get_authenticated_client, log_api_call_attempt, log_client_exception,
    direct_post, logger
)
from tsxapipy.api.exceptions import InvalidParameterError, ContractNotFoundError, APIHttpError

def run_contract_search_by_id_error_tests():
    api_client = get_authenticated_client()
    if not api_client: return
    logger.info("====== Testing Contract Search By ID Error Codes ======")

    # Scenario 1: Empty contractId
    test_name = "Contract Search By ID (Direct) - Empty contractId"
    payload = {"contractId": ""}
    log_api_call_attempt(test_name, "/api/Contract/searchById", payload)
    direct_post(api_client, "/api/Contract/searchById", payload)
    # TODO: Observe (likely HTTP 400 or errorCode). Update APIClient & test mapping.

    # Scenario 2: Non-existent contractId
    test_name = "Contract Search By ID (Direct) - Non-existent contractId"
    payload_non_existent = {"contractId": "CON.F.XX.DOESNOTEXIST.Z99"}
    log_api_call_attempt(test_name, "/api/Contract/searchById", payload_non_existent)
    direct_post(api_client, "/api/Contract/searchById", payload_non_existent)
    # TODO: Observe (errorCode 8 like place_order? or empty list?). Update APIClient & test mapping.
    # Example client test after mapping:
    # try:
    #     api_client.search_contract_by_id("CON.F.XX.DOESNOTEXIST.Z99")
    # except ContractNotFoundError as e: logger.info(f"{test_name} Client: SUCCESS - Caught {type(e).__name__}")
    # except Exception as e: log_client_exception(test_name + " Client", e)

    logger.info("====== Finished Testing Contract Search By ID Error Codes ======")

if __name__ == "__main__":
    run_contract_search_by_id_error_tests()