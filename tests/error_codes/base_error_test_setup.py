# examples/test_error_codes/base_error_test_setup.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))

import logging
import json
from typing import Optional, Dict, Any
import requests # <--- ADD THIS LINE

from tsxapipy.auth import authenticate, AuthenticationError
from tsxapipy.api import APIClient, APIError, APIHttpError, APIResponseError
from tsxapipy import DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH, DEFAULT_CONFIG_CONTRACT_ID

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s')
logger = logging.getLogger("API_ERROR_TESTER")

def get_authenticated_client() -> Optional[APIClient]:
    """Authenticates and returns an APIClient instance or None on failure."""
    logger.info("Attempting authentication...")
    try:
        initial_token, token_acquired_at = authenticate()
        api_client = APIClient(initial_token=initial_token, token_acquired_at=token_acquired_at)
        logger.info("Authentication successful. APIClient initialized.")
        return api_client
    except (AuthenticationError, ValueError, APIError) as e:
        logger.error(f"Failed to authenticate or initialize APIClient: {e}")
        return None

def log_api_call_attempt(test_name: str, endpoint: str, payload: Optional[Dict[str, Any]] = None):
    logger.info(f"\n--- {test_name} ---")
    logger.info(f"Attempting call to: {endpoint}")
    if payload:
        logger.info(f"With payload: {json.dumps(payload, indent=2)}")

def log_api_response(response: requests.Response): # Now 'requests' is defined
    logger.info(f"HTTP Status Code: {response.status_code}")
    try:
        response_json = response.json()
        logger.info("Response JSON:")
        logger.info(json.dumps(response_json, indent=2))
        # Extract common fields for quick summary
        success = response_json.get("success")
        errorCode = response_json.get("errorCode")
        errorMessage = response_json.get("errorMessage")
        logger.info(f"Summary: success={success}, errorCode={errorCode}, errorMessage='{errorMessage}'")
    except json.JSONDecodeError:
        logger.warning("Response was not valid JSON.")
        logger.info(f"Response Text (first 500 chars): {response.text[:500]}")
    logger.info("-" * 30)

def log_client_exception(test_name: str, e: Exception):
    logger.error(f"--- {test_name} ---")
    logger.error(f"APIClient method raised an exception: {type(e).__name__}: {e}")
    if isinstance(e, APIHttpError):
        logger.error(f"  HTTP Status: {e.status_code}")
        logger.error(f"  Response Text: {e.response_text}")
    elif isinstance(e, APIResponseError):
        logger.error(f"  API Error Code: {e.error_code}")
        logger.error(f"  Raw Response: {e.raw_response}")
    logger.info("-" * 30)

# Get default account and contract for testing
TEST_ACCOUNT_ID = DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH
TEST_CONTRACT_ID = DEFAULT_CONFIG_CONTRACT_ID

if not TEST_ACCOUNT_ID:
    logger.warning("TEST_ACCOUNT_ID not found in config. Some tests will be skipped or may fail.")
if not TEST_CONTRACT_ID:
    logger.warning("TEST_CONTRACT_ID not found in config. Some tests will be skipped or may fail.")

# Helper to make direct post requests
def direct_post(api_client: APIClient, endpoint: str, payload: Dict[str,Any]):
    """Makes a direct post using the client's session and base_url for raw response."""
    if not api_client: return None
    full_url = f"{api_client.base_url}{endpoint}"
    try:
        response = api_client.session.post(full_url, json=payload, timeout=(api_client.connect_timeout, api_client.read_timeout))
        log_api_response(response)
        return response
    except requests.RequestException as e: # Now 'requests' is defined
        logger.error(f"Direct POST to {full_url} failed: {e}")
        return None

# This base setup file is not meant to be run directly,
# so the if __name__ == "__main__": block is not strictly necessary here.
# However, if you wanted to test some part of it:
# if __name__ == "__main__":
#     logger.info("Running base_error_test_setup.py directly (for testing setup itself).")
#     client = get_authenticated_client()
#     if client:
#         logger.info("Base setup: Authentication and client creation successful.")
#     else:
#         logger.error("Base setup: Authentication or client creation failed.")