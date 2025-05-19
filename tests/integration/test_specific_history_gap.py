# topstep_data_suite/tests/integration/test_specific_history_gap.py

# ---- sys.path modification FOR DIRECT EXECUTION ----
import sys
import os

_src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)
# ---- End sys.path modification ----

import requests
import json
from datetime import datetime, timedelta
import pytz
import pandas as pd
import logging

try:
    from tsxapipy.config import API_URL as LIB_API_URL, USERNAME as LIB_USERNAME, API_KEY as LIB_API_KEY
    from tsxapipy.auth import authenticate
    from tsxapipy.common.time_utils import UTC_TZ
    from tsxapipy.api.client import APIClient
except ImportError:
    print("CRITICAL WARNING: Could not import from tsxapipy even after sys.path modification. "
          "Ensure the project structure is correct and 'src' directory exists at the expected location relative to this test file.")
    LIB_API_URL, LIB_USERNAME, LIB_API_KEY = None, None, None
    authenticate = None # type: ignore
    APIClient = None # type: ignore
    class FallbackUTC:
        def __str__(self): return "UTC"
        def localize(self, dt): return dt.replace(tzinfo=self) if dt.tzinfo is None else dt.astimezone(self)
        def normalize(self, dt): return self.localize(dt)
        def utcoffset(self, dt): return timedelta(0)
        def dst(self, dt): return timedelta(0)
        def tzname(self, dt): return "UTC"
    UTC_TZ = FallbackUTC() # type: ignore

logger = logging.getLogger("tests.integration.specific_history_gap")

API_URL = LIB_API_URL if LIB_API_URL else os.getenv("API_URL_OVERRIDE_ENV", "https://api.topstepx.com")
USERNAME = LIB_USERNAME if LIB_USERNAME else os.getenv("USERNAME", "your_username_env_fallback")
API_KEY = LIB_API_KEY if LIB_API_KEY else os.getenv("API_KEY", "your_api_key_env_fallback")

CONTRACT_TO_TEST = os.getenv("TEST_GAP_CONTRACT_ID", "CON.F.US.ENQ.M25")
LAST_KNOWN_BAR_TIMESTAMP_STR = os.getenv("TEST_GAP_LAST_TS", "2025-05-16T04:20:00+00:00")

def get_api_token_from_library_or_direct(username_param, api_key_param, api_url_param) -> tuple[str | None, str | None]:
    token_str: str | None = None

    if authenticate:
        logger.info("Attempting authentication via library function...")
        try:
            token_tuple = authenticate()
            token_str = token_tuple[0]
            logger.info("Library authentication successful.")
            return token_str, "library_auth"
        except Exception as e:
            logger.warning(f"Library authentication failed: {e}. Falling back to direct auth for test.")

    logger.info("Attempting direct authentication...")
    auth_url = f"{api_url_param}/api/Auth/loginKey"
    headers = {"Content-Type": "application/json"}
    data = {"userName": username_param, "apiKey": api_key_param}
    try:
        response = requests.post(auth_url, headers=headers, json=data, timeout=10)
        response.raise_for_status()
        token_data = response.json()
        token_str = token_data.get("token")
        if not token_str:
            logger.error("Direct auth failed: Token not in response.")
            return None, "direct_auth_failed"
        logger.info("Direct authentication successful.")
        return token_str, "direct_auth"
    except requests.exceptions.RequestException as e:
        logger.error(f"Direct authentication network error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response content: {e.response.text}")
        return None, "direct_auth_network_error"
    except Exception as e:
        logger.error(f"Direct authentication unexpected error: {e}")
        return None, "direct_auth_unexpected_error"


def fetch_bars_via_direct_post(token: str, contract_id: str, start_time_iso: str, end_time_iso: str,
                               api_url_param: str, unit: int = 2, unit_number: int = 1, limit: int = 10) -> list:
    request_url = f"{api_url_param}/api/History/retrieveBars"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "contractId": contract_id, "live": False, "startTime": start_time_iso,
        "endTime": end_time_iso, "unit": unit, "unitNumber": unit_number,
        "limit": limit, "includePartialBar": False
    }
    logger.info(f"\nRequesting bars for {contract_id} (Direct API Call)")
    logger.info(f"  URL:       {request_url}")
    logger.info(f"  StartTime: {start_time_iso}")
    logger.info(f"  EndTime:   {end_time_iso}")
    logger.info(f"  Limit:     {limit}")
    logger.debug(f"  Payload:   {json.dumps(payload)}")

    try:
        response = requests.post(request_url, headers=headers, json=payload, timeout=15)
        logger.info(f"API Response Status Code: {response.status_code}")
        response_data = response.json()
        logger.info("API Full Response (Direct Call):")
        logger.info(json.dumps(response_data, indent=2))

        if response_data.get("success"):
            bars = response_data.get("bars", [])
            if bars is None: bars = []
            logger.info(f"\nNumber of bars received: {len(bars)}")
            if bars:
                logger.info("First few bars received:")
                for i, bar in enumerate(bars[:min(5, len(bars))]):
                    logger.info(f"  Bar {i+1}: {bar}")
            else:
                logger.info("No bars were returned in this specific window.")
            return bars
        else:
            logger.error(f"API indicated failure: errorCode={response_data.get('errorCode')}, "
                         f"errorMessage={response_data.get('errorMessage')}")
            return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error during direct API call: {e}", exc_info=True)
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response content (if any): {e.response.text}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON response from API: {e}", exc_info=True)
        if 'response' in locals() and response is not None:
             logger.error(f"Response text that failed to parse: {response.text}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error during direct API call: {e}", exc_info=True)
        return []

def test_specific_gap_scenario():
    try:
        import pytest
    except ImportError:
        class PytestMock:
            def skip(self, reason, allow_module_level=False): print(f"MOCK PYTEST SKIP: {reason}"); return
            def fail(self, reason): print(f"MOCK PYTEST FAIL: {reason}"); raise AssertionError(reason)
        pytest = PytestMock() # type: ignore

    logger.info("--- Starting Specific History Gap Integration Test ---")
    if USERNAME == "your_username_env_fallback" or API_KEY == "your_api_key_env_fallback":
        logger.warning("Default/Placeholder API credentials detected. This test requires live credentials.")
        pytest.skip("API credentials are placeholders, cannot run live integration test.")
        return

    api_token, auth_method = get_api_token_from_library_or_direct(USERNAME, API_KEY, API_URL)

    if not api_token:
        logger.error("Failed to obtain API token for test using any method. Test cannot proceed.")
        pytest.fail(f"Could not obtain API token (tried {auth_method}). Test aborted.")
        return
    logger.info(f"API token obtained via: {auth_method}")

    try:
        last_known_dt_utc_aware = pd.to_datetime(LAST_KNOWN_BAR_TIMESTAMP_STR)
        if last_known_dt_utc_aware.tzinfo is None:
            last_known_dt_utc_aware = UTC_TZ.localize(last_known_dt_utc_aware)
        else:
            last_known_dt_utc_aware = last_known_dt_utc_aware.astimezone(UTC_TZ)
    except Exception as e:
        logger.error(f"Error parsing LAST_KNOWN_BAR_TIMESTAMP_STR ('{LAST_KNOWN_BAR_TIMESTAMP_STR}'): {e}")
        pytest.fail(f"Could not parse test timestamp: {e}")
        return

    logger.info(f"Using Contract: {CONTRACT_TO_TEST}, Last Known Bar TS (UTC): {last_known_dt_utc_aware.isoformat()}")

    bar_interval = timedelta(minutes=1)
    
    test_start_dt_1 = last_known_dt_utc_aware + bar_interval
    test_end_dt_1 = test_start_dt_1 + timedelta(minutes=30)
    test_start_iso_1 = test_start_dt_1.strftime("%Y-%m-%dT%H:%M:%SZ")
    test_end_iso_1 = test_end_dt_1.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info("\n--- Test Case 1: Querying data immediately AFTER the last known bar's interval ---")
    bars1 = fetch_bars_via_direct_post(api_token, CONTRACT_TO_TEST, test_start_iso_1, test_end_iso_1, API_URL, limit=10)
    assert isinstance(bars1, list), "Test Case 1: fetch_bars_via_direct_post should return a list."
    logger.info(f"Test Case 1 returned {len(bars1)} bars.")

    test_start_dt_2 = last_known_dt_utc_aware
    test_end_dt_2 = test_start_dt_2 + timedelta(minutes=30)
    test_start_iso_2 = test_start_dt_2.strftime("%Y-%m-%dT%H:%M:%SZ")
    test_end_iso_2 = test_end_dt_2.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info("\n--- Test Case 2: Querying data starting AT the last known bar's timestamp ---")
    bars2 = fetch_bars_via_direct_post(api_token, CONTRACT_TO_TEST, test_start_iso_2, test_end_iso_2, API_URL, limit=10)
    assert isinstance(bars2, list), "Test Case 2: fetch_bars_via_direct_post should return a list."
    logger.info(f"Test Case 2 returned {len(bars2)} bars.")
    if bars2:
        first_bar2_ts_str = bars2[0].get('t')
        if first_bar2_ts_str:
            try:
                first_bar2_dt = pd.to_datetime(first_bar2_ts_str).astimezone(UTC_TZ)
                assert first_bar2_dt >= last_known_dt_utc_aware, \
                    f"Test Case 2: First bar timestamp {first_bar2_dt} is before last known {last_known_dt_utc_aware}"
            except Exception as e_ts:
                logger.warning(f"Could not parse or compare timestamp from Test Case 2's first bar: {e_ts}")

    logger.info("--- Specific History Gap Integration Test Finished ---")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - [%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True
    )
    logger.info("Running `test_specific_history_gap.py` directly...")
    test_specific_gap_scenario()
    logger.info("Direct execution of `test_specific_history_gap.py` complete.")