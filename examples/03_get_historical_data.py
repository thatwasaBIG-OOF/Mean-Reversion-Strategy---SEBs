"""
Example 03: Get Historical Data & Contract Details.

This script demonstrates:
1. Authenticating and initializing APIClient.
2. Using `get_futures_contract_details` to determine the current contract
   for a given symbol root.
3. Fetching historical bar data for the determined (or a default) contract.
"""
# pylint: disable=invalid-name # Allow filename for example script
# pylint: disable=too-many-locals, too-many-branches, too-many-statements
import sys
import os
import logging
from datetime import datetime, timedelta, date
from typing import Optional, Any # Added Any

# ---- sys.path modification ----
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ---- End sys.path modification ----

from tsxapipy import (
    APIClient,
    get_futures_contract_details,
    APIError,
    ContractNotFoundError,
    AuthenticationError,
    ConfigurationError,
    LibraryError,
    InvalidParameterError, # Added this
    MAX_BARS_PER_REQUEST,
    DEFAULT_CONFIG_CONTRACT_ID,
    UTC_TZ,
    authenticate
)

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def run_example():
    """Runs the example for getting historical data and contract details."""
    logger.info("--- Example 03: Get Historical Data & Contract Details ---")
    api_client: Optional[APIClient] = None
    try:
        # 1. Authenticate
        logger.info("Attempting authentication...")
        initial_token, token_acquired_at = authenticate()
        logger.info("Authentication successful. Token acquired at: %s",
                    token_acquired_at.isoformat())

        api_client = APIClient(initial_token=initial_token,
                               token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")

        # --- Part 1: Get Current Contract Details ---
        symbol_to_check = "NQ"
        processing_today = date.today()
        log_msg_contract = ("\nDetermining current contract details for %s as of %s...",
                            symbol_to_check, processing_today.isoformat())
        logger.info(*log_msg_contract)

        current_contract_string_id_for_history: Optional[str] = None
        current_contract_numeric_id_for_history: Optional[int] = None

        try:
            contract_details_tuple = get_futures_contract_details(
                api_client=api_client,
                processing_date=processing_today,
                symbol_root=symbol_to_check
            )
            if contract_details_tuple and contract_details_tuple[0]:
                string_id, numeric_id = contract_details_tuple
                logger.info("Determined Contract for %s today:", symbol_to_check)
                logger.info("  String ID (for most API calls, source col): %s", string_id)
                numeric_id_display = numeric_id if numeric_id is not None \
                                                else 'Not found/available'
                logger.info("  Numeric ID (potentially for history API): %s",
                            numeric_id_display)
                current_contract_string_id_for_history = string_id
                current_contract_numeric_id_for_history = numeric_id
            else:
                logger.error("Could not determine current contract for %s.",
                             symbol_to_check)
                current_contract_string_id_for_history = DEFAULT_CONFIG_CONTRACT_ID
                logger.warning("Falling back to default contract ID from config: %s "
                               "for history fetch.",
                               current_contract_string_id_for_history)

        except ContractNotFoundError as e:
            logger.error("Contract not found via utility for %s: %s", symbol_to_check, e)
            current_contract_string_id_for_history = DEFAULT_CONFIG_CONTRACT_ID
            logger.warning("Falling back to default contract ID from config: %s "
                           "for history fetch.",
                           current_contract_string_id_for_history)
        except APIError as e:
            logger.error("API error determining contract for %s: %s", symbol_to_check, e)
            current_contract_string_id_for_history = DEFAULT_CONFIG_CONTRACT_ID
            logger.warning("Falling back to default contract ID from config: %s "
                           "for history fetch.",
                           current_contract_string_id_for_history)

        # --- Part 2: Fetch Historical Data ---
        if not current_contract_string_id_for_history:
            logger.error("No contract ID available to fetch history. Exiting history part.")
            return

        history_api_contract_param: Any # Can be int or str
        if current_contract_numeric_id_for_history is not None:
            history_api_contract_param = current_contract_numeric_id_for_history
            logger.info("\nFetching history using NUMERIC contract ID: %s",
                        history_api_contract_param)
        else:
            history_api_contract_param = current_contract_string_id_for_history
            log_msg_hist = ("\nFetching history using STRING contract ID: %s "
                            "(numeric not available/resolved)",
                            history_api_contract_param)
            logger.info(*log_msg_hist)

        end_time_dt = datetime.now(UTC_TZ)
        start_time_dt = end_time_dt - timedelta(hours=1)

        logger.info("Fetching 1-minute bars for contract param '%s'",
                    history_api_contract_param)
        logger.info("Time window: %s to %s",
                    start_time_dt.isoformat(), end_time_dt.isoformat())

        try:
            bars = api_client.get_historical_bars(
                contract_id=history_api_contract_param,
                start_time_iso=start_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                end_time_iso=end_time_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                unit=2, # Minute
                unit_number=1,
                limit=MAX_BARS_PER_REQUEST
            )

            if bars:
                logger.info("Successfully fetched %d bars.", len(bars))
                for i, bar_data in enumerate(bars[:5]): # pylint: disable=disallowed-name
                    log_bar_details = (
                        "  Bar %d: T=%s, O=%s, H=%s, L=%s, C=%s, V=%s",
                        i + 1, bar_data.get('t'), bar_data.get('o'), bar_data.get('h'),
                        bar_data.get('l'), bar_data.get('c'), bar_data.get('v')
                    )
                    logger.info(*log_bar_details)
                if len(bars) > 5:
                    logger.info("  ...")
            else:
                logger.info("No bars returned for the period and contract.")

        except ContractNotFoundError as e:
            logger.error("History fetch failed: Contract (param: %s) not found: %s",
                         history_api_contract_param, e)
        except InvalidParameterError as e:
            logger.error("History fetch failed due to invalid parameters: %s", e)
        except APIError as e:
            logger.error("API Error during history fetch: %s", e)

    except ConfigurationError as e:
        logger.error("CONFIGURATION ERROR: %s", e)
    except AuthenticationError as e:
        logger.error("AUTHENTICATION FAILED: %s", e)
    except LibraryError as e:
        logger.error("LIBRARY ERROR: %s", e)
    except ValueError as e:
        logger.error("VALUE ERROR: %s", e)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("AN UNEXPECTED ERROR OCCURRED: %s", e_gen, exc_info=True)

if __name__ == "__main__":
    run_example()