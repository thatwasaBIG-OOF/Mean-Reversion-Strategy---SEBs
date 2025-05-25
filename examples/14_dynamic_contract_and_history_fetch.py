# examples/14_dynamic_contract_and_history_fetch.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

import logging
import argparse
from datetime import datetime, timedelta, date
from typing import Optional, Any, List # Added List for type hinting

from tsxapipy import (
    APIClient,
    get_futures_contract_details, 
    authenticate,
    AuthenticationError,
    ConfigurationError,
    APIError,
    ContractNotFoundError,
    InvalidParameterError, 
    MAX_BARS_PER_REQUEST,
    UTC_TZ,
    LibraryError, # Added
    api_schemas # Import Pydantic schemas
)
from tsxapipy.api.exceptions import APIResponseParsingError # Added

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("DynamicContractHistoryExample")

def run_dynamic_contract_example(symbol_root: str, history_lookback_hours: int):
    logger.info(f"--- Example: Dynamic Contract Resolution & History Fetch (Using Pydantic Models) ---")
    logger.info(f"Symbol Root: {symbol_root}, History Lookback: {history_lookback_hours} hours")

    api_client: Optional[APIClient] = None

    try:
        logger.info("Authenticating...")
        initial_token, token_acquired_at = authenticate()
        
        api_client = APIClient(
            initial_token=initial_token,
            token_acquired_at=token_acquired_at
        )
        logger.info("APIClient initialized.")

        # --- 1. Dynamically Resolve Current Contract ---
        logger.info(f"\n--- Resolving current contract for symbol root: '{symbol_root}' ---")
        processing_today = date.today() 
        
        string_contract_id: Optional[str] = None
        numeric_contract_id_for_history: Optional[int] = None 

        try:
            # get_futures_contract_details signature and return type remain the same externally
            contract_details_tuple = get_futures_contract_details(
                api_client=api_client,
                processing_date=processing_today,
                symbol_root=symbol_root
            )
            if contract_details_tuple and contract_details_tuple[0]: 
                string_contract_id, numeric_contract_id_for_history = contract_details_tuple
                logger.info(f"Successfully resolved contract for '{symbol_root}' as of {processing_today.isoformat()}:")
                logger.info(f"  String Contract ID (for general API use, logging): {string_contract_id}")
                logger.info(f"  Numeric Contract ID (preferred for history API):   {numeric_contract_id_for_history if numeric_contract_id_for_history is not None else 'Not resolved/available'}")
            else:
                logger.error(f"Could not resolve a contract for '{symbol_root}'. Cannot fetch history.")
                return 

        except ContractNotFoundError as e_cnf_resolve: # Can be raised by get_futures_contract_details if underlying search finds nothing
            logger.error(f"ContractNotFoundError while resolving contract for '{symbol_root}': {e_cnf_resolve}")
            logger.info("This might mean no active contract matches the resolution logic for the current date, or an API issue.")
            return
        except APIError as e_api_contract: # Other API errors from get_futures_contract_details
            logger.error(f"APIError during contract resolution for '{symbol_root}': {e_api_contract}")
            return
        except Exception as e_contract_resolve: # pylint: disable=broad-except
            logger.error(f"Unexpected error resolving contract for '{symbol_root}': {e_contract_resolve}", exc_info=True)
            return

        # --- 2. Fetch Recent Historical Data for the Resolved Contract ---
        logger.info(f"\n--- Fetching recent historical bars for resolved contract ---")
        
        history_api_contract_param: Any # For APIClient call
        if numeric_contract_id_for_history is not None:
            history_api_contract_param = numeric_contract_id_for_history
            logger.info(f"Using NUMERIC contract ID for history API: {history_api_contract_param}")
        elif string_contract_id: 
            history_api_contract_param = string_contract_id
            logger.info(f"Using STRING contract ID for history API (numeric not available/resolved): {history_api_contract_param}")
            logger.warning("If the history API strictly requires an integer ID for this contract type, this call might fail or return no data.")
        else: 
            logger.error("Critical: No contract ID available for history fetch after resolution step.")
            return

        end_time_utc = datetime.now(UTC_TZ)
        start_time_utc = end_time_utc - timedelta(hours=history_lookback_hours)
        
        start_iso = start_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = end_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(f"Fetching 1-minute bars for contract param '{history_api_contract_param}' (Source String ID: {string_contract_id})")
        logger.info(f"Time window: {start_iso} to {end_iso}")

        try:
            # api_client.get_historical_bars now returns schemas.HistoricalBarsResponse
            historical_response: api_schemas.HistoricalBarsResponse = api_client.get_historical_bars(
                contract_id=history_api_contract_param,
                start_time_iso=start_iso,
                end_time_iso=end_iso,
                unit=2, # Minute
                unit_number=1, # 1-minute bars
                limit=MAX_BARS_PER_REQUEST 
            )

            # Access .bars attribute for List[api_schemas.BarData]
            bar_models: List[api_schemas.BarData] = historical_response.bars
            
            if bar_models:
                logger.info(f"Successfully fetched {len(bar_models)} bar(s) for '{history_api_contract_param}'.")
                logger.info("First 5 bars (oldest first):")
                for i, bar_model_data in enumerate(bar_models[:5]):
                    # Access attributes directly from Pydantic model
                    # bar_model_data.t is already a datetime object
                    log_bar_details = (
                        "  Bar %d: T=%s, O=%.2f, H=%.2f, L=%.2f, C=%.2f, V=%.0f",
                        i + 1, 
                        bar_model_data.t.isoformat(), # Format datetime for logging
                        bar_model_data.o,
                        bar_model_data.h,
                        bar_model_data.l,
                        bar_model_data.c,
                        bar_model_data.v
                    )
                    logger.info(*log_bar_details)
                if len(bar_models) > 5:
                    logger.info("  ...")
            else:
                logger.info(f"No bars returned for '{history_api_contract_param}' in the specified period.")

        except ContractNotFoundError as e_hist_cnf: 
            logger.error(f"History fetch failed: Contract (param: {history_api_contract_param}, source: {string_contract_id or 'N/A'}) not found by history API: {e_hist_cnf}")
        except InvalidParameterError as e_hist_param:
            logger.error(f"History fetch failed due to invalid parameters for {history_api_contract_param}: {e_hist_param}")
        except APIResponseParsingError as e_parse_hist:
            logger.error(f"API RESPONSE PARSING ERROR during history fetch for {history_api_contract_param}: {e_parse_hist}")
            if e_parse_hist.raw_response_text:
                logger.error("Raw problematic response text (preview): %s", e_parse_hist.raw_response_text[:500])
        except APIError as e_hist_api:
            logger.error(f"APIError during history fetch for {history_api_contract_param}: {e_hist_api}")
        except Exception as e_hist_gen: # pylint: disable=broad-except
            logger.error(f"Unexpected error during history fetch for {history_api_contract_param}: {e_hist_gen}", exc_info=True)
            
    except ConfigurationError as e_conf:
        logger.error(f"CONFIGURATION ERROR: {e_conf}")
    except AuthenticationError as e_auth:
        logger.error(f"AUTHENTICATION FAILED: {e_auth}")
    except APIResponseParsingError as e_parse_main: # Could occur if get_futures_contract_details has parsing issue
        logger.error(f"API RESPONSE PARSING ERROR (likely during contract resolution): {e_parse_main}")
    except APIError as e_general_api: 
        logger.error(f"GENERAL API ERROR: {e_general_api}")
    except LibraryError as e_lib: # Broader library errors
        logger.error(f"LIBRARY ERROR: {e_lib}")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Exiting...")
    except Exception as e_main: # pylint: disable=broad-except
        logger.error(f"AN UNEXPECTED ERROR OCCURRED IN MAIN EXECUTION: {e_main}", exc_info=True)
    finally:
        logger.info("Dynamic contract resolution and history fetch example finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Resolve current futures contract and fetch recent history.")
    parser.add_argument(
        "--symbol", 
        type=str, 
        default="NQ", 
        help="Futures symbol root (e.g., NQ, ES, CL, GC) to resolve."
    )
    parser.add_argument(
        "--hours", 
        type=int, 
        default=1, 
        help="Number of past hours of 1-minute historical data to fetch for the resolved contract."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG level logging."
    )
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.DEBUG)
        logger.info("DEBUG logging enabled.")

    if args.hours <=0:
        logger.error("--hours must be a positive integer.")
        sys.exit(1)

    run_dynamic_contract_example(
        symbol_root=args.symbol,
        history_lookback_hours=args.hours
    )