# examples/10_programmatic_historical_updater.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

import logging
import argparse
from typing import Optional # datetime import was unused here directly
# from datetime import datetime # Not directly used, but often useful with historical data

from tsxapipy import (
    APIClient,
    HistoricalDataUpdater,
    authenticate,
    AuthenticationError,
    ConfigurationError,
    APIError,
    LibraryError, # For ParquetHandlerError, etc.
    DEFAULT_CONFIG_CONTRACT_ID # Though updater usually determines contract
)
from tsxapipy.api.exceptions import APIResponseParsingError # Added

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ProgrammaticUpdaterExample")

def run_updater_example(
    symbol_root: str,
    parquet_file_path: str,
    api_bar_unit: int,
    api_bar_unit_number: int,
    fetch_days_if_new: int,
    start_date_override: Optional[str] = None,
    end_date_override: Optional[str] = None
):
    logger.info(f"--- Example: Programmatic HistoricalDataUpdater ---")
    logger.info(f"Symbol: {symbol_root}, Target File: {parquet_file_path}")
    logger.info(f"API Bar: Unit={api_bar_unit}, Number={api_bar_unit_number}")
    logger.info(f"Fetch Days if New: {fetch_days_if_new}")
    if start_date_override: logger.info(f"Start Date Override: {start_date_override}")
    if end_date_override: logger.info(f"End Date Override: {end_date_override}")


    api_client: Optional[APIClient] = None
    # updater variable is already Optional[HistoricalDataUpdater] by inference

    try:
        logger.info("Authenticating...")
        initial_token, token_acquired_at = authenticate()
        
        api_client = APIClient(
            initial_token=initial_token,
            token_acquired_at=token_acquired_at
        )
        logger.info("APIClient initialized.")

        logger.info("Initializing HistoricalDataUpdater...")
        updater = HistoricalDataUpdater( # Define updater here
            api_client=api_client,
            symbol_root=symbol_root,
            main_parquet_file=parquet_file_path,
            temp_file_suffix="_prog_update_temp", 
            api_bar_unit=api_bar_unit,
            api_bar_unit_number=api_bar_unit_number,
            fetch_days_if_new=fetch_days_if_new,
            overall_start_date_override=start_date_override,
            overall_end_date_override=end_date_override
        )
        logger.info("HistoricalDataUpdater initialized.")

        logger.info("Starting data update process...")
        updater.update_data() 
        logger.info("Historical data update process completed successfully.")
        logger.info(f"Total new bars potentially added to temp file during this run: {updater.total_new_bars_appended_this_run}")


    except ConfigurationError as e_conf:
        logger.error(f"CONFIGURATION ERROR: {e_conf}")
    except AuthenticationError as e_auth:
        logger.error(f"AUTHENTICATION FAILED: {e_auth}")
    except APIResponseParsingError as e_parse: # Added
        logger.error(f"API RESPONSE PARSING ERROR during update: {e_parse}")
        if e_parse.raw_response_text:
            logger.error(f"Raw problematic response text (preview): {e_parse.raw_response_text[:500]}")
    except APIError as e_api:
        logger.error(f"API ERROR during update: {e_api}")
    except LibraryError as e_lib: 
        logger.error(f"LIBRARY ERROR during update: {e_lib}")
    except FileNotFoundError as e_fnf:
        logger.error(f"FILE NOT FOUND ERROR: {e_fnf}. Check Parquet file paths.")
    except Exception as e_generic: # Catch-all
        logger.error(f"AN UNEXPECTED ERROR OCCURRED: {e_generic}", exc_info=True)
    finally:
        logger.info("Programmatic updater example finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Programmatically run HistoricalDataUpdater.")
    parser.add_argument(
        "--symbol", 
        type=str, 
        default="NQ", 
        help="Symbol root for historical data (e.g., NQ, ES)."
    )
    parser.add_argument(
        "--file", 
        type=str, 
        default="data/programmatic_updater_nq_1min.parquet", 
        help="Path to the main Parquet file to update."
    )
    parser.add_argument(
        "--unit", 
        type=int, 
        default=2, 
        help="API bar unit (e.g., 2 for Minute, 3 for Hour, 4 for Day)."
    )
    parser.add_argument(
        "--unit_num", 
        type=int, 
        default=1, 
        help="API bar unit number (e.g., 1 for 1-minute, 5 for 5-minute)."
    )
    parser.add_argument(
        "--fetch_days", 
        type=int, 
        default=30, 
        help="Number of past days to fetch if Parquet file is new (and no start_date_override)."
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default=None, 
        help="Optional: Overall start date for fetching (YYYY-MM-DD). Overrides fetch_days/last known."
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None, 
        help="Optional: Overall end date for fetching (YYYY-MM-DD). Defaults to current day."
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

    output_dir = os.path.dirname(args.file)
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"Created output directory: {output_dir}")
        except OSError as e:
            logger.error(f"Could not create output directory {output_dir}: {e}. Parquet file saving might fail.")
            
    run_updater_example(
        symbol_root=args.symbol,
        parquet_file_path=args.file,
        api_bar_unit=args.unit,
        api_bar_unit_number=args.unit_num,
        fetch_days_if_new=args.fetch_days,
        start_date_override=args.start_date,
        end_date_override=args.end_date
    )