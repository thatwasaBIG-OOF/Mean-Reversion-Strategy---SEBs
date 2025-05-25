"""
CLI script to fetch and update historical bar data in Parquet files
for the TopStepX API.
"""
# pylint: disable=too-many-locals, too-many-branches, too-many-statements
import sys
import os

# ---- sys.path modification ----
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ---- End sys.path modification ----

import argparse
import logging
from typing import Optional 

from tsxapipy.auth import authenticate, AuthenticationError
from tsxapipy.historical import HistoricalDataUpdater
from tsxapipy.api import APIClient, APIError
from tsxapipy.api.exceptions import ConfigurationError, LibraryError, APIResponseParsingError # Added

# Configure logging for the CLI script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
script_logger = logging.getLogger(__name__) # Use __name__ for script-specific logger

def main(): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    """Main function to parse arguments and run historical data updater."""
    parser = argparse.ArgumentParser(
        description="Fetch and update historical bar data in Parquet files for TopStepX API.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "main_parquet_file",
        type=str,
        help="Path to the main Parquet file to update (e.g., data/nq_1min_utc.parquet)."
    )
    parser.add_argument(
        "--symbol_root", type=str, required=True,
        help="The root symbol of the instrument (e.g., ES, NQ, EP)."
    )
    parser.add_argument(
        "--contract_override", type=str, default=None,
        help="Optional: Explicitly specify a single contract ID to fetch data for. "
             "This will override automatic monthly/quarterly contract determination."
    )
    parser.add_argument(
        "--api_period", type=str, default="5min",
        choices=['1min', '5min', '15min', '30min', '60min', '1hour', '2hour', '4hour', 'daily'],
        help="The bar period/aggregation for API requests."
    )
    parser.add_argument(
        "--api_tf_value", type=int, default=0,
        help="Explicit timeframe value (unitNumber) for API requests. "
             "If 0 (default), it's parsed from --api_period (e.g., '5min' -> 5). "
             "Use this to override if --api_period (like '1hour') doesn't specify a number."
    )
    parser.add_argument(
        "--fetch_days_if_new", type=int, default=90,
        help="If the main Parquet file is new/empty, number of past days of data "
             "to attempt to fetch."
    )
    parser.add_argument(
        "--sleep_between_requests", type=float, default=0.25,
        help="Seconds to sleep between paginated API requests during historical data fetching."
    )
    parser.add_argument(
        "--temp_file_suffix", type=str, default="_update_temp",
        help="Suffix for the temporary Parquet file used during updates."
    )
    parser.add_argument(
        "--temp_file_dir", type=str, default=None,
        help="Optional directory for temporary Parquet files. "
             "If not specified, defaults to the same directory as the main_parquet_file."
    )
    parser.add_argument(
        "--max_gap_fill_passes", type=int, default=3,
        help="Maximum number of passes for iterative gap filling in historical data."
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Enable DEBUG level logging for all library and script loggers."
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG) # Set root logger level
        # This will ensure all handlers also respect the new level if not individually set lower.
        # for handler in logging.getLogger().handlers:
        #     handler.setLevel(logging.DEBUG) # This is often not needed if root is set
        script_logger.info("DEBUG logging enabled by CLI flag for all modules.")

    script_logger.info("--- Starting Historical Data Fetch for %s ---", args.symbol_root)
    script_logger.info("Target main Parquet file: %s", args.main_parquet_file)

    bar_unit: int = 0
    bar_unit_number: int = 0
    tf_val_from_period_str: str = ""

    try:
        period_lower = args.api_period.lower()
        if period_lower.endswith("min"):
            bar_unit = 2 # API unit for Minute
            tf_val_from_period_str = period_lower[:-3]
        elif period_lower.endswith("hour"):
            bar_unit = 3 # API unit for Hour
            tf_val_from_period_str = period_lower[:-4]
        elif period_lower == "daily":
            bar_unit = 4 # API unit for Day
            tf_val_from_period_str = "1" # Daily implies 1 unit of Day
        else:
            script_logger.error("Unsupported --api_period string: '%s'. Must be like '1min', '5min', '1hour', 'daily'.", args.api_period)
            sys.exit(1)

        if not tf_val_from_period_str: # If only "hour" was given, assume "1hour"
            if period_lower == "1hour": # Or just "hour"
                 tf_val_from_period_str = "1"
            else: # Should not happen if choices enforce format, but defensive
                script_logger.warning("Could not extract numeric value from --api_period "
                                      "'%s'. Assuming unit number of 1.", args.api_period)
                tf_val_from_period_str = "1"
        
        parsed_tf_val = int(tf_val_from_period_str) # Convert extracted string part to int
        bar_unit_number = args.api_tf_value if args.api_tf_value > 0 else parsed_tf_val
        
        if bar_unit_number <= 0:
            script_logger.warning("Resulting bar_unit_number is invalid (%d) after parsing. Defaulting to 1.",
                                  bar_unit_number)
            bar_unit_number = 1
            
    except ValueError:
        script_logger.error("Could not parse numeric value from period string part: '%s' "
                            "(derived from --api_period '%s')",
                            tf_val_from_period_str, args.api_period)
        sys.exit(1)
    except Exception as e_cfg_parse: # pylint: disable=broad-except
        script_logger.error("Error processing API period/timeframe arguments: %s", e_cfg_parse,
                            exc_info=True)
        sys.exit(1)

    log_api_params = ("Determined API params: unit=%d, unit_number=%d from period '%s' "
                      "(explicit tf_value override: %s)", 
                      bar_unit, bar_unit_number, args.api_period,
                      args.api_tf_value if args.api_tf_value > 0 else 'N/A (used parsed value)')
    script_logger.info(*log_api_params)

    api_client_instance: Optional[APIClient] = None
    try:
        script_logger.info("Authenticating with API...")
        initial_token, token_acquired_at = authenticate()

        api_client_instance = APIClient(
            initial_token=initial_token,
            token_acquired_at=token_acquired_at
        )
        script_logger.info("APIClient initialized successfully.")

        script_logger.info("Initializing HistoricalDataUpdater for symbol: %s...",
                           args.symbol_root)
        updater = HistoricalDataUpdater(
            api_client=api_client_instance,
            symbol_root=args.symbol_root,
            main_parquet_file=args.main_parquet_file,
            temp_file_suffix=args.temp_file_suffix,
            api_bar_unit=bar_unit,
            api_bar_unit_number=bar_unit_number,
            contract_override=args.contract_override,
            fetch_days_if_new=args.fetch_days_if_new,
            sleep_between_requests=args.sleep_between_requests,
            max_gap_fill_passes=args.max_gap_fill_passes,
            temp_file_dir=args.temp_file_dir
        )
        script_logger.info("HistoricalDataUpdater initialized. "
                           "Starting data update process...")
        updater.update_data()
        script_logger.info("Historical data update process completed successfully.")

    except ConfigurationError as e_conf:
        script_logger.error("CONFIGURATION ERROR: %s", e_conf)
        script_logger.error("Please ensure API_KEY and USERNAME are correctly set "
                            "in your .env file (at project root).")
    except AuthenticationError as e_auth:
        script_logger.error("AUTHENTICATION FAILED: %s", e_auth)
    except APIResponseParsingError as e_parse: # Added
        script_logger.error("API RESPONSE PARSING ERROR (likely during auth or internal APIClient calls): %s", e_parse)
        if e_parse.raw_response_text:
            script_logger.error("Raw problematic response text (preview): %s", e_parse.raw_response_text[:500])
    except APIError as e_api: # Errors from HistoricalDataUpdater using APIClient
        script_logger.error("API ERROR during data update: %s", e_api)
    except LibraryError as e_lib: # Covers ParquetHandlerError etc.
        script_logger.error("LIBRARY ERROR during data update: %s", e_lib)
    except FileNotFoundError as e_fnf:
        script_logger.error("FILE NOT FOUND ERROR: %s. Check paths for Parquet files.", e_fnf)
    except ValueError as e_val: # For int conversion errors or other value issues
        script_logger.error("VALUE ERROR during script execution: %s", e_val)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        script_logger.error("AN UNEXPECTED CRITICAL ERROR occurred in CLI execution: %s",
                            e_gen, exc_info=True)

if __name__ == "__main__":
    main()