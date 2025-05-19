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
from typing import Optional # For APIClient type hint

from tsxapipy.auth import authenticate, AuthenticationError
from tsxapipy.historical import HistoricalDataUpdater
from tsxapipy.api import APIClient, APIError
from tsxapipy.api.exceptions import ConfigurationError, LibraryError

# Configure logging for the CLI script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
script_logger = logging.getLogger(__name__)

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
        logging.getLogger().setLevel(logging.DEBUG)
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.DEBUG)
        script_logger.info("DEBUG logging enabled by CLI flag for all modules.")

    script_logger.info("--- Starting Historical Data Fetch for %s ---", args.symbol_root)
    script_logger.info("Target main Parquet file: %s", args.main_parquet_file)

    bar_unit: int = 0
    bar_unit_number: int = 0
    tf_val_from_period_str: str = ""

    try:
        period_lower = args.api_period.lower()
        if period_lower.endswith("min"):
            bar_unit = 2
            tf_val_from_period_str = period_lower[:-3]
        elif period_lower.endswith("hour"):
            bar_unit = 3
            tf_val_from_period_str = period_lower[:-4]
        elif period_lower == "daily":
            bar_unit = 4
            tf_val_from_period_str = "1"
        else:
            script_logger.error("Unsupported --api_period string: '%s'.", args.api_period)
            sys.exit(1)

        if not tf_val_from_period_str:
            if period_lower == "1hour":
                tf_val_from_period_str = "1"
            else:
                script_logger.warning("Could not extract numeric value from --api_period "
                                      "'%s'. Assuming unit number of 1.", args.api_period)
                tf_val_from_period_str = "1"
        parsed_tf_val = int(tf_val_from_period_str)
        bar_unit_number = args.api_tf_value if args.api_tf_value > 0 else parsed_tf_val
        if bar_unit_number <= 0:
            script_logger.warning("Resulting bar_unit_number is invalid (%d). Defaulting to 1.",
                                  bar_unit_number)
            bar_unit_number = 1
    except ValueError:
        script_logger.error("Could not parse numeric value from period string part: '%s' "
                            "(derived from '%s')",
                            tf_val_from_period_str, args.api_period)
        sys.exit(1)
    except Exception as e_cfg: # pylint: disable=broad-exception-caught
        script_logger.error("Error processing API period/timeframe arguments: %s", e_cfg,
                            exc_info=True)
        sys.exit(1)

    log_api_params = ("Determined API params: unit=%d, unit_number=%d from period '%s' "
                      "(explicit tf_value: %s)", bar_unit, bar_unit_number, args.api_period,
                      args.api_tf_value if args.api_tf_value > 0 else 'N/A')
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

    except ConfigurationError as e:
        script_logger.error("CONFIGURATION ERROR: %s", e)
        script_logger.error("Please ensure API_KEY and USERNAME are correctly set "
                            "in your .env file (at project root).")
    except AuthenticationError as e:
        script_logger.error("AUTHENTICATION FAILED: %s", e)
    except APIError as e:
        script_logger.error("API ERROR during data update: %s", e)
    except LibraryError as e:
        script_logger.error("LIBRARY ERROR during data update: %s", e)
    except FileNotFoundError as e:
        script_logger.error("FILE NOT FOUND ERROR: %s. Check paths for Parquet files.", e)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        script_logger.error("AN UNEXPECTED CRITICAL ERROR occurred in CLI execution: %s",
                            e_gen, exc_info=True)

if __name__ == "__main__":
    main()