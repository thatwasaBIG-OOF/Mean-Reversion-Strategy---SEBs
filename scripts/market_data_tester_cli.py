"""
CLI script to test the real-time market data stream (DataStream)
from the TopStepX API.
"""
# pylint: disable=too-many-locals, too-many-branches, too-many-statements
import sys
import os

# ---- sys.path modification ----
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
# ---- End sys.path modification ----

import logging
import time
import pprint
import argparse
from typing import Optional, Any

from tsxapipy.auth import authenticate, AuthenticationError
from tsxapipy.api import APIClient, APIError
from tsxapipy.real_time import DataStream, StreamConnectionState # Added StreamConnectionState
from tsxapipy.config import CONTRACT_ID as DEFAULT_CONTRACT_ID
from tsxapipy.api.exceptions import ConfigurationError, LibraryError, APIResponseParsingError # Added

# Configure logging for the CLI script
logging.basicConfig( # BasicConfig should be called only once at the application entry point
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    # force=True # force=True can be useful if other parts of library also call basicConfig
)
logger = logging.getLogger(__name__) # Use __name__ for script-specific logger


# pylint: disable=global-statement
current_contract_id_testing: str = ""

def handle_quote_data(quote_payload: Any):
    """Callback for quote data from the stream."""
    logger.info("--- Received GatewayQuote for %s ---", current_contract_id_testing or "N/A")
    logger.info(pprint.pformat(quote_payload, indent=1, width=100, compact=True))

def handle_optional_trade_data(trade_payload: Any):
    """Callback for trade data from the stream."""
    logger.info("--- Received GatewayTrade (Optional) for %s ---",
                current_contract_id_testing or "N/A")
    logger.info(pprint.pformat(trade_payload, indent=1, width=100, compact=True))

def handle_optional_depth_data(depth_payload: Any):
    """Callback for depth data from the stream."""
    logger.info("--- Received GatewayDepth (Optional) for %s ---",
                current_contract_id_testing or "N/A")
    logger.info(pprint.pformat(depth_payload, indent=1, width=100, compact=True))

def handle_cli_stream_state_change(state_str: str): # Receives state name as string
    """Callback for stream state changes."""
    logger.info("CLI MarketDataStream state changed to: '%s' for contract %s",
                state_str, current_contract_id_testing or "N/A")

def handle_cli_stream_error(error: Any):
    """Callback for stream errors."""
    logger.error("CLI MarketDataStream encountered an error for contract %s: %s",
                 current_contract_id_testing or "N/A", error)

def main(): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    """Main function to parse arguments and run the market data tester."""
    parser = argparse.ArgumentParser(
        description="Test real-time market data stream from TopStepX API.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--contract_id", type=str, default=DEFAULT_CONTRACT_ID,
        help=f"Contract ID to subscribe to. Default is from config/env: "
             f"{DEFAULT_CONTRACT_ID or 'Not Set'}" # Show if default is None
    )
    parser.add_argument(
        "--quotes", action="store_true", default=True, # Quotes enabled by default
        help="Subscribe to quote updates (default behavior)."
    )
    parser.add_argument(
        "--no-quotes", action="store_false", dest="quotes",
        help="Disable quote subscription."
    )
    parser.add_argument("--trades", action="store_true",
                        help="Subscribe to trade updates.")
    parser.add_argument("--depth", action="store_true",
                        help="Subscribe to market depth updates.")
    parser.add_argument("--debug", action="store_true",
                        help="Enable DEBUG level logging for all modules.")

    args = parser.parse_args()

    # Reconfigure logging level if --debug is passed AFTER basicConfig has run
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        # for handler in logging.getLogger().handlers: # If basicConfig already set handlers
        #     handler.setLevel(logging.DEBUG)
        logger.info("DEBUG logging enabled by CLI flag for all modules.")


    global current_contract_id_testing
    current_contract_id_testing = args.contract_id

    if not current_contract_id_testing:
        logger.error("No Contract ID specified. Provide --contract_id or set "
                     "CONTRACT_ID in your .env file.")
        sys.exit(1)

    logger.info("Starting Market Data Tester for CONTRACT_ID: %s",
                current_contract_id_testing)
    logger.info("Attempting Subscriptions: Quotes=%s, Trades=%s, Depth=%s",
                args.quotes, args.trades, args.depth)

    api_client_instance: Optional[APIClient] = None
    stream: Optional[DataStream] = None

    try:
        logger.info("Authenticating with API...")
        initial_token, token_acquired_time = authenticate()
        api_client_instance = APIClient(
            initial_token=initial_token,
            token_acquired_at=token_acquired_time
        )
        logger.info("APIClient initialized successfully.")

        quote_cb = handle_quote_data if args.quotes else None
        trade_cb = handle_optional_trade_data if args.trades else None
        depth_cb = handle_optional_depth_data if args.depth else None

        if not (quote_cb or trade_cb or depth_cb):
            logger.warning("No data subscriptions enabled (quotes, trades, or depth). "
                           "Stream will start but show no market data.")

        logger.info("Initializing DataStream...")
        stream = DataStream(
            api_client=api_client_instance,
            contract_id_to_subscribe=current_contract_id_testing,
            on_quote_callback=quote_cb,
            on_trade_callback=trade_cb,
            on_depth_callback=depth_cb,
            on_state_change_callback=handle_cli_stream_state_change,
            on_error_callback=handle_cli_stream_error
        )
        logger.info("DataStream initialized.")


        if not stream.start():
            # Log current stream status name if start fails
            status_name = stream.connection_status.name if stream else "N/A (stream object None)"
            logger.error(f"Failed to start DataStream (current status: {status_name}). Exiting.")
            sys.exit(1)

        logger.info("Stream processing started. Press Ctrl+C to stop.")
        while True:
            # Loop to keep the main thread alive while the stream runs in background threads.
            # Check stream status periodically for debugging or more complex logic.
            if stream and stream.connection_status == StreamConnectionState.ERROR:
                logger.error("Stream is in ERROR state. Terminating monitor.")
                break
            if stream and stream.connection_status == StreamConnectionState.DISCONNECTED:
                logger.warning("Stream is DISCONNECTED (and not auto-reconnecting if retries exhausted). Terminating monitor.")
                break
            time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down gracefully...")
    except ConfigurationError as e_conf:
        logger.error("CONFIGURATION ERROR: %s", e_conf)
        logger.error("Please ensure API_KEY and USERNAME are correctly set "
                     "in your .env file.")
    except AuthenticationError as e_auth:
        logger.error("AUTHENTICATION FAILED: %s", e_auth)
    except APIResponseParsingError as e_parse:
        logger.error(f"API RESPONSE PARSING ERROR (likely during auth): {e_parse}")
        if e_parse.raw_response_text:
            logger.error("Raw problematic response text (preview): %s", e_parse.raw_response_text[:500])
    except APIError as e_api: # Catch other API errors
        logger.error("API ERROR: %s", e_api)
    except LibraryError as e_lib: # Catch other library errors
        logger.error("LIBRARY ERROR: %s", e_lib)
    except ValueError as e_val: # e.g. from APIClient init if token format is wrong
        logger.error("VALUE ERROR: %s", e_val)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.critical("UNHANDLED CRITICAL ERROR in Market Data Tester: %s",
                        e_gen, exc_info=True)
    finally:
        if stream:
            status_name = stream.connection_status.name if stream.connection_status else "N/A"
            logger.info(f"Stopping data stream (current status: {status_name})...")
            stream.stop(reason_for_stop="CLI script finishing")
        logger.info("Market Data Tester terminated.")

if __name__ == "__main__":
    main()