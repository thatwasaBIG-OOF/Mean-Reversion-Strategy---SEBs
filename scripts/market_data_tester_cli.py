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
from tsxapipy.real_time import DataStream
from tsxapipy.config import CONTRACT_ID as DEFAULT_CONTRACT_ID
from tsxapipy.api.exceptions import ConfigurationError, LibraryError

logger = logging.getLogger(__name__)

# pylint: disable=global-statement
current_contract_id_testing: str = ""

def handle_quote_data(quote_payload: Any):
    """Callback for quote data from the stream."""
    logger.info("--- Received GatewayQuote for %s ---", current_contract_id_testing)
    logger.info(pprint.pformat(quote_payload, indent=2, width=120))

def handle_optional_trade_data(trade_payload: Any):
    """Callback for trade data from the stream."""
    logger.info("--- Received GatewayTrade (Optional) for %s ---",
                current_contract_id_testing)
    logger.info(pprint.pformat(trade_payload, indent=2, width=120))

def handle_optional_depth_data(depth_payload: Any):
    """Callback for depth data from the stream."""
    logger.info("--- Received GatewayDepth (Optional) for %s ---",
                current_contract_id_testing)
    logger.info(pprint.pformat(depth_payload, indent=2, width=120))

def handle_cli_stream_state_change(state: str):
    """Callback for stream state changes."""
    logger.info("CLI MarketDataStream state changed to: '%s' for contract %s",
                state, current_contract_id_testing)

def handle_cli_stream_error(error: Any):
    """Callback for stream errors."""
    logger.error("CLI MarketDataStream encountered an error for contract %s: %s",
                 current_contract_id_testing, error)

def main(): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    """Main function to parse arguments and run the market data tester."""
    parser = argparse.ArgumentParser(
        description="Test real-time market data stream from TopStepX API.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--contract_id", type=str, default=DEFAULT_CONTRACT_ID,
        help=f"Contract ID to subscribe to. Default is from config/env: "
             f"{DEFAULT_CONTRACT_ID}"
    )
    parser.add_argument(
        "--quotes", action="store_true", default=True,
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

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True
    )
    if args.debug:
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

        # Example of finer-grained logging for library components if needed
        # logging.getLogger("tsxapipy").setLevel(logging.DEBUG)
        # logging.getLogger("SignalRCoreClient").setLevel(logging.DEBUG) # If using signalrcore

        if not stream.start():
            logger.error("Failed to start DataStream. Exiting.")
            sys.exit(1)

        logger.info("Stream processing started. Press Ctrl+C to stop.")
        while True:
            time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down gracefully...")
    except ConfigurationError as e:
        logger.error("CONFIGURATION ERROR: %s", e)
        logger.error("Please ensure API_KEY and USERNAME are correctly set "
                     "in your .env file.")
    except AuthenticationError as e:
        logger.error("AUTHENTICATION FAILED: %s", e)
    except APIError as e:
        logger.error("API ERROR: %s", e)
    except LibraryError as e:
        logger.error("LIBRARY ERROR: %s", e)
    except ValueError as e:
        logger.error("VALUE ERROR: %s", e)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.critical("UNHANDLED CRITICAL ERROR in Market Data Tester: %s",
                        e_gen, exc_info=True)
    finally:
        if stream:
            logger.info("Stopping data stream...")
            stream.stop()
        logger.info("Market Data Tester terminated.")

if __name__ == "__main__":
    main()