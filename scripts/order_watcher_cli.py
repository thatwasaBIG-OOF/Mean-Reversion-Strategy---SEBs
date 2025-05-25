"""
CLI script to watch real-time user hub events (orders, account, positions, trades)
for a specific account using UserHubStream.
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
from tsxapipy.real_time import UserHubStream, StreamConnectionState # Added StreamConnectionState
from tsxapipy.config import ACCOUNT_ID_TO_WATCH as DEFAULT_ACCOUNT_ID_TO_WATCH
from tsxapipy.api.exceptions import ConfigurationError, LibraryError, APIResponseParsingError # Added

# Configure logging for the CLI script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True 
)
logger = logging.getLogger(__name__) # Use __name__ for script-specific logger

# pylint: disable=global-statement
current_account_id_watching: Optional[int] = None

def handle_user_order_update_cli(order_data_payload: Any):
    """Callback for user order updates."""
    acc_id = order_data_payload.get('accountId', current_account_id_watching or 'N/A')
    logger.info("--- CLI: User Order Update for Account %s ---", acc_id)
    logger.info(pprint.pformat(order_data_payload, indent=1, width=100, compact=True))

def handle_user_account_update_cli(account_data: Any):
    """Callback for user account updates."""
    acc_id = account_data.get('id', current_account_id_watching or 'N/A')
    logger.info("--- CLI: User Account Update for Account %s ---", acc_id)
    logger.info(pprint.pformat(account_data, indent=1, width=100, compact=True))

def handle_user_position_update_cli(position_data: Any):
    """Callback for user position updates."""
    acc_id = position_data.get('accountId', current_account_id_watching or 'N/A')
    logger.info("--- CLI: User Position Update for Account %s ---", acc_id)
    logger.info(pprint.pformat(position_data, indent=1, width=100, compact=True))

def handle_user_trade_update_cli(user_trade_data: Any):
    """Callback for user trade execution updates."""
    acc_id = user_trade_data.get('accountId', current_account_id_watching or 'N/A')
    logger.info("--- CLI: User Trade Execution for Account %s ---", acc_id)
    logger.info(pprint.pformat(user_trade_data, indent=1, width=100, compact=True))

def handle_cli_user_stream_state_change(state_str: str): # Receives state name as string
    """Callback for UserHubStream state changes."""
    logger.info("CLI UserHubStream state changed to: '%s' for account %s",
                state_str, current_account_id_watching or 'Global')

def handle_cli_user_stream_error(error: Any):
    """Callback for UserHubStream errors."""
    logger.error("CLI UserHubStream encountered an error for account %s: %s",
                 current_account_id_watching or 'Global', error)

def main(): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    """Main function to parse arguments and run the order watcher."""
    parser = argparse.ArgumentParser(
        description="Watch real-time user hub events for a specific account.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--account_id", 
        type=int, 
        # Handle if DEFAULT_ACCOUNT_ID_TO_WATCH is None (e.g. from empty .env var)
        default=int(DEFAULT_ACCOUNT_ID_TO_WATCH) if DEFAULT_ACCOUNT_ID_TO_WATCH is not None else 0,
        help="Account ID to watch. Uses ACCOUNT_ID_TO_WATCH from .env. Set to 0 or less for global account subscriptions only."
    )
    parser.add_argument("--no_orders", action="store_false", dest="sub_orders",
                        default=True, help="Disable order updates subscription.")
    parser.add_argument("--no_account", action="store_false", dest="sub_account",
                        default=True, help="Disable global account updates subscription.")
    parser.add_argument("--no_positions", action="store_false", dest="sub_positions",
                        default=True, help="Disable position updates subscription.")
    parser.add_argument("--no_user_trades", action="store_false",
                        dest="sub_user_trades", default=True,
                        help="Disable user trade execution updates subscription.")
    parser.add_argument("--debug", action="store_true",
                        help="Enable DEBUG level logging for all modules.")
    args = parser.parse_args()

    # Reconfigure logging level if --debug is passed
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("DEBUG logging enabled by CLI flag for all modules.")

    global current_account_id_watching
    current_account_id_watching = args.account_id if args.account_id > 0 else None


    if not current_account_id_watching and (args.sub_orders or args.sub_positions or args.sub_user_trades):
        logger.critical("A valid positive Account ID (--account_id or ACCOUNT_ID_TO_WATCH in .env) "
                        "is required for Orders, Positions, or UserTrades subscriptions.")
        sys.exit(1)
    
    if not current_account_id_watching and not args.sub_account: # No specific account and no global account sub
        logger.warning("No subscriptions enabled (neither specific account events nor global account updates). "
                       "Stream will connect but likely show no user data.")
    
    display_account_id_log = current_account_id_watching if current_account_id_watching else "Global (if enabled)"
    logger.info("Starting User Hub Watcher for ACCOUNT_ID: %s", display_account_id_log)
    logger.info("Attempting Subscriptions: Orders=%s, Account(Global)=%s, "
                "Positions=%s, UserTrades=%s", 
                args.sub_orders and current_account_id_watching is not None, # Only if account_id valid
                args.sub_account,
                args.sub_positions and current_account_id_watching is not None, # Only if account_id valid
                args.sub_user_trades and current_account_id_watching is not None  # Only if account_id valid
                )

    api_client_instance: Optional[APIClient] = None
    stream: Optional[UserHubStream] = None

    try:
        logger.info("Authenticating with API...")
        initial_token, token_acquired_time = authenticate()

        api_client_instance = APIClient(
            initial_token=initial_token,
            token_acquired_at=token_acquired_time
        )
        logger.info("APIClient initialized successfully.")

        logger.info("Initializing UserHubStream...")
        # Pass 0 if current_account_id_watching is None, as UserHubStream expects int
        watch_id_for_stream_init = current_account_id_watching if current_account_id_watching else 0

        stream = UserHubStream(
            api_client=api_client_instance,
            account_id_to_watch=watch_id_for_stream_init,
            on_order_update=handle_user_order_update_cli if args.sub_orders and current_account_id_watching else None,
            on_account_update=handle_user_account_update_cli if args.sub_account else None,
            on_position_update=handle_user_position_update_cli if args.sub_positions and current_account_id_watching else None,
            on_user_trade_update=handle_user_trade_update_cli if args.sub_user_trades and current_account_id_watching else None,
            subscribe_to_accounts_globally=args.sub_account,
            on_state_change_callback=handle_cli_user_stream_state_change,
            on_error_callback=handle_cli_user_stream_error
        )
        logger.info("UserHubStream initialized.")

        if not stream.start():
            status_name = stream.connection_status.name if stream else "N/A (stream object None)"
            logger.error(f"Failed to start UserHubStream (current status: {status_name}). Exiting.")
            sys.exit(1)

        logger.info("User Hub stream processing started... Press Ctrl+C to stop.")
        while True:
            if stream and stream.connection_status == StreamConnectionState.ERROR:
                logger.error("UserHubStream is in ERROR state. Terminating monitor.")
                break
            if stream and stream.connection_status == StreamConnectionState.DISCONNECTED:
                logger.warning("UserHubStream is DISCONNECTED (and not auto-reconnecting if retries exhausted). Terminating monitor.")
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
    except APIError as e_api:
        logger.error("API ERROR: %s", e_api)
    except LibraryError as e_lib:
        logger.error("LIBRARY ERROR: %s", e_lib)
    except ValueError as e_val: 
        logger.error("VALUE ERROR: %s", e_val)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.critical("UNHANDLED CRITICAL ERROR in User Hub Watcher: %s",
                        e_gen, exc_info=True)
    finally:
        if stream:
            status_name = stream.connection_status.name if stream.connection_status else "N/A"
            logger.info(f"Stopping User Hub stream (current status: {status_name})...")
            stream.stop(reason_for_stop="CLI script finishing")
        logger.info("User Hub Watcher terminated.")

if __name__ == "__main__":
    main()