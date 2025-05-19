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
from tsxapipy.real_time import UserHubStream
from tsxapipy.config import ACCOUNT_ID_TO_WATCH as DEFAULT_ACCOUNT_ID_TO_WATCH
from tsxapipy.api.exceptions import ConfigurationError, LibraryError

logger = logging.getLogger(__name__)

# pylint: disable=global-statement
current_account_id_watching: Optional[int] = None

def handle_user_order_update_cli(order_data_payload: Any):
    """Callback for user order updates."""
    acc_id = order_data_payload.get('accountId', current_account_id_watching or 'N/A')
    logger.info("--- CLI: User Order Update for Account %s ---", acc_id)
    logger.info(pprint.pformat(order_data_payload, indent=2, width=120))

def handle_user_account_update_cli(account_data: Any):
    """Callback for user account updates."""
    acc_id = account_data.get('id', current_account_id_watching or 'N/A')
    logger.info("--- CLI: User Account Update for Account %s ---", acc_id)
    logger.info(pprint.pformat(account_data, indent=2, width=120))

def handle_user_position_update_cli(position_data: Any):
    """Callback for user position updates."""
    acc_id = position_data.get('accountId', current_account_id_watching or 'N/A')
    logger.info("--- CLI: User Position Update for Account %s ---", acc_id)
    logger.info(pprint.pformat(position_data, indent=2, width=120))

def handle_user_trade_update_cli(user_trade_data: Any):
    """Callback for user trade execution updates."""
    acc_id = user_trade_data.get('accountId', current_account_id_watching or 'N/A')
    logger.info("--- CLI: User Trade Execution for Account %s ---", acc_id)
    logger.info(pprint.pformat(user_trade_data, indent=2, width=120))

def handle_cli_user_stream_state_change(state: str):
    """Callback for UserHubStream state changes."""
    logger.info("CLI UserHubStream state changed to: '%s' for account %s",
                state, current_account_id_watching or 'N/A')

def handle_cli_user_stream_error(error: Any):
    """Callback for UserHubStream errors."""
    logger.error("CLI UserHubStream encountered an error for account %s: %s",
                 current_account_id_watching or 'N/A', error)

def main(): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    """Main function to parse arguments and run the order watcher."""
    parser = argparse.ArgumentParser(
        description="Watch real-time user hub events for a specific account.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--account_id", type=int, default=DEFAULT_ACCOUNT_ID_TO_WATCH,
        help="Account ID to watch. Uses ACCOUNT_ID_TO_WATCH from .env or default."
    )
    parser.add_argument("--no_orders", action="store_false", dest="sub_orders",
                        default=True, help="Disable order updates subscription.")
    parser.add_argument("--no_account", action="store_false", dest="sub_account",
                        default=True, help="Disable account updates subscription.")
    parser.add_argument("--no_positions", action="store_false", dest="sub_positions",
                        default=True, help="Disable position updates subscription.")
    parser.add_argument("--no_user_trades", action="store_false",
                        dest="sub_user_trades", default=True,
                        help="Disable user trade execution updates subscription.")
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

    global current_account_id_watching
    # Convert account_id from args (which might be None if default from env is empty)
    account_id_from_args_str = args.account_id
    parsed_account_id: Optional[int] = None

    if isinstance(account_id_from_args_str, str) and account_id_from_args_str.strip():
        try:
            parsed_account_id = int(account_id_from_args_str)
        except ValueError:
            logger.error("Provided --account_id '%s' is not a valid integer.",
                         account_id_from_args_str)
            sys.exit(1)
    elif isinstance(account_id_from_args_str, int): # If it was already int (e.g. default was int)
        parsed_account_id = account_id_from_args_str


    current_account_id_watching = parsed_account_id

    if not current_account_id_watching or current_account_id_watching <= 0:
        if args.sub_orders or args.sub_positions or args.sub_user_trades:
            logger.critical("A valid positive Account ID is required for Orders, "
                            "Positions, or UserTrades subscriptions. Provide --account_id "
                            "or set ACCOUNT_ID_TO_WATCH in your .env file.")
            sys.exit(1)
        elif not args.sub_account:
            logger.warning("No subscriptions enabled and no valid account ID. "
                           "Stream will not show data.")
        else:
            logger.info("No specific positive Account ID; "
                        "only global account subscriptions will be active if enabled.")

    display_account_id = current_account_id_watching \
        if current_account_id_watching and current_account_id_watching > 0 \
        else 'Global Accounts'
    logger.info("Starting User Hub Watcher for ACCOUNT_ID: %s", display_account_id)
    logger.info("Attempting Subscriptions: Orders=%s, Account(Global)=%s, "
                "Positions=%s, UserTrades=%s", args.sub_orders, args.sub_account,
                args.sub_positions, args.sub_user_trades)

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
        # Ensure account_id_to_watch passed to UserHubStream is an int, or handle appropriately
        # The UserHubStream class itself validates if account_id_to_watch is int.
        # For this CLI, if only global sub is active, we might pass a non-positive ID or handle it.
        # However, current UserHubStream logic expects an int.
        watch_id_for_stream = current_account_id_watching if current_account_id_watching else 0

        stream = UserHubStream(
            api_client=api_client_instance,
            account_id_to_watch=watch_id_for_stream,
            on_order_update=handle_user_order_update_cli if args.sub_orders else None,
            on_account_update=handle_user_account_update_cli if args.sub_account else None,
            on_position_update=handle_user_position_update_cli if args.sub_positions else None,
            on_user_trade_update=handle_user_trade_update_cli if args.sub_user_trades else None,
            subscribe_to_accounts_globally=args.sub_account,
            on_state_change_callback=handle_cli_user_stream_state_change,
            on_error_callback=handle_cli_user_stream_error
        )
        logger.info("UserHubStream initialized.")

        if not stream.start():
            logger.error("Failed to start UserHubStream. Exiting.")
            sys.exit(1)

        logger.info("User Hub stream processing started... Press Ctrl+C to stop.")
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
    except ValueError as e: # Catches int conversion errors or other value issues
        logger.error("VALUE ERROR: %s", e)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.critical("UNHANDLED CRITICAL ERROR in User Hub Watcher: %s",
                        e_gen, exc_info=True)
    finally:
        if stream:
            logger.info("Stopping User Hub stream...")
            stream.stop()
        logger.info("User Hub Watcher terminated.")

if __name__ == "__main__":
    main()