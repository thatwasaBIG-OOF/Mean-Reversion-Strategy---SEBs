# examples/09_concurrent_streams_monitor.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

import logging
import time
import pprint
import argparse
from typing import Any, Optional

from tsxapipy import (
    APIClient,
    DataStream,
    UserHubStream,
    authenticate,
    AuthenticationError,
    ConfigurationError,
    APIError,
    DEFAULT_CONFIG_CONTRACT_ID,
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH,
    TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES
)

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ConcurrentStreamsExample")

# --- Globals for Callbacks (optional, for simplicity in example) ---
current_contract_monitoring: Optional[str] = None
current_account_monitoring: Optional[int] = None

# --- DataStream Callbacks ---
def handle_market_quote(quote_data: Any):
    logger.info(f"--- MARKET QUOTE ({current_contract_monitoring}) ---")
    logger.info(pprint.pformat(quote_data, indent=2, width=120))

def handle_market_trade(trade_data: Any):
    logger.info(f"--- MARKET TRADE ({current_contract_monitoring}) ---")
    logger.info(pprint.pformat(trade_data, indent=2, width=120))

def handle_market_stream_state(state: str):
    logger.info(f"MarketDataStream state for '{current_contract_monitoring}': {state}")

def handle_market_stream_error(error: Any):
    logger.error(f"MarketDataStream error for '{current_contract_monitoring}': {error}")

# --- UserHubStream Callbacks ---
def handle_user_order(order_data: Any):
    logger.info(f"--- USER ORDER (Account: {order_data.get('accountId', current_account_monitoring)}) ---")
    logger.info(pprint.pformat(order_data, indent=2, width=120))

def handle_user_account(account_data: Any):
    logger.info(f"--- USER ACCOUNT (ID: {account_data.get('id', current_account_monitoring)}) ---")
    logger.info(pprint.pformat(account_data, indent=2, width=120))

def handle_user_stream_state(state: str):
    logger.info(f"UserHubStream state for account '{current_account_monitoring}': {state}")

def handle_user_stream_error(error: Any):
    logger.error(f"UserHubStream error for account '{current_account_monitoring}': {error}")


def run_concurrent_streams_example(contract_id: str, account_id: int, duration_seconds: int, token_refresh_interval_seconds: int):
    global current_contract_monitoring, current_account_monitoring
    current_contract_monitoring = contract_id
    current_account_monitoring = account_id

    logger.info(f"--- Example: Concurrent Market and User Streams ---")
    logger.info(f"Monitoring Contract: {contract_id}, Account: {account_id}")
    logger.info(f"Running for {duration_seconds} seconds. Token refresh check every {token_refresh_interval_seconds}s.")

    api_client: Optional[APIClient] = None
    data_stream: Optional[DataStream] = None
    user_stream: Optional[UserHubStream] = None

    try:
        logger.info("Authenticating...")
        initial_token, token_acquired_at = authenticate()
        
        api_client = APIClient(
            initial_token=initial_token,
            token_acquired_at=token_acquired_at
        )
        logger.info("APIClient initialized.")

        # Setup DataStream
        logger.info(f"Initializing DataStream for contract: {contract_id}")
        data_stream = DataStream(
            api_client=api_client,
            contract_id_to_subscribe=contract_id,
            on_quote_callback=handle_market_quote,
            on_trade_callback=handle_market_trade,
            on_state_change_callback=handle_market_stream_state,
            on_error_callback=handle_market_stream_error
        )
        if not data_stream.start():
            logger.error("Failed to start DataStream. Exiting.")
            return
        logger.info("DataStream started.")

        # Setup UserHubStream
        if account_id > 0:
            logger.info(f"Initializing UserHubStream for account: {account_id}")
            user_stream = UserHubStream(
                api_client=api_client,
                account_id_to_watch=account_id,
                on_order_update=handle_user_order,
                on_account_update=handle_user_account,
                # Add other user hub callbacks (position, trade) if desired
                subscribe_to_accounts_globally=True, # Can be False if only interested in specific account_id
                on_state_change_callback=handle_user_stream_state,
                on_error_callback=handle_user_stream_error
            )
            if not user_stream.start():
                logger.error("Failed to start UserHubStream. Exiting.")
                # Optionally stop data_stream here if user_stream fails
                if data_stream: data_stream.stop()
                return
            logger.info("UserHubStream started.")
        else:
            logger.warning("Account ID is not valid (<=0). UserHubStream will not be started.")


        logger.info("Both streams (if configured) are running. Monitoring...")
        
        end_time = time.monotonic() + duration_seconds
        last_token_refresh_check = time.monotonic()

        while time.monotonic() < end_time:
            # Periodically ensure streams have the latest token from APIClient
            if time.monotonic() - last_token_refresh_check >= token_refresh_interval_seconds:
                logger.debug("Performing periodic token refresh check for streams...")
                if api_client:
                    try:
                        # This call ensures APIClient re-validates/re-authenticates if its token is old
                        latest_token = api_client.current_token 
                        
                        if data_stream and data_stream.connection_status != "disconnected":
                            logger.debug(f"Updating token for DataStream ({data_stream.connection_status})")
                            data_stream.update_token(latest_token)
                        
                        if user_stream and user_stream.connection_status != "disconnected":
                            logger.debug(f"Updating token for UserHubStream ({user_stream.connection_status})")
                            user_stream.update_token(latest_token)
                            
                    except AuthenticationError as auth_err:
                        logger.error(f"Failed to get/refresh token from APIClient for stream updates: {auth_err}")
                        # Streams might fail to reconnect if their old token expires
                    except Exception as e_token_update:
                        logger.error(f"Unexpected error during stream token update logic: {e_token_update}", exc_info=True)
                last_token_refresh_check = time.monotonic()
            
            time.sleep(1)  # Main loop polling interval
        
        logger.info("Example duration finished.")

    except ConfigurationError as e:
        logger.error(f"CONFIGURATION ERROR: {e}")
    except AuthenticationError as e:
        logger.error(f"AUTHENTICATION FAILED: {e}")
    except APIError as e:
        logger.error(f"API ERROR: {e}")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down streams...")
    except Exception as e:
        logger.error(f"AN UNEXPECTED ERROR OCCURRED: {e}", exc_info=True)
    finally:
        if data_stream:
            logger.info("Stopping DataStream...")
            data_stream.stop()
        if user_stream:
            logger.info("Stopping UserHubStream...")
            user_stream.stop()
        logger.info("Concurrent streams example finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run concurrent Market Data and User Hub streams.")
    parser.add_argument(
        "--contract_id", 
        type=str, 
        default=DEFAULT_CONFIG_CONTRACT_ID,
        help="Contract ID to subscribe to for market data."
    )
    parser.add_argument(
        "--account_id", 
        type=int, 
        default=DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH,
        help="Account ID to watch for user-specific data. Set to 0 or less to disable UserHubStream."
    )
    parser.add_argument(
        "--duration", 
        type=int, 
        default=120, 
        help="How long (in seconds) to run the streams."
    )
    parser.add_argument(
        "--token_refresh_check", 
        type=int, 
        default=(TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES * 60) // 3, # e.g., every 10 mins if safety is 30 mins
        help="Interval (in seconds) to check and propagate APIClient token to streams."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG level logging for all modules."
    )
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG) # Set root logger
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.DEBUG)
        logger.info("DEBUG logging enabled.")
        # For very verbose SignalR client logs:
        # logging.getLogger("SignalRCoreClient").setLevel(logging.DEBUG)


    if not args.contract_id:
        logger.error("A contract_id must be provided via --contract_id or in .env as CONTRACT_ID.")
        sys.exit(1)
    # account_id can be non-positive to skip UserHubStream

    run_concurrent_streams_example(
        contract_id=args.contract_id,
        account_id=args.account_id,
        duration_seconds=args.duration,
        token_refresh_interval_seconds=args.token_refresh_check
    )