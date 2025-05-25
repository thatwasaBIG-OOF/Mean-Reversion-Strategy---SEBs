"""
Example 09: Concurrent Market Data and User Hub Streams Monitor.

This script demonstrates:
1. Authenticating and initializing APIClient.
2. Initializing and starting both DataStream (for market data) and
   UserHubStream (for user-specific data like orders, account details).
3. Periodically checking and refreshing stream tokens.
4. Handling stream state changes and errors for both streams.
"""
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
    LibraryError, # Added
    StreamConnectionState, # Added
    DEFAULT_CONFIG_CONTRACT_ID,
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH,
    TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES
)
from tsxapipy.api.exceptions import APIResponseParsingError # Added

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
    # Using pprint for potentially large dicts, adjust as needed
    logger.info(pprint.pformat(quote_data, indent=1, width=100, compact=True))


def handle_market_trade(trade_data: Any):
    logger.info(f"--- MARKET TRADE ({current_contract_monitoring}) ---")
    logger.info(pprint.pformat(trade_data, indent=1, width=100, compact=True))

def handle_market_stream_state(state_str: str): # Receives state name as string
    logger.info(f"MarketDataStream state for '{current_contract_monitoring}': {state_str}")

def handle_market_stream_error(error: Any):
    logger.error(f"MarketDataStream error for '{current_contract_monitoring}': {error}")

# --- UserHubStream Callbacks ---
def handle_user_order(order_data: Any):
    logger.info(f"--- USER ORDER (Account: {order_data.get('accountId', current_account_monitoring or 'N/A')}) ---")
    logger.info(pprint.pformat(order_data, indent=1, width=100, compact=True))

def handle_user_account(account_data: Any):
    logger.info(f"--- USER ACCOUNT (ID: {account_data.get('id', current_account_monitoring or 'N/A')}) ---")
    logger.info(pprint.pformat(account_data, indent=1, width=100, compact=True))

def handle_user_stream_state(state_str: str): # Receives state name as string
    logger.info(f"UserHubStream state for account '{current_account_monitoring or 'Global'}': {state_str}")

def handle_user_stream_error(error: Any):
    logger.error(f"UserHubStream error for account '{current_account_monitoring or 'Global'}': {error}")


def run_concurrent_streams_example(contract_id: str, account_id: Optional[int], duration_seconds: int, token_refresh_interval_seconds: int):
    global current_contract_monitoring, current_account_monitoring
    current_contract_monitoring = contract_id
    current_account_monitoring = account_id # Can be None if account_id is not positive

    account_id_for_log = account_id if account_id and account_id > 0 else "Global (if enabled)"
    logger.info(f"--- Example: Concurrent Market and User Streams ---")
    logger.info(f"Monitoring Contract: {contract_id}, Account: {account_id_for_log}")
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
            logger.error(f"Failed to start DataStream (status: {data_stream.connection_status.name}). Exiting.")
            return
        logger.info("DataStream start initiated.")

        # Setup UserHubStream
        if account_id and account_id > 0: # Ensure account_id is positive for user stream
            logger.info(f"Initializing UserHubStream for account: {account_id}")
            user_stream = UserHubStream(
                api_client=api_client,
                account_id_to_watch=account_id,
                on_order_update=handle_user_order,
                on_account_update=handle_user_account,
                subscribe_to_accounts_globally=True, 
                on_state_change_callback=handle_user_stream_state,
                on_error_callback=handle_user_stream_error
            )
            if not user_stream.start():
                logger.error(f"Failed to start UserHubStream (status: {user_stream.connection_status.name}). Exiting.")
                if data_stream: data_stream.stop(reason_for_stop="UserHubStream failed to start")
                return
            logger.info("UserHubStream start initiated.")
        else:
            logger.warning(f"Account ID ({account_id}) is not valid for UserHubStream. UserHubStream will not be started.")


        logger.info("Streams (if configured) are starting/running. Monitoring...")
        
        end_time = time.monotonic() + duration_seconds
        last_token_refresh_check = time.monotonic()

        while time.monotonic() < end_time:
            if time.monotonic() - last_token_refresh_check >= token_refresh_interval_seconds:
                logger.debug("Performing periodic token refresh check for streams...")
                if api_client:
                    try:
                        latest_token = api_client.current_token 
                        
                        if data_stream and data_stream.connection_status not in [
                            StreamConnectionState.DISCONNECTED, 
                            StreamConnectionState.ERROR, 
                            StreamConnectionState.STOPPING
                        ]:
                            logger.debug(f"Updating token for DataStream (current state: {data_stream.connection_status.name})")
                            data_stream.update_token(latest_token)
                        
                        if user_stream and user_stream.connection_status not in [
                            StreamConnectionState.DISCONNECTED, 
                            StreamConnectionState.ERROR,
                            StreamConnectionState.STOPPING
                        ]:
                            logger.debug(f"Updating token for UserHubStream (current state: {user_stream.connection_status.name})")
                            user_stream.update_token(latest_token)
                            
                    except AuthenticationError as auth_err:
                        logger.error(f"Failed to get/refresh token from APIClient for stream updates: {auth_err}")
                    except Exception as e_token_update:
                        logger.error(f"Unexpected error during stream token update logic: {e_token_update}", exc_info=True)
                last_token_refresh_check = time.monotonic()
            
            time.sleep(1)  
        
        logger.info("Example duration finished.")

    except ConfigurationError as e_conf:
        logger.error(f"CONFIGURATION ERROR: {e_conf}")
    except AuthenticationError as e_auth:
        logger.error(f"AUTHENTICATION FAILED: {e_auth}")
    except APIResponseParsingError as e_parse: 
        logger.error(f"API RESPONSE PARSING ERROR (likely during auth or token refresh): {e_parse}")
        if e_parse.raw_response_text:
            logger.error("Raw problematic response text (preview): %s", e_parse.raw_response_text[:500])
    except APIError as e_api:
        logger.error(f"API ERROR: {e_api}")
    except LibraryError as e_lib:
        logger.error(f"LIBRARY ERROR: {e_lib}")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down streams...")
    except Exception as e_generic:
        logger.error(f"AN UNEXPECTED ERROR OCCURRED: {e_generic}", exc_info=True)
    finally:
        if data_stream:
            logger.info(f"Stopping DataStream (current status: {data_stream.connection_status.name})...")
            data_stream.stop(reason_for_stop="Example script finishing")
        if user_stream:
            logger.info(f"Stopping UserHubStream (current status: {user_stream.connection_status.name})...")
            user_stream.stop(reason_for_stop="Example script finishing")
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
        # Ensure default from config is treated as Optional[int] or handle None from getenv
        default=int(DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH) if DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH else 0,
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
        default=max(60, (TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES * 60) // 3), # Ensure at least 60s
        help="Interval (in seconds) to check and propagate APIClient token to streams."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG level logging for all modules."
    )
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG) 
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.DEBUG)
        logger.info("DEBUG logging enabled.")

    if not args.contract_id:
        logger.error("A contract_id must be provided via --contract_id or in .env as CONTRACT_ID.")
        sys.exit(1)
    
    # account_id can be 0 or negative from arg parsing to skip UserHubStream
    account_id_to_run_with = args.account_id if args.account_id > 0 else None


    run_concurrent_streams_example(
        contract_id=args.contract_id,
        account_id=account_id_to_run_with, # Pass Optional[int]
        duration_seconds=args.duration,
        token_refresh_interval_seconds=args.token_refresh_check
    )