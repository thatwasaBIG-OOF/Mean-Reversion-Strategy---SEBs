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
# Ensure the 'src' directory (containing 'tsxapipy') is on the path
# This assumes the example script is in 'examples/' and 'src/' is a sibling to 'examples/'
SRC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)
    print(f"Added to sys.path for example: {SRC_PATH}")


import logging
import time
import pprint
import argparse
from typing import Any, Optional, Dict # Ensure Dict is imported

from tsxapipy import (
    APIClient,
    DataStream,
    UserHubStream,
    authenticate,
    AuthenticationError,
    ConfigurationError,
    APIError,
    LibraryError, 
    StreamConnectionState, 
    DEFAULT_CONFIG_CONTRACT_ID,
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH, # This is the string version from config export
    TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES
)
from tsxapipy.api.exceptions import APIResponseParsingError 

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ConcurrentStreamsExample")

# --- Globals for Callbacks (optional, for simplicity in example) ---
current_contract_monitoring: Optional[str] = None
current_account_monitoring: Optional[int] = None # Will store the integer account ID

# --- DataStream Callbacks ---
def handle_market_quote(quote_data: Dict[str, Any]): # Type hint for quote_data
    logger.info(f"--- MARKET QUOTE ({current_contract_monitoring or 'N/A'}) ---")
    logger.info(pprint.pformat(quote_data, indent=1, width=100, compact=True))

def handle_market_trade(trade_data: Dict[str, Any]): # Type hint for trade_data
    logger.info(f"--- MARKET TRADE ({current_contract_monitoring or 'N/A'}) ---")
    logger.info(pprint.pformat(trade_data, indent=1, width=100, compact=True))

def handle_market_depth(depth_data: Dict[str, Any]): # Added depth callback
    logger.info(f"--- MARKET DEPTH ({current_contract_monitoring or 'N/A'}) ---")
    logger.info(pprint.pformat(depth_data, indent=1, width=100, compact=True))

def handle_market_stream_state(state_str: str): 
    logger.info(f"MarketDataStream state for '{current_contract_monitoring or 'N/A'}': {state_str}")

def handle_market_stream_error(error: Any):
    logger.error(f"MarketDataStream error for '{current_contract_monitoring or 'N/A'}': {error}")

# --- UserHubStream Callbacks ---
def handle_user_order(order_data: Dict[str, Any]): # Type hint
    logger.info(f"--- USER ORDER (Account: {order_data.get('accountId', current_account_monitoring or 'N/A')}) ---")
    logger.info(pprint.pformat(order_data, indent=1, width=100, compact=True))

def handle_user_account(account_data: Dict[str, Any]): # Type hint
    logger.info(f"--- USER ACCOUNT (ID: {account_data.get('id', current_account_monitoring or 'N/A')}) ---")
    logger.info(pprint.pformat(account_data, indent=1, width=100, compact=True))

def handle_user_position_update(position_data: Dict[str, Any]): # Type hint
    logger.info(f"--- USER POSITION UPDATE (Account: {position_data.get('accountId', current_account_monitoring or 'N/A')}) ---")
    logger.info(pprint.pformat(position_data, indent=1, width=100, compact=True))

def handle_user_trade_execution(trade_data: Dict[str, Any]): # Type hint
    logger.info(f"--- USER TRADE EXECUTION (Account: {trade_data.get('accountId', current_account_monitoring or 'N/A')}) ---")
    logger.info(pprint.pformat(trade_data, indent=1, width=100, compact=True))

def handle_user_stream_state(state_str: str): 
    logger.info(f"UserHubStream state for account '{current_account_monitoring or 'Global/N/A'}': {state_str}")

def handle_user_stream_error(error: Any):
    logger.error(f"UserHubStream error for account '{current_account_monitoring or 'Global/N/A'}': {error}")


def run_concurrent_streams_example(
    contract_id_arg: str, 
    account_id_arg: Optional[int], # This is now Optional[int]
    duration_seconds: int, 
    token_refresh_interval_seconds: int
):
    global current_contract_monitoring, current_account_monitoring
    current_contract_monitoring = contract_id_arg
    current_account_monitoring = account_id_arg # Store the int or None

    account_id_for_log = str(account_id_arg) if account_id_arg and account_id_arg > 0 else "Not specified or global only"
    logger.info(f"--- Example: Concurrent Market and User Streams ---")
    logger.info(f"Monitoring Contract: {contract_id_arg}, Account: {account_id_for_log}")
    logger.info(f"Running for {duration_seconds} seconds. Token refresh check every {token_refresh_interval_seconds}s.")

    api_client: Optional[APIClient] = None
    data_stream: Optional[DataStream] = None
    user_stream: Optional[UserHubStream] = None

    try:
        logger.info("Authenticating...")
        initial_token, token_acquired_at = authenticate() # Uses config USERNAME/API_KEY
        if not initial_token or not token_acquired_at:
            logger.error("Authentication failed. Cannot proceed.")
            return
        
        api_client = APIClient(
            initial_token=initial_token,
            token_acquired_at=token_acquired_at
            # APIClient __init__ should handle reauth_username/api_key from config if not passed
        )
        logger.info("APIClient initialized.")

        # Setup DataStream
        logger.info(f"Initializing DataStream for contract: {contract_id_arg}")
        data_stream = DataStream(
            api_client=api_client,
            contract_id_to_subscribe=contract_id_arg, # Corrected to match DataStream __init__
            on_quote_callback=handle_market_quote,
            on_trade_callback=handle_market_trade,
            on_depth_callback=handle_market_depth, # Added depth callback
            on_state_change_callback=handle_market_stream_state,
            on_error_callback=handle_market_stream_error,
            auto_subscribe_quotes=True, # Example: enable quotes
            auto_subscribe_trades=True, # Example: enable trades
            auto_subscribe_depth=False  # Example: disable depth initially
        )
        if not data_stream.start():
            ds_status_name = data_stream.connection_status.name if hasattr(data_stream, 'connection_status') and data_stream.connection_status else "N/A"
            logger.error(f"Failed to start DataStream (status: {ds_status_name}). Exiting.")
            return
        logger.info("DataStream start initiated.")

        # Setup UserHubStream only if a valid positive account_id_arg is provided
        if account_id_arg and account_id_arg > 0:
            logger.info(f"Initializing UserHubStream for account: {account_id_arg}")
            user_stream = UserHubStream(
                api_client=api_client,
                account_id_to_watch=account_id_arg, # Matches UserHubStream __init__
                on_order_update=handle_user_order,
                on_account_update=handle_user_account,
                on_position_update=handle_user_position_update,
                on_user_trade_update=handle_user_trade_execution,
                subscribe_to_accounts_globally=True, 
                on_state_change_callback=handle_user_stream_state,
                on_error_callback=handle_user_stream_error
            )
            if not user_stream.start():
                uhs_status_name = user_stream.connection_status.name if hasattr(user_stream, 'connection_status') and user_stream.connection_status else "N/A"
                logger.error(f"Failed to start UserHubStream (status: {uhs_status_name}). Exiting.")
                if data_stream and hasattr(data_stream, 'stop'): data_stream.stop(reason_for_stop="UserHubStream failed to start")
                return
            logger.info("UserHubStream start initiated.")
        else:
            logger.warning(f"Account ID ({account_id_arg}) is not valid or not provided for UserHubStream. UserHubStream will not be started.")


        logger.info("Streams (if configured) are starting/running. Monitoring...")
        
        end_time = time.monotonic() + duration_seconds
        last_token_refresh_check = time.monotonic()

        while time.monotonic() < end_time:
            current_monotonic_time = time.monotonic()
            if current_monotonic_time - last_token_refresh_check >= token_refresh_interval_seconds:
                logger.debug("Performing periodic token refresh check for streams...")
                if api_client: # Ensure api_client is not None
                    try:
                        latest_token = api_client.current_token # This will trigger re-auth within APIClient if needed
                        
                        if data_stream and hasattr(data_stream, 'update_token') and \
                           hasattr(data_stream, 'connection_status') and data_stream.connection_status and \
                           data_stream.connection_status not in [
                            StreamConnectionState.DISCONNECTED, 
                            StreamConnectionState.ERROR, 
                            StreamConnectionState.STOPPING]:
                            logger.debug(f"Updating token for DataStream (current state: {data_stream.connection_status.name})")
                            data_stream.update_token(latest_token)
                        
                        if user_stream and hasattr(user_stream, 'update_token') and \
                           hasattr(user_stream, 'connection_status') and user_stream.connection_status and \
                           user_stream.connection_status not in [
                            StreamConnectionState.DISCONNECTED, 
                            StreamConnectionState.ERROR,
                            StreamConnectionState.STOPPING]:
                            logger.debug(f"Updating token for UserHubStream (current state: {user_stream.connection_status.name})")
                            user_stream.update_token(latest_token)
                            
                    except AuthenticationError as auth_err:
                        logger.error(f"Failed to get/refresh token from APIClient for stream updates: {auth_err}")
                    except Exception as e_token_update:
                        logger.error(f"Unexpected error during stream token update logic: {e_token_update}", exc_info=True)
                last_token_refresh_check = current_monotonic_time
            
            time.sleep(1)  # Main loop polling interval
        
        logger.info("Example duration finished.")

    except ConfigurationError as e_conf:
        logger.error(f"CONFIGURATION ERROR: {e_conf}")
    except AuthenticationError as e_auth:
        logger.error(f"AUTHENTICATION FAILED: {e_auth}")
    except APIResponseParsingError as e_parse: 
        logger.error(f"API RESPONSE PARSING ERROR (likely during auth or token refresh): {e_parse}")
        if hasattr(e_parse, 'raw_response_text') and e_parse.raw_response_text:
            logger.error(f"Raw problematic response text (preview): {e_parse.raw_response_text[:500]}")
    except APIError as e_api:
        logger.error(f"API ERROR: {e_api}")
    except LibraryError as e_lib:
        logger.error(f"LIBRARY ERROR: {e_lib}")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down streams...")
    except Exception as e_generic: # Catch any other unexpected error
        logger.error(f"AN UNEXPECTED ERROR OCCURRED: {e_generic}", exc_info=True)
    finally:
        if data_stream:
            ds_final_status = data_stream.connection_status.name if hasattr(data_stream, 'connection_status') and data_stream.connection_status else "N/A"
            logger.info(f"Stopping DataStream (current status: {ds_final_status})...")
            if hasattr(data_stream, 'stop'): data_stream.stop(reason_for_stop="Example script finishing")
        if user_stream:
            uhs_final_status = user_stream.connection_status.name if hasattr(user_stream, 'connection_status') and user_stream.connection_status else "N/A"
            logger.info(f"Stopping UserHubStream (current status: {uhs_final_status})...")
            if hasattr(user_stream, 'stop'): user_stream.stop(reason_for_stop="Example script finishing")
        logger.info("Concurrent streams example finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run concurrent Market Data and User Hub streams.")
    parser.add_argument(
        "--contract_id", 
        type=str, 
        default=DEFAULT_CONFIG_CONTRACT_ID, # This is fine, imported from tsxapipy (__init__)
        help="Contract ID to subscribe to for market data."
    )
    parser.add_argument(
        "--account_id", 
        type=str, # Keep as string to match how DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH is imported
        default=DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH, # This is the string version
        help="Account ID to watch for user-specific data. Set to '0', empty, or non-numeric to disable UserHubStream."
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
        default=max(60, (TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES * 60) // 3), # Correctly uses imported constant
        help="Interval (in seconds) to check and propagate APIClient token to streams."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG level logging for all modules."
    )
    args = parser.parse_args()

    if args.debug:
        # Set root logger level, which will be inherited
        logging.getLogger().setLevel(logging.DEBUG) 
        logger.info("DEBUG logging enabled for all modules via script flag.")

    if not args.contract_id:
        logger.error("A contract_id must be provided via --contract_id or in .env as DEFAULT_CONFIG_CONTRACT_ID.")
        sys.exit(1)
    
    # Parse account_id argument from string to Optional[int]
    parsed_account_id_int: Optional[int] = None
    if args.account_id and args.account_id.strip(): # Check if not None and not empty string
        try:
            acc_id_val = int(args.account_id)
            if acc_id_val > 0:
                parsed_account_id_int = acc_id_val
            else:
                logger.info(f"Account_id argument '{args.account_id}' is not a positive integer. UserHubStream will be disabled.")
        except ValueError:
            logger.warning(f"Invalid account_id argument: '{args.account_id}'. Must be an integer. UserHubStream will be disabled.")
    else:
        logger.info("No account_id provided or it's empty. UserHubStream will be disabled.")
        
    run_concurrent_streams_example(
        contract_id_arg=args.contract_id,
        account_id_arg=parsed_account_id_int, # Pass the parsed Optional[int]
        duration_seconds=args.duration,
        token_refresh_interval_seconds=args.token_refresh_check
    )