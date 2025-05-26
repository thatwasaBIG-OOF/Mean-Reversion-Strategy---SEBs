# C:\work\tsxapi4py\examples\08_real_time_user_data.py

import logging
import time
import pprint # For pretty printing the received data
from typing import Any, Optional, Dict # Ensure Dict is imported if used by callbacks

# Correct imports based on previous fixes for __init__.py and config constants
from tsxapipy import (
    APIClient,
    UserHubStream, 
    authenticate,
    AuthenticationError,
    ConfigurationError,
    APIError,
    LibraryError,
    StreamConnectionState, # Useful for checking stream.connection_status
    # This is the string version from config, exported by __init__.py
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH as DEFAULT_ACCOUNT_ID_STR_FROM_CONFIG 
)
from tsxapipy.config import TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES # Import the constant

# Import exceptions and schemas if more detailed API interaction or error handling is done
from tsxapipy.api.exceptions import APIResponseParsingError 
from tsxapipy.api import schemas as api_schemas 
from tsxapipy.common.time_utils import UTC_TZ 


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__) # Use __main__ if script is run directly, or specific name

# --- Global or state variable for callbacks to know which account is being watched ---
current_account_watching: Optional[int] = None

# --- Callback Function Definitions (These were missing) ---
def handle_user_order_update(order_data: Dict[str, Any]):
    logger.info(f"--- USER ORDER Update for Account {order_data.get('accountId', current_account_watching or 'N/A')} ---")
    logger.info(pprint.pformat(order_data, indent=2, width=120))

def handle_user_account_update(account_data: Dict[str, Any]):
    logger.info(f"--- USER ACCOUNT Update for Account {account_data.get('id', current_account_watching or 'N/A')} ---")
    logger.info(pprint.pformat(account_data, indent=2, width=120))

def handle_user_position_update(position_data: Dict[str, Any]):
    logger.info(f"--- USER POSITION Update for Account {position_data.get('accountId', current_account_watching or 'N/A')} ---")
    logger.info(pprint.pformat(position_data, indent=2, width=120))

def handle_user_trade_execution(trade_data: Dict[str, Any]): # Renamed from handle_user_trade_update for clarity
    logger.info(f"--- USER TRADE Execution for Account {trade_data.get('accountId', current_account_watching or 'N/A')} ---")
    logger.info(pprint.pformat(trade_data, indent=2, width=120))

def handle_user_stream_state_change(state_str: str): # Receives state name as string
    logger.info(f"User Hub Stream state changed to: {state_str} for account {current_account_watching or 'Global/N/A'}")

def handle_user_stream_error(error: Any):
    logger.error(f"User Hub Stream encountered an error for account {current_account_watching or 'Global/N/A'}: {error}")
# --- End of Callback Function Definitions ---


def run_user_data_example():
    global current_account_watching # Allow modification of the global
    
    account_id_for_test: Optional[int] = None
    if DEFAULT_ACCOUNT_ID_STR_FROM_CONFIG and DEFAULT_ACCOUNT_ID_STR_FROM_CONFIG.strip():
        try:
            account_id_for_test = int(DEFAULT_ACCOUNT_ID_STR_FROM_CONFIG)
            if account_id_for_test <= 0:
                logger.error(
                    f"DEFAULT_ACCOUNT_ID_TO_WATCH ('{DEFAULT_ACCOUNT_ID_STR_FROM_CONFIG}') from .env "
                    f"parsed to a non-positive integer ({account_id_for_test}). "
                    "A positive integer is required."
                )
                account_id_for_test = None 
        except ValueError:
            logger.error(
                f"DEFAULT_ACCOUNT_ID_TO_WATCH ('{DEFAULT_ACCOUNT_ID_STR_FROM_CONFIG}') from .env "
                "is not a valid integer."
            )
            account_id_for_test = None
    
    if account_id_for_test is None: 
        logger.error(
            "A valid positive ACCOUNT_ID_TO_WATCH was not found or parsed correctly from your .env file. "
            "This example requires it to monitor effectively for all event types."
        )
        return
        
    current_account_watching = account_id_for_test # Set the global for callbacks
    logger.info(f"--- Example: Real-Time User Data for Account ID: {account_id_for_test} ---")

    api_client: Optional[APIClient] = None
    stream: Optional[UserHubStream] = None

    try:
        logger.info("Authenticating...")
        initial_token, token_acquired_at = authenticate() 
        if not initial_token or not token_acquired_at:
            logger.error("Authentication failed. Cannot proceed.")
            return
        
        api_client = APIClient(
            initial_token=initial_token,
            token_acquired_at=token_acquired_at
            # Ensure APIClient __init__ can pick up reauth_username/api_key from config if not passed
        )
        logger.info("APIClient initialized.")

        logger.info(f"Initializing UserHubStream for Account ID: {account_id_for_test}...")
        stream = UserHubStream(
            api_client=api_client, 
            account_id_to_watch=account_id_for_test, 
            on_order_update=handle_user_order_update,
            on_account_update=handle_user_account_update,
            on_position_update=handle_user_position_update,
            on_user_trade_update=handle_user_trade_execution, # Corrected name
            subscribe_to_accounts_globally=True,             
            on_state_change_callback=handle_user_stream_state_change, 
            on_error_callback=handle_user_stream_error          
        )
        logger.info("UserHubStream initialized.")

        if not stream.start():
            current_state_name = stream.connection_status.name if stream and hasattr(stream, 'connection_status') else "N/A"
            logger.error(f"Failed to start UserHubStream (current status: {current_state_name}). Exiting example.")
            return

        logger.info("User Hub stream started. Monitoring for 60 seconds... Press Ctrl+C to stop earlier.")
        logger.info("Perform some actions on your account (e.g., place/cancel an order) in the trading platform to see updates.")
        
        main_loop_duration = 60  
        check_interval = 5     
        end_time = time.monotonic() + main_loop_duration
        last_token_update_check = time.monotonic()

        while time.monotonic() < end_time:
            current_monotonic_time = time.monotonic()
            if stream:
                stream_state_name = stream.connection_status.name if hasattr(stream, 'connection_status') else "N/A"
                logger.debug(f"Stream status for Account {account_id_for_test}: {stream_state_name}")

                if api_client and hasattr(stream, 'update_token') and \
                   (current_monotonic_time - last_token_update_check > (TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES * 60) / 2): 
                    logger.info("UserHubStream: Attempting periodic token update...")
                    try:
                        latest_api_token = api_client.current_token 
                        stream.update_token(latest_api_token) 
                        logger.info("UserHubStream: Token update call made.")
                    except Exception as e_token_update:
                        logger.error(f"UserHubStream: Error during token update: {e_token_update}")
                    last_token_update_check = current_monotonic_time
            else:
                logger.debug(f"Stream object not available for Account {account_id_for_test}.")
                break 
            
            time.sleep(check_interval)
        
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
    except ValueError as e_val: 
        logger.error(f"VALUE ERROR: {e_val}") 
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down...")
    except Exception as e_generic: 
        logger.error(f"AN UNEXPECTED ERROR OCCURRED: {e_generic}", exc_info=True)
    finally:
        if stream:
            stream_state_name_final = stream.connection_status.name if hasattr(stream, 'connection_status') else "N/A"
            logger.info(f"Stopping User Hub stream (current status: {stream_state_name_final})...")
            stream.stop(reason_for_stop="Example script finishing") 
        logger.info("User Hub data example finished.")


if __name__ == "__main__":
    # Add the sys.path modification here if running this example directly
    # and the tsxapipy package is not installed in the Python environment.
    import sys
    import os
    # Assuming this example file is in C:\work\tsxapi4py\examples
    # and the 'src' directory containing 'tsxapipy' is at C:\work\tsxapi4py\src
    SRC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
    if SRC_PATH not in sys.path:
        sys.path.insert(0, SRC_PATH)
        print(f"Added to sys.path for example: {SRC_PATH}")

    run_user_data_example()