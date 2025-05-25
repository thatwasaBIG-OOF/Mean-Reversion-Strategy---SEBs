"""
Example 08: Real-Time User Data Stream.

This script demonstrates:
1. Authenticating and initializing APIClient.
2. Initializing UserHubStream with an APIClient instance.
3. Starting the stream and monitoring user-specific events like orders,
   account updates, positions, and trades.
4. Handling stream state changes and errors.
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

import logging
import time
import pprint
from typing import Any, Optional

from tsxapipy import (
    APIClient,
    UserHubStream, 
    authenticate,
    AuthenticationError,
    ConfigurationError,
    APIError,
    LibraryError, # Added
    StreamConnectionState, # Added
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH as DEFAULT_ACCOUNT_ID 
)
from tsxapipy.api.exceptions import APIResponseParsingError # Added

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s')
logger = logging.getLogger(__name__)

current_account_watching: Optional[int] = None

def handle_user_order_update(order_data: Any):
    logger.info(f"--- USER ORDER Update for Account {order_data.get('accountId', current_account_watching or 'N/A')} ---")
    logger.info(pprint.pformat(order_data, indent=2, width=120))

def handle_user_account_update(account_data: Any):
    logger.info(f"--- USER ACCOUNT Update for Account {account_data.get('id', current_account_watching or 'N/A')} ---")
    logger.info(pprint.pformat(account_data, indent=2, width=120))

def handle_user_position_update(position_data: Any):
    logger.info(f"--- USER POSITION Update for Account {position_data.get('accountId', current_account_watching or 'N/A')} ---")
    logger.info(pprint.pformat(position_data, indent=2, width=120))

def handle_user_trade_execution(trade_data: Any):
    logger.info(f"--- USER TRADE Execution for Account {trade_data.get('accountId', current_account_watching or 'N/A')} ---")
    logger.info(pprint.pformat(trade_data, indent=2, width=120))

def handle_user_stream_state_change(state_str: str): # Receives state name as string
    logger.info(f"User Hub Stream state changed to: {state_str} for account {current_account_watching or 'N/A'}")

def handle_user_stream_error(error: Any):
    logger.error(f"User Hub Stream encountered an error for account {current_account_watching or 'N/A'}: {error}")


def run_user_data_example():
    global current_account_watching
    account_id_for_test: Optional[int] = DEFAULT_ACCOUNT_ID # Already Optional[int] from config
    
    if not isinstance(account_id_for_test, int) or account_id_for_test <= 0:
        logger.error("DEFAULT_ACCOUNT_ID_TO_WATCH is not set or is not a valid positive integer in your .env file. "
                       "This example requires a valid positive account ID to monitor effectively for all event types.")
        return
        
    current_account_watching = account_id_for_test
    logger.info(f"--- Example: Real-Time User Data for Account ID: {account_id_for_test} ---")

    api_client: Optional[APIClient] = None
    stream: Optional[UserHubStream] = None

    try:
        logger.info("Authenticating...")
        initial_token, token_acquired_at = authenticate()
        
        api_client = APIClient(
            initial_token=initial_token,
            token_acquired_at=token_acquired_at
        )
        logger.info("APIClient initialized.")

        logger.info(f"Initializing UserHubStream for Account ID: {account_id_for_test}...")
        stream = UserHubStream(
            api_client=api_client, 
            account_id_to_watch=account_id_for_test, 
            on_order_update=handle_user_order_update,
            on_account_update=handle_user_account_update,
            on_position_update=handle_user_position_update,
            on_user_trade_update=handle_user_trade_execution,
            subscribe_to_accounts_globally=True, 
            on_state_change_callback=handle_user_stream_state_change,
            on_error_callback=handle_user_stream_error
        )
        logger.info("UserHubStream initialized.")

        if not stream.start():
            logger.error(f"Failed to start UserHubStream (current status: {stream.connection_status.name if stream else 'N/A'}). Exiting example.")
            return

        logger.info("User Hub stream started. Monitoring for 60 seconds... Press Ctrl+C to stop earlier.")
        logger.info("Perform some actions on your account (e.g., place/cancel an order) in the trading platform to see updates.")
        
        main_loop_duration = 60  
        check_interval = 5     
        end_time = time.monotonic() + main_loop_duration

        while time.monotonic() < end_time:
            if stream: # Check if stream object exists
                # Log the string name of the Enum state
                logger.debug(f"Stream status for Account {account_id_for_test}: {stream.connection_status.name}")
            else:
                logger.debug(f"Stream object not available for Account {account_id_for_test}.")
                break # Exit loop if stream somehow becomes None
            
            # The UserHubStream (and DataStream) class's update_token method should be called
            # by an application-level timer if long-lived token refresh is needed.
            # This example relies on signalrcore's auto-reconnect with the initial token,
            # which will eventually fail if the token expires without an explicit update_token call.
            # For a short example, this is fine. For long-running apps, token refresh is crucial.
            time.sleep(check_interval)
        
        logger.info("Example duration finished.")

    except ConfigurationError as e_conf:
        logger.error(f"CONFIGURATION ERROR: {e_conf}")
    except AuthenticationError as e_auth:
        logger.error(f"AUTHENTICATION FAILED: {e_auth}")
    except APIResponseParsingError as e_parse: # Catch Pydantic validation errors from APIClient
        logger.error(f"API RESPONSE PARSING ERROR (likely during auth or token refresh): {e_parse}")
        if e_parse.raw_response_text:
            logger.error("Raw problematic response text (preview): %s", e_parse.raw_response_text[:500])
    except APIError as e_api:
        logger.error(f"API ERROR: {e_api}")
    except LibraryError as e_lib: # Catch other library-specific errors
        logger.error(f"LIBRARY ERROR: {e_lib}")
    except ValueError as e_val: # e.g. int conversion for ACCOUNT_ID
        logger.error(f"VALUE ERROR: {e_val}")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down...")
    except Exception as e_generic: # Catch-all
        logger.error(f"AN UNEXPECTED ERROR OCCURRED: {e_generic}", exc_info=True)
    finally:
        if stream:
            logger.info(f"Stopping User Hub stream (current status: {stream.connection_status.name})...")
            stream.stop()
        logger.info("User Hub data example finished.")

if __name__ == "__main__":
    run_user_data_example()