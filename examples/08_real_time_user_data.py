# examples/08_real_time_user_data.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

import logging
import time
import pprint
from typing import Any, Optional

from tsxapipy import (
    APIClient,
    UserHubStream, # Import UserHubStream
    authenticate,
    AuthenticationError,
    ConfigurationError,
    APIError,
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH as DEFAULT_ACCOUNT_ID,
    # TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES # Not directly needed here with factory
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s')
logger = logging.getLogger(__name__)

# Global or passed-in variable to make account_id accessible to callbacks if needed
current_account_watching: Optional[int] = None

# Define callback functions for UserHubStream events
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

def handle_user_stream_state_change(state: str):
    logger.info(f"User Hub Stream state changed to: {state} for account {current_account_watching or 'N/A'}")

def handle_user_stream_error(error: Any):
    logger.error(f"User Hub Stream encountered an error for account {current_account_watching or 'N/A'}: {error}")


def run_user_data_example():
    global current_account_watching
    # DEFAULT_ACCOUNT_ID from config is already an int or None
    account_id_for_test: Optional[int] = DEFAULT_ACCOUNT_ID
    
    # Check if the integer account_id_for_test is valid
    if not account_id_for_test or account_id_for_test <= 0:
        logger.error("DEFAULT_ACCOUNT_ID_TO_WATCH is not set or invalid in your .env file. "
                       "This example requires a valid account ID to monitor.")
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
            api_client=api_client, # MODIFIED: Pass APIClient instance
            account_id_to_watch=account_id_for_test, # Pass the integer value
            on_order_update=handle_user_order_update,
            on_account_update=handle_user_account_update,
            on_position_update=handle_user_position_update,
            on_user_trade_update=handle_user_trade_execution,
            subscribe_to_accounts_globally=True, # Set to False if you only want specific account updates
            on_state_change_callback=handle_user_stream_state_change,
            on_error_callback=handle_user_stream_error
        )
        logger.info("UserHubStream initialized.")

        if not stream.start():
            logger.error("Failed to start UserHubStream. Exiting example.")
            return

        logger.info("User Hub stream started. Monitoring for 60 seconds... Press Ctrl+C to stop earlier.")
        logger.info("Perform some actions on your account (e.g., place/cancel an order) in the trading platform to see updates.")
        
        main_loop_duration = 60  # seconds
        check_interval = 5     # seconds
        end_time = time.monotonic() + main_loop_duration

        while time.monotonic() < end_time:
            logger.debug(f"Stream status for Account {account_id_for_test}: {stream.connection_status}")
            # With accessTokenFactory, the stream should self-manage token for reconnections.
            time.sleep(check_interval)
        
        logger.info("Example duration finished.")

    except ConfigurationError as e:
        logger.error(f"CONFIGURATION ERROR: {e}")
    except AuthenticationError as e:
        logger.error(f"AUTHENTICATION FAILED: {e}")
    except APIError as e:
        logger.error(f"API ERROR: {e}")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down...")
    except Exception as e:
        logger.error(f"AN UNEXPECTED ERROR OCCURRED: {e}", exc_info=True)
    finally:
        if stream:
            logger.info("Stopping User Hub stream...")
            stream.stop()
        logger.info("User Hub data example finished.")

if __name__ == "__main__":
    run_user_data_example()