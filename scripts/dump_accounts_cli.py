"""
CLI script to fetch and display TopStep account IDs and details.
Allows fetching all accounts or only active ones.
Now uses Pydantic models for account data.
"""
import sys
import os
import logging
import argparse
from typing import Optional, List # Added List

# ---- sys.path modification ----
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
# ---- End sys.path modification ----

from tsxapipy.auth import authenticate, AuthenticationError
from tsxapipy.api import APIClient
from tsxapipy.api.exceptions import (
    APIError, ConfigurationError, LibraryError, APIResponseParsingError # Added more
)
from tsxapipy import api_schemas # Import Pydantic schemas

# --- Configure Root Logging ---
LOG_FORMAT = '%(asctime)s - %(name)s [%(levelname)s]: %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
# --- End Root Logging Config ---

logger = logging.getLogger(__name__)

def main():
    """Main function to parse arguments and dump account details."""
    parser = argparse.ArgumentParser(
        description="Fetch and display account IDs and details."
    )
    parser.add_argument("--all", action="store_true",
                        help="Fetch all accounts (including inactive). "
                             "Default is active only.")
    parser.add_argument("--debug", action="store_true",
                        help="Enable DEBUG logging for all loggers.")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG) # Set root logger level
        logger.info("DEBUG logging enabled by CLI flag.")

    api_client: Optional[APIClient] = None
    try:
        logger.info("Authenticating...")
        initial_token, token_acquired_time = authenticate()

        api_client = APIClient(
            initial_token=initial_token,
            token_acquired_at=token_acquired_time
        )
        logger.info("APIClient initialized successfully.")

    except ConfigurationError as e_conf:
        logger.error("Configuration error: %s", e_conf)
        sys.exit(1)
    except AuthenticationError as e_auth:
        logger.error("Authentication failed: %s", e_auth)
        sys.exit(1)
    except ValueError as e_val: # From APIClient init if token/datetime invalid
        logger.error("Value error during APIClient init: %s", e_val)
        sys.exit(1)
    except LibraryError as e_lib: # Catch other general library errors during setup
        logger.error("Library error during setup: %s", e_lib)
        sys.exit(1)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("An unexpected error occurred during setup: %s", e_gen,
                     exc_info=True)
        sys.exit(1)

    fetch_only_active = not args.all
    logger.info("Fetching %s accounts...",
                'active only' if fetch_only_active else 'all')

    try:
        # api_client.get_accounts() now returns List[api_schemas.Account]
        accounts: List[api_schemas.Account] = api_client.get_accounts(only_active=fetch_only_active)

        if not accounts:
            logger.info("No accounts found matching the criteria.")
        else:
            account_details_log_message = [
                f"--- Found {len(accounts)} Account(s) ---"
            ]
            for i, acc_model in enumerate(accounts): # Iterate over Pydantic models
                account_details_log_message.append(f"Account #{i + 1}:")
                account_details_log_message.append(f"  ID:         {acc_model.id}")
                account_details_log_message.append(f"  Name:       {acc_model.name or 'N/A'}")
                account_details_log_message.append(f"  Balance:    {acc_model.balance:.2f}" if acc_model.balance is not None else "N/A")
                account_details_log_message.append(f"  Can Trade:  {acc_model.can_trade if acc_model.can_trade is not None else 'N/A'}")
                account_details_log_message.append(f"  Is Visible: {acc_model.is_visible if acc_model.is_visible is not None else 'N/A'}")
                account_details_log_message.append("-" * 20)
            logger.info("\n".join(account_details_log_message))

            print("\n--- Summary of Account IDs ---")
            for acc_model in accounts:
                print(f"ID: {acc_model.id}\tName: {acc_model.name or 'N/A'}")

            print("\nSuggestion: You can use one of these IDs for "
                  "ACCOUNT_ID_TO_WATCH in your .env file.")
        logger.info("Account dump complete.")

    except APIResponseParsingError as e_parse:
        logger.error("API RESPONSE PARSING ERROR retrieving account information: %s", e_parse)
        if e_parse.raw_response_text:
            logger.error("Raw problematic response text (preview): %s", e_parse.raw_response_text[:500])
    except APIError as e_api:
        logger.error("API Error retrieving account information: %s", e_api)
    except LibraryError as e_lib: # Catch other library errors during API call
        logger.error("Library error retrieving account information: %s", e_lib)
    except Exception as e_gen_fetch: # pylint: disable=broad-exception-caught
        logger.error("An unexpected error occurred while fetching accounts: %s",
                     e_gen_fetch, exc_info=True)

if __name__ == "__main__":
    main()