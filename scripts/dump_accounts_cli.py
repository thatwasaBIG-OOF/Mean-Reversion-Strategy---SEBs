"""
CLI script to fetch and display TopStep account IDs and details.
Allows fetching all accounts or only active ones.
"""
import sys
import os

# ---- sys.path modification ----
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
# ---- End sys.path modification ----

import logging
import argparse
from typing import Optional

from tsxapipy.auth import authenticate, AuthenticationError
from tsxapipy.api import APIClient
from tsxapipy.api.exceptions import APIError, ConfigurationError # Added ConfigurationError

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
        logging.getLogger().setLevel(logging.DEBUG)
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

    except ConfigurationError as e:
        logger.error("Configuration error: %s", e)
        sys.exit(1)
    except AuthenticationError as e:
        logger.error("Authentication failed: %s", e)
        sys.exit(1)
    except ValueError as e:
        logger.error("Value error during APIClient init: %s", e)
        sys.exit(1)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("An unexpected error occurred during setup: %s", e_gen,
                     exc_info=True)
        sys.exit(1)

    fetch_only_active = not args.all
    logger.info("Fetching %s accounts...",
                'active only' if fetch_only_active else 'all')

    try:
        accounts = api_client.get_accounts(only_active=fetch_only_active)

        if not accounts:
            logger.info("No accounts found matching the criteria.")
        else:
            account_details_log_message = [
                "--- Found %d Account(s) ---" % len(accounts)
            ]
            for i, acc in enumerate(accounts):
                account_details_log_message.append("Account #%d:" % (i + 1))
                account_details_log_message.append("  ID:         %s" % acc.get('id'))
                account_details_log_message.append("  Name:       %s" % acc.get('name'))
                account_details_log_message.append("  Balance:    %s" % acc.get('balance'))
                account_details_log_message.append("  Can Trade:  %s" % acc.get('canTrade'))
                account_details_log_message.append("  Is Visible: %s" % acc.get('isVisible'))
                account_details_log_message.append("-" * 20)
            logger.info("\n".join(account_details_log_message))

            print("\n--- Summary of Account IDs ---")
            for acc in accounts:
                print(f"ID: {acc.get('id')}\tName: {acc.get('name')}")

            print("\nSuggestion: You can use one of these IDs for "
                  "ACCOUNT_ID_TO_WATCH in your .env file.")
        logger.info("Account dump complete.")

    except APIError as e:
        logger.error("API Error retrieving account information: %s", e)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("An unexpected error occurred while fetching accounts: %s",
                     e_gen, exc_info=True)

if __name__ == "__main__":
    main()