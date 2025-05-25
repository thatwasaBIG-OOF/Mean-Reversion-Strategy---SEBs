"""
Example 01: Authenticate and Get Accounts.

This script demonstrates:
1. Importing necessary components from the tsxapipy library.
2. Authenticating with the API using credentials from the .env file.
3. Initializing the APIClient.
4. Fetching and displaying all accounts, now using Pydantic models.
5. Fetching and displaying only active accounts, now using Pydantic models.
"""
# pylint: disable=invalid-name  # Allow filename for example script
import sys
import os
import logging
from typing import Optional # Added for APIClient type hint

# ---- sys.path modification ----
# Ensure the 'src' directory is on the path to find the 'tsxapipy' package
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ---- End sys.path modification ----

from tsxapipy.auth import authenticate
from tsxapipy.api import APIClient, APIError, AuthenticationError
from tsxapipy.api.exceptions import ConfigurationError, LibraryError, APIResponseParsingError
from tsxapipy import api_schemas # Import the schemas module

# --- Configure Logging ---
# Basic configuration for the example script's output
# Library modules will use their own loggers but inherit this level if not configured otherwise.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__) # Logger for this specific example script

def run_example():
    """Runs the example for authenticating and fetching accounts."""
    logger.info("--- Example 01: Authenticate and Get Accounts (Using Pydantic Models) ---")
    api_client: Optional[APIClient] = None
    try:
        # 1. Authenticate
        logger.info("Attempting authentication...")
        initial_token, token_acquired_at = authenticate()
        logger.info("Authentication successful. Token acquired at: %s",
                    token_acquired_at.isoformat())

        # 2. Create APIClient instance
        api_client = APIClient(initial_token=initial_token,
                               token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")

        # 3. Get Account Details
        logger.info("Fetching all accounts (including inactive)...")
        # api_client.get_accounts() now returns List[api_schemas.Account]
        all_accounts: List[api_schemas.Account] = api_client.get_accounts(only_active=False)
        
        if all_accounts:
            logger.info("Found %d total accounts:", len(all_accounts))
            for acc_model in all_accounts: # Iterate over Pydantic models
                logger.info("  ID: %s, Name: %s, Balance: %s, CanTrade: %s, Visible: %s",
                            acc_model.id,
                            acc_model.name if acc_model.name is not None else "N/A",
                            f"{acc_model.balance:.2f}" if acc_model.balance is not None else "N/A",
                            acc_model.can_trade if acc_model.can_trade is not None else "N/A",
                            acc_model.is_visible if acc_model.is_visible is not None else "N/A")
        else:
            logger.info("No accounts found (when fetching all).")

        logger.info("\nFetching only active accounts...")
        active_accounts: List[api_schemas.Account] = api_client.get_accounts(only_active=True)
        if active_accounts:
            logger.info("Found %d active accounts:", len(active_accounts))
            for acc_model in active_accounts:
                logger.info("  ID: %s, Name: %s, Balance: %s",
                            acc_model.id,
                            acc_model.name if acc_model.name is not None else "N/A",
                            f"{acc_model.balance:.2f}" if acc_model.balance is not None else "N/A")
        else:
            logger.info("No active accounts found.")

    except ConfigurationError as e:
        logger.error("CONFIGURATION ERROR: %s", e)
        logger.error("Please ensure API_KEY and USERNAME are correctly set in "
                     "your .env file (located at the project root).")
    except AuthenticationError as e:
        logger.error("AUTHENTICATION FAILED: %s", e)
    except APIResponseParsingError as e_parse: # Catch Pydantic validation errors from APIClient
        logger.error("API RESPONSE PARSING ERROR: %s", e_parse)
        if e_parse.raw_response_text:
            logger.error("Raw problematic response text (preview): %s", e_parse.raw_response_text[:500])
    except APIError as e: # Catches other API-related errors from APIClient
        logger.error("API ERROR: %s", e)
    except ValueError as e: # For other ValueErrors (e.g., during APIClient init)
        logger.error("VALUE ERROR: %s (This might indicate an issue with data "
                     "passed to a function or an unexpected internal value).", e)
    except LibraryError as e: # Catch any other library-specific error not caught above
        logger.error("LIBRARY ERROR: %s", e)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("AN UNEXPECTED ERROR OCCURRED: %s", e_gen, exc_info=True)

if __name__ == "__main__":
    run_example()