"""
Example 02: Search Contracts.

This script demonstrates:
1. Authenticating with the API.
2. Initializing the APIClient.
3. Searching for contracts using a text query.
4. Searching for a specific contract by its ID.
"""
# pylint: disable=invalid-name # Allow filename for example script
import sys
import os
import logging
# from datetime import datetime # Not strictly needed for this example
from typing import Optional # For APIClient type hint

# ---- sys.path modification ----
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ---- End sys.path modification ----

from tsxapipy.auth import authenticate
from tsxapipy.api import APIClient, APIError, AuthenticationError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s'
)
logger = logging.getLogger(__name__)

def run_example():
    """Runs the example for searching contracts."""
    logger.info("--- Example 2: Search Contracts ---")
    api_client: Optional[APIClient] = None
    try:
        initial_token, token_acquired_at = authenticate()
        api_client = APIClient(initial_token=initial_token,
                               token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")

        # --- Search by text ---
        search_query = "GC" # Example: Search for Gold futures
        logger.info("\nSearching for contracts with text: '%s'...", search_query)
        # Search non-live contracts
        contracts_by_text = api_client.search_contracts(search_text=search_query,
                                                        live=False)
        if contracts_by_text:
            logger.info("Found %d contract(s) for '%s':",
                        len(contracts_by_text), search_query)
            for contract in contracts_by_text[:5]: # Display first 5
                logger.info("  ID: %s, Name: %s, Desc: %s",
                            contract.get('id'), contract.get('name'),
                            contract.get('description'))
        else:
            logger.info("No contracts found for '%s'.", search_query)

        # --- Search by a specific known ID ---
        # Replace with a valid contract ID you know or get from the previous search
        known_contract_id = "CON.F.US.NQ.M24" # EXAMPLE - THIS MIGHT BE EXPIRED
        if contracts_by_text: # Try to use an ID from the previous search if available
            known_contract_id = contracts_by_text[0].get('id', known_contract_id)

        logger.info("\nSearching for contract by ID: '%s'...", known_contract_id)
        contracts_by_id = api_client.search_contract_by_id(
            contract_id=known_contract_id
        )
        if contracts_by_id:
            logger.info("Found %d contract(s) for ID '%s':",
                        len(contracts_by_id), known_contract_id)
            for contract in contracts_by_id:
                logger.info("  ID: %s, Name: %s, Desc: %s, TickSize: %s, TickValue: %s",
                            contract.get('id'), contract.get('name'),
                            contract.get('description'), contract.get('tickSize'),
                            contract.get('tickValue'))
        else:
            logger.info("No contract found for ID '%s'.", known_contract_id)

    except AuthenticationError as e:
        logger.error("Auth failed: %s", e)
    except APIError as e:
        logger.error("API Error: %s", e)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("Unexpected error: %s", e_gen, exc_info=True)

if __name__ == "__main__":
    run_example()