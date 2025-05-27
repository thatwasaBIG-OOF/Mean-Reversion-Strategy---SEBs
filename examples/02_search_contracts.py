"""
Example 02: Search Contracts.

This script demonstrates:
1. Authenticating with the API.
2. Initializing the APIClient.
3. Searching for contracts using a text query and Pydantic models.
4. Searching for a specific contract by its ID using Pydantic models.
"""
# pylint: disable=invalid-name # Allow filename for example script
import sys
import os
import logging
from typing import Optional, List # Added List for type hinting

# ---- sys.path modification ----
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ---- End sys.path modification ----

from tsxapipy.auth import authenticate
from tsxapipy.api import APIClient, APIError, AuthenticationError
from tsxapipy.api.exceptions import ConfigurationError, LibraryError, APIResponseParsingError # Added
from tsxapipy import api_schemas # Import Pydantic schemas

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s'
)
logger = logging.getLogger(__name__)

def run_example():
    """Runs the example for searching contracts."""
    logger.info("--- Example 2: Search Contracts (Using Pydantic Models) ---")
    api_client: Optional[APIClient] = None
    try:
        initial_token, token_acquired_at = authenticate()
        api_client = APIClient(initial_token=initial_token,
                               token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")

        # --- Search by text ---
        search_query = "NQ" # Example: Search for Gold futures
        logger.info("\nSearching for contracts with text: '%s'...", search_query)
        
        # api_client.search_contracts now returns List[api_schemas.Contract]
        contracts_by_text: List[api_schemas.Contract] = api_client.search_contracts(
            search_text=search_query,
            live=False # Search non-live contracts
        )
        
        if contracts_by_text:
            logger.info("Found %d contract(s) for '%s':",
                        len(contracts_by_text), search_query)
            for contract_model in contracts_by_text[:5]: # Display first 5
                logger.info("  ID: %s, Name: %s, Desc: %s",
                            contract_model.id if contract_model.id else "N/A",
                            contract_model.name if contract_model.name is not None else "N/A",
                            contract_model.description if contract_model.description is not None else "N/A")
        else:
            logger.info("No contracts found for '%s'.", search_query)

        # --- Search by a specific known ID ---
        # Replace with a valid contract ID you know or get from the previous search
        known_contract_id = "CON.F.US.NQ.M24" # EXAMPLE - THIS MIGHT BE EXPIRED
        if contracts_by_text and contracts_by_text[0].id: # Try to use an ID from the previous search if available and not None
            known_contract_id = contracts_by_text[0].id
            logger.info("Using contract ID from previous search: %s", known_contract_id)


        logger.info("\nSearching for contract by ID: '%s'...", known_contract_id)
        # api_client.search_contract_by_id now returns List[api_schemas.Contract]
        contracts_by_id: List[api_schemas.Contract] = api_client.search_contract_by_id(
            contract_id=known_contract_id
        )
        if contracts_by_id:
            logger.info("Found %d contract(s) for ID '%s':",
                        len(contracts_by_id), known_contract_id)
            for contract_model in contracts_by_id:
                logger.info("  ID: %s, Name: %s, Desc: %s, TickSize: %s, TickValue: %s, InstrumentID: %s",
                            contract_model.id if contract_model.id else "N/A",
                            contract_model.name if contract_model.name is not None else "N/A",
                            contract_model.description if contract_model.description is not None else "N/A",
                            f"{contract_model.tick_size:.4f}" if contract_model.tick_size is not None else "N/A",
                            f"{contract_model.tick_value:.2f}" if contract_model.tick_value is not None else "N/A",
                            contract_model.instrument_id if contract_model.instrument_id is not None else "N/A"
                           )
        else:
            logger.info("No contract found for ID '%s'.", known_contract_id)

    except ConfigurationError as e:
        logger.error("CONFIGURATION ERROR: %s", e)
    except AuthenticationError as e:
        logger.error("Auth failed: %s", e)
    except APIResponseParsingError as e_parse:
        logger.error("API RESPONSE PARSING ERROR: %s", e_parse)
        if e_parse.raw_response_text:
            logger.error("Raw problematic response text (preview): %s", e_parse.raw_response_text[:500])
    except APIError as e:
        logger.error("API Error: %s", e)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("Unexpected error: %s", e_gen, exc_info=True)

if __name__ == "__main__":
    run_example()