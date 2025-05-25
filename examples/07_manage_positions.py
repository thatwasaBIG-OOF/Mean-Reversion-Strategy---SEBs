"""
Example 07: Manage Positions.

This script demonstrates:
1. Searching for open positions for a specified account using Pydantic models.
2. If positions exist, attempting to partially close the first one found.
3. Attempting to fully close the remaining part of that position.
   Responses from close operations are now Pydantic models.

WARNING: This script can CLOSE or PARTIALLY CLOSE LIVE POSITIONS.
Ensure you are using a DEMO account or understand the risks.
"""
# pylint: disable=invalid-name # Allow filename for example script
# pylint: disable=too-many-locals, too-many-branches, too-many-statements
import sys
import os
import logging
import time
from typing import Optional, List # Added List

# ---- sys.path modification ----
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ---- End sys.path modification ----

from tsxapipy.auth import authenticate
from tsxapipy.api import APIClient
from tsxapipy import (
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH as ACCOUNT_ID_STR, # Keep as STR
    DEFAULT_CONFIG_CONTRACT_ID as FALLBACK_CONTRACT_ID,
    api_schemas # Import Pydantic schemas
)
from tsxapipy.api.exceptions import (
    APIError, AuthenticationError, ConfigurationError, LibraryError,
    APIResponseParsingError # Added
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s'
)
logger = logging.getLogger(__name__)

def run_example():
    """Runs the example for managing positions."""
    logger.warning("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    logger.warning("!!! WARNING: THIS SCRIPT CAN CLOSE OR PARTIALLY CLOSE LIVE POSITIONS! !!!")
    logger.warning("!!! ENSURE YOU ARE USING A DEMO ACCOUNT OR UNDERSTAND THE RISK!       !!!")
    logger.warning("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    if not ACCOUNT_ID_STR:
        logger.error("ACCOUNT_ID_TO_WATCH not set in .env. Cannot manage positions.")
        return
    
    try:
        account_id_int = int(ACCOUNT_ID_STR)
        if account_id_int <= 0:
            raise ValueError("ACCOUNT_ID_TO_WATCH must be a positive integer.")
    except ValueError:
        logger.error(f"Invalid ACCOUNT_ID_TO_WATCH in .env: '{ACCOUNT_ID_STR}'. Must be a positive integer.")
        return

    logger.info("--- Example 7: Manage Positions for Account %s (Using Pydantic Models) ---", account_id_int)
    api_client: Optional[APIClient] = None
    try:
        initial_token, token_acquired_at = authenticate()
        api_client = APIClient(initial_token=initial_token,
                               token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")

        # --- 1. Search for Open Positions ---
        logger.info("\nSearching for open positions for Account ID: %s...",
                    account_id_int)
        
        # api_client.search_open_positions now returns List[api_schemas.Position]
        open_positions: List[api_schemas.Position] = api_client.search_open_positions(
            account_id=account_id_int
        )

        if not open_positions:
            logger.info("No open positions found for this account.")
            logger.info("To test close/partial close, an open position is needed.")
            fallback_contract_display = FALLBACK_CONTRACT_ID or 'a known contract'
            logger.info("Consider placing an order for %s first.",
                        fallback_contract_display)
            return

        logger.info("Found %d open position(s):", len(open_positions))
        for pos_model in open_positions:
            logger.info("  Contract: %s, Size: %s, AvgPrice: %s",
                        pos_model.contract_id or "N/A",
                        pos_model.size if pos_model.size is not None else "N/A",
                        f"{pos_model.average_price:.4f}" if pos_model.average_price is not None else "N/A")

        # --- Attempt to manage the first open position found ---
        target_position_model = open_positions[0]
        pos_contract_id = target_position_model.contract_id
        pos_size = target_position_model.size if target_position_model.size is not None else 0

        if not pos_contract_id or pos_size == 0:
            logger.error("Invalid contract ID or size (0) from first open position: %s",
                         target_position_model.model_dump_json(indent=2) if target_position_model else "N/A")
            return

        logger.info("\nSelected position for management: %s, Current Size: %s",
                    pos_contract_id, pos_size)

        # --- 2. Partially Close Position (if abs(size) > 1) ---
        if abs(pos_size) > 1:
            partial_close_size = 1 # Attempt to close 1 lot
            logger.warning("Action: Attempting to PARTIALLY CLOSE %d lot(s) of %s.",
                           partial_close_size, pos_contract_id)
            partial_confirm_key = f"YES_PARTIAL_CLOSE_{pos_contract_id.replace('.', '_')}" # Make key filename-safe
            confirm_partial = input(f"Type '{partial_confirm_key}' to "
                                    "partially close: ")
            if confirm_partial == partial_confirm_key:
                try:
                    # partial_close_contract_position now returns schemas.PositionManagementResponse
                    response_model: api_schemas.PositionManagementResponse = \
                        api_client.partial_close_contract_position(
                            account_id=account_id_int,
                            contract_id=pos_contract_id,
                            size=partial_close_size
                        )
                    if response_model.success:
                        log_msg = ("Partial close request for %s (size %d) "
                                   "submitted successfully. Message: %s",
                                   pos_contract_id, partial_close_size, response_model.message or "None")
                        logger.info(*log_msg)
                        # Update local pos_size for subsequent full close logic
                        if pos_size > 0:
                            pos_size -= partial_close_size
                        else: # pos_size < 0
                            pos_size += partial_close_size 
                    else:
                        logger.error("Partial close request failed: %s (Code: %s)",
                                     response_model.error_message or "Unknown API error", response_model.error_code)
                except APIResponseParsingError as e_parse:
                    logger.error("API RESPONSE PARSING ERROR during partial close: %s", e_parse)
                except APIError as e:
                    logger.error("API Error during partial close: %s", e)
                time.sleep(3) # Allow time for position update
            else:
                logger.info("Partial close action cancelled by user.")
        else:
            logger.info("Position size for %s is %d. Skipping partial close (requires abs(size) > 1).",
                        pos_contract_id, pos_size)

        # --- 3. Fully Close Remaining Position (if any) ---
        if pos_size != 0: # Check updated pos_size
            logger.warning("Action: Attempting to FULLY CLOSE remaining position of %s "
                           "for %s.", pos_size, pos_contract_id)
            full_confirm_key = f"YES_FULL_CLOSE_{pos_contract_id.replace('.', '_')}"
            confirm_full = input(f"Type '{full_confirm_key}' to fully close: ")
            if confirm_full == full_confirm_key:
                try:
                    # close_contract_position now returns schemas.PositionManagementResponse
                    response_model_full: api_schemas.PositionManagementResponse = \
                        api_client.close_contract_position(
                            account_id=account_id_int,
                            contract_id=pos_contract_id
                        )
                    if response_model_full.success:
                        logger.info("Full close request for %s submitted successfully. Message: %s",
                                    pos_contract_id, response_model_full.message or "None")
                        pos_size = 0 # Position should be flat now
                    else:
                        logger.error("Full close request failed: %s (Code: %s)",
                                     response_model_full.error_message or "Unknown API error", response_model_full.error_code)
                except APIResponseParsingError as e_parse:
                    logger.error("API RESPONSE PARSING ERROR during full close: %s", e_parse)
                except APIError as e:
                    logger.error("API Error during full close: %s", e)
            else:
                logger.info("Full close action cancelled by user.")
        elif pos_contract_id: # pos_contract_id would be set if a position was initially found
            logger.info("Position for %s is now considered flat (or was initially size 1 and not partially closed). No full close action needed based on script logic.",
                        pos_contract_id)

    except ConfigurationError as e_conf:
        logger.error("CONFIGURATION ERROR: %s", e_conf)
    except AuthenticationError as e_auth:
        logger.error("Auth failed: %s", e_auth)
    except APIResponseParsingError as e_parse_main: # Catch parsing errors from search_open_positions
        logger.error("API RESPONSE PARSING ERROR: %s", e_parse_main)
        if e_parse_main.raw_response_text:
            logger.error("Raw problematic response text (preview): %s", e_parse_main.raw_response_text[:500])
    except APIError as e_api:
        logger.error("API Error: %s", e_api)
    except ValueError as e_val: 
        logger.error("Value/Config Error: %s", e_val)
    except LibraryError as e_lib:
        logger.error("LIBRARY ERROR: %s", e_lib)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("Unexpected error: %s", e_gen, exc_info=True)

if __name__ == "__main__":
    run_example()