"""
Example 07: Manage Positions.

This script demonstrates:
1. Searching for open positions for a specified account.
2. If positions exist, attempting to partially close the first one found (if size > 1).
3. Attempting to fully close the remaining part of that position.

WARNING: This script can CLOSE or PARTIALLY CLOSE LIVE POSITIONS.
Ensure you are using a DEMO account or understand the risks.
"""
# pylint: disable=invalid-name # Allow filename for example script
# pylint: disable=too-many-locals, too-many-branches, too-many-statements
import sys
import os
import logging
import time
from typing import Optional # For APIClient type hint
# from datetime import datetime # Not directly used

# ---- sys.path modification ----
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ---- End sys.path modification ----

from tsxapipy.auth import authenticate
from tsxapipy.api import APIClient
from tsxapipy import (
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH as ACCOUNT_ID,
    DEFAULT_CONFIG_CONTRACT_ID as FALLBACK_CONTRACT_ID
)
from tsxapipy.api.exceptions import APIError, AuthenticationError

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

    if not ACCOUNT_ID:
        logger.error("ACCOUNT_ID not set. Cannot manage positions.")
        return
    
    account_id_int = int(ACCOUNT_ID) # Do this once

    logger.info("--- Example 7: Manage Positions for Account %s ---", account_id_int)
    api_client: Optional[APIClient] = None
    try:
        initial_token, token_acquired_at = authenticate()
        api_client = APIClient(initial_token=initial_token,
                               token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")

        # --- 1. Search for Open Positions ---
        logger.info("\nSearching for open positions for Account ID: %s...",
                    account_id_int)
        open_positions = api_client.search_open_positions(
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
        for pos in open_positions:
            logger.info("  Contract: %s, Size: %s, AvgPrice: %s",
                        pos.get('contractId'), pos.get('size'),
                        pos.get('averagePrice'))

        # --- Attempt to manage the first open position found ---
        target_position = open_positions[0]
        pos_contract_id = target_position.get("contractId")
        pos_size = target_position.get("size", 0)

        if not pos_contract_id or pos_size == 0:
            logger.error("Invalid contract ID or size from first open position: %s",
                         target_position)
            return

        logger.info("\nSelected position for management: %s, Current Size: %s",
                    pos_contract_id, pos_size)

        # --- 2. Partially Close Position (if size > 1 or < -1) ---
        if abs(pos_size) > 1:
            partial_close_size = 1
            logger.warning("Action: Attempting to PARTIALLY CLOSE %d lot(s) of %s.",
                           partial_close_size, pos_contract_id)
            partial_confirm_key = f"YES_PARTIAL_CLOSE_{pos_contract_id}"
            confirm_partial = input(f"Type '{partial_confirm_key}' to "
                                    "partially close: ")
            if confirm_partial == partial_confirm_key:
                try:
                    response = api_client.partial_close_contract_position(
                        account_id=account_id_int,
                        contract_id=pos_contract_id,
                        size=partial_close_size
                    )
                    if response.get("success"):
                        log_msg = ("Partial close request for %s (size %d) "
                                   "submitted successfully.",
                                   pos_contract_id, partial_close_size)
                        logger.info(*log_msg)
                        pos_size -= partial_close_size if pos_size > 0 \
                                                       else -partial_close_size
                    else:
                        logger.error("Partial close request failed: %s",
                                     response.get('errorMessage'))
                except APIError as e:
                    logger.error("API Error during partial close: %s", e)
                time.sleep(3)
            else:
                logger.info("Partial close action cancelled by user.")
        else:
            logger.info("Position size for %s is 1 (or -1). Skipping partial close.",
                        pos_contract_id)

        # --- 3. Fully Close Remaining Position (if any) ---
        if pos_size != 0:
            logger.warning("Action: Attempting to FULLY CLOSE remaining position of %s "
                           "for %s.", pos_size, pos_contract_id)
            full_confirm_key = f"YES_FULL_CLOSE_{pos_contract_id}"
            confirm_full = input(f"Type '{full_confirm_key}' to fully close: ")
            if confirm_full == full_confirm_key:
                try:
                    response = api_client.close_contract_position(
                        account_id=account_id_int,
                        contract_id=pos_contract_id
                    )
                    if response.get("success"):
                        logger.info("Full close request for %s submitted successfully.",
                                    pos_contract_id)
                    else:
                        logger.error("Full close request failed: %s",
                                     response.get('errorMessage'))
                except APIError as e:
                    logger.error("API Error during full close: %s", e)
            else:
                logger.info("Full close action cancelled by user.")
        else:
            logger.info("Position for %s is now flat. No full close needed.",
                        pos_contract_id)

    except AuthenticationError as e:
        logger.error("Auth failed: %s", e)
    except APIError as e:
        logger.error("API Error: %s", e)
    except ValueError as e: # Catches int(ACCOUNT_ID) if it's not set or other issues
        logger.error("Value/Config Error: %s", e)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("Unexpected error: %s", e_gen, exc_info=True)

if __name__ == "__main__":
    run_example()