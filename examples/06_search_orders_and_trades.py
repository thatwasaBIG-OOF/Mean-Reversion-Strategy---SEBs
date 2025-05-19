"""
Example 06: Search Historical Orders and Trades.

This script demonstrates:
1. Authenticating and initializing APIClient.
2. Defining a time range (e.g., last 24 hours).
3. Searching for orders within that time range for a specified account.
4. Searching for trades within that time range for the same account.
"""
# pylint: disable=invalid-name # Allow filename for example script
import sys
import os
import logging
from datetime import datetime, timedelta
from typing import Optional # For APIClient type hint

# ---- sys.path modification ----
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ---- End sys.path modification ----

from tsxapipy.auth import authenticate
from tsxapipy.api import APIClient, APIError, AuthenticationError
from tsxapipy.common import UTC_TZ
from tsxapipy import DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH as ACCOUNT_ID

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s'
)
logger = logging.getLogger(__name__)

def run_example():
    """Runs the example for searching orders and trades."""
    logger.info("--- Example 6: Search Historical Orders and Trades ---")

    if not ACCOUNT_ID:
        logger.error("ACCOUNT_ID not set. Cannot search.")
        return

    api_client: Optional[APIClient] = None
    try:
        initial_token, token_acquired_at = authenticate()
        api_client = APIClient(initial_token=initial_token,
                               token_acquired_at=token_acquired_at)
        logger.info("APIClient initialized.")

        # --- Define Time Range (e.g., last 24 hours) ---
        end_time_utc = datetime.now(UTC_TZ)
        start_time_utc = end_time_utc - timedelta(days=1)

        start_iso = start_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_iso = end_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        log_search_orders = ("\nSearching orders for Account ID: %s from %s to %s",
                             ACCOUNT_ID, start_iso, end_iso)
        logger.info(*log_search_orders)

        orders_found = api_client.search_orders(
            account_id=int(ACCOUNT_ID) if ACCOUNT_ID else 0,
            start_timestamp_iso=start_iso,
            end_timestamp_iso=end_iso
        )

        if orders_found:
            logger.info("Found %d order(s) in the last 24 hours:",
                        len(orders_found))
            for order in orders_found[:5]: # Display first 5
                log_order_details = (
                    "  Order ID: %s, Contract: %s, Status: %s, Type: %s, "
                    "Side: %s, Size: %s, Created: %s",
                    order.get('id'), order.get('contractId'), order.get('status'),
                    order.get('type'), order.get('side'), order.get('size'),
                    order.get('creationTimestamp')
                )
                logger.info(*log_order_details)
        else:
            logger.info("No orders found in the last 24 hours for this account.")

        # --- Search for Trades ---
        log_search_trades = ("\nSearching trades for Account ID: %s from %s to %s",
                             ACCOUNT_ID, start_iso, end_iso)
        logger.info(*log_search_trades)
        trades_found = api_client.search_trades(
            account_id=int(ACCOUNT_ID) if ACCOUNT_ID else 0,
            start_timestamp_iso=start_iso,
            end_timestamp_iso=end_iso
        )

        if trades_found:
            logger.info("Found %d trade(s) in the last 24 hours:",
                        len(trades_found))
            for trade in trades_found[:5]: # Display first 5
                log_trade_details = (
                    "  Trade ID: %s, Order ID: %s, Contract: %s, Time: %s, "
                    "Price: %s, Size: %s, Side: %s, PnL: %s",
                    trade.get('id'), trade.get('orderId'),
                    trade.get('contractId'), trade.get('creationTimestamp'),
                    trade.get('price'), trade.get('size'), trade.get('side'),
                    trade.get('profitAndLoss')
                )
                logger.info(*log_trade_details)
        else:
            logger.info("No trades found in the last 24 hours for this account.")

    except AuthenticationError as e:
        logger.error("Auth failed: %s", e)
    except APIError as e:
        logger.error("API Error: %s", e)
    except ValueError as e: # Catches int(ACCOUNT_ID) or other config issues
        logger.error("Value/Config Error: %s", e)
    except Exception as e_gen: # pylint: disable=broad-exception-caught
        logger.error("Unexpected error: %s", e_gen, exc_info=True)

if __name__ == "__main__":
    run_example()