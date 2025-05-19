# examples/12_dedicated_market_data_stream.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

import logging
import time
import pprint
import argparse
from typing import Any, Optional

from tsxapipy import (
    APIClient,
    DataStream,
    authenticate,
    AuthenticationError,
    ConfigurationError,
    APIError,
    DEFAULT_CONFIG_CONTRACT_ID,
    TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES
)

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("MarketDataStreamExample")

# --- Global for Callback context (optional) ---
current_contract_streaming: Optional[str] = None

# --- DataStream Callbacks ---
def handle_live_quote(quote_data: Any):
    logger.info(f"--- LIVE QUOTE ({current_contract_streaming}) ---")
    logger.info(f"  Bid: {quote_data.get('bp')} @ {quote_data.get('bs')} | Ask: {quote_data.get('ap')} @ {quote_data.get('as')} | Last: {quote_data.get('price')} @ {quote_data.get('volume')} (ts: {quote_data.get('timestamp')})")
    # logger.debug(pprint.pformat(quote_data)) # For full data

def handle_live_trade(trade_data: Any):
    logger.info(f"--- LIVE TRADE ({current_contract_streaming}) ---")
    logger.info(f"  Price: {trade_data.get('price')} | Size: {trade_data.get('size')} | Aggressor: {trade_data.get('aggressorSide')} (ts: {trade_data.get('timestamp')})")
    # logger.debug(pprint.pformat(trade_data))

def handle_live_depth(depth_data: Any):
    logger.info(f"--- LIVE DEPTH ({current_contract_streaming}) ---")
    # Depth data can be extensive; decide how much to log
    bids = depth_data.get('bids', [])
    asks = depth_data.get('asks', [])
    logger.info(f"  Top 3 Bids: {bids[:3]}")
    logger.info(f"  Top 3 Asks: {asks[:3]}")
    # logger.debug(pprint.pformat(depth_data))

def handle_stream_state_change(state: str):
    logger.info(f"MarketDataStream for '{current_contract_streaming}' state changed to: {state}")

def handle_stream_error(error: Any):
    logger.error(f"MarketDataStream for '{current_contract_streaming}' error: {error}")


def run_market_data_example(contract_id: str, sub_quotes: bool, sub_trades: bool, sub_depth: bool, duration_seconds: int):
    global current_contract_streaming
    current_contract_streaming = contract_id

    logger.info(f"--- Example: Dedicated Real-Time Market Data Stream ---")
    logger.info(f"Contract: {contract_id}")
    logger.info(f"Subscriptions: Quotes={sub_quotes}, Trades={sub_trades}, Depth={sub_depth}")
    logger.info(f"Running for {duration_seconds} seconds.")

    api_client: Optional[APIClient] = None
    stream: Optional[DataStream] = None

    try:
        logger.info("Authenticating...")
        initial_token, token_acquired_at = authenticate()
        
        api_client = APIClient(
            initial_token=initial_token,
            token_acquired_at=token_acquired_at
        )
        logger.info("APIClient initialized.")

        logger.info(f"Initializing DataStream for contract: {contract_id}")
        stream = DataStream(
            api_client=api_client, # DataStream now uses APIClient for token management
            contract_id_to_subscribe=contract_id,
            on_quote_callback=handle_live_quote if sub_quotes else None,
            on_trade_callback=handle_live_trade if sub_trades else None,
            on_depth_callback=handle_live_depth if sub_depth else None,
            on_state_change_callback=handle_stream_state_change,
            on_error_callback=handle_stream_error
        )
        
        if not stream.start():
            logger.error("Failed to start DataStream. Exiting.")
            return
        logger.info("DataStream started and subscriptions sent (if any). Monitoring for market data...")
        
        end_time = time.monotonic() + duration_seconds
        # Token refresh for streams is now primarily handled by accessTokenFactory within the stream class.
        # An explicit app-level token update loop is less critical for basic stream survival,
        # but might be kept if the application wants to force token alignment or if APIClient's token
        # could change for reasons external to the stream's own lifecycle.
        # For simplicity, this dedicated example will rely on the stream's internal token management.

        while time.monotonic() < end_time:
            logger.debug(f"Main loop: Stream status for {current_contract_streaming}: {stream.connection_status if stream else 'N/A'}")
            time.sleep(5) # Keep main thread alive, check status periodically
        
        logger.info("Example duration finished.")

    except ConfigurationError as e:
        logger.error(f"CONFIGURATION ERROR: {e}")
    except AuthenticationError as e:
        logger.error(f"AUTHENTICATION FAILED: {e}")
    except APIError as e:
        logger.error(f"API ERROR: {e}")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down stream...")
    except Exception as e:
        logger.error(f"AN UNEXPECTED ERROR OCCURRED: {e}", exc_info=True)
    finally:
        if stream:
            logger.info("Stopping DataStream...")
            stream.stop()
        logger.info("Market data stream example finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream real-time market data for a contract.")
    parser.add_argument(
        "--contract_id", 
        type=str, 
        default=DEFAULT_CONFIG_CONTRACT_ID,
        help="Contract ID to subscribe to."
    )
    parser.add_argument(
        "--no_quotes", 
        action="store_false", 
        dest="subscribe_quotes",
        default=True, # Subscribe to quotes by default
        help="Disable real-time quote (best bid/ask) updates."
    )
    parser.add_argument(
        "--trades", 
        action="store_true", 
        help="Subscribe to real-time trade execution updates."
    )
    parser.add_argument(
        "--depth", 
        action="store_true", 
        help="Subscribe to real-time market depth (DOM) updates."
    )
    parser.add_argument(
        "--duration", 
        type=int, 
        default=120, 
        help="How long (in seconds) to run the stream."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG level logging."
    )
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.DEBUG)
        logger.info("DEBUG logging enabled.")
        # logging.getLogger("SignalRCoreClient").setLevel(logging.DEBUG) # For SignalR client verbosity

    if not args.contract_id:
        logger.error("A contract_id must be provided via --contract_id or in .env as CONTRACT_ID.")
        sys.exit(1)
    
    if not (args.subscribe_quotes or args.trades or args.depth):
        logger.warning("No data types selected for subscription (quotes, trades, depth). Stream will connect but show no market data.")

    run_market_data_example(
        contract_id=args.contract_id,
        sub_quotes=args.subscribe_quotes,
        sub_trades=args.trades,
        sub_depth=args.depth,
        duration_seconds=args.duration
    )