# examples/15_doc_aligned_market_stream.py

import sys
import os
# Ensure the 'src' directory (containing 'tsxapipy') is on the path
SRC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)
    print(f"Added to sys.path for example: {SRC_PATH}")

import logging
import time
import pprint
import argparse
from typing import Any, Optional, List, Dict 

from tsxapipy import (
    authenticate,
    AuthenticationError,
    ConfigurationError,
    APIError,
    LibraryError, 
    DEFAULT_CONFIG_CONTRACT_ID,
)
from tsxapipy.api.exceptions import APIResponseParsingError 
from tsxapipy.config import MARKET_HUB_URL 
from signalrcore.hub_connection_builder import HubConnectionBuilder

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("DocAlignedMarketStreamExample")

# --- Globals for Callbacks ---
current_contract_testing: Optional[str] = None
connection_global: Optional[HubConnectionBuilder] = None 
is_stream_connected: bool = False 
cli_args = None # Will be populated by __main__

# --- Callbacks for direct SignalR connection ---
def handle_doc_aligned_quote(message_args: List[Any]):
    global current_contract_testing
    if not isinstance(message_args, list) or len(message_args) < 2:
        logger.error(f"GatewayQuote: Unexpected message_args format: {message_args}")
        return
    
    contract_id_from_event = message_args[0]
    quote_data = message_args[1]

    if not isinstance(contract_id_from_event, str) or not isinstance(quote_data, dict):
        logger.error(f"GatewayQuote: Unexpected types in message_args. ContractID: {type(contract_id_from_event)}, Data: {type(quote_data)}")
        return

    if contract_id_from_event == current_contract_testing:
        logger.info(f"--- DOC ALIGNED QUOTE ({current_contract_testing}) ---")
        logger.info(pprint.pformat(quote_data, indent=1, width=100, compact=True))
    else:
        logger.warning(f"Received quote for unexpected contract: {contract_id_from_event}, expecting: {current_contract_testing}")

def handle_doc_aligned_trade(message_args: List[Any]):
    global current_contract_testing
    if not isinstance(message_args, list) or len(message_args) < 2:
        logger.error(f"GatewayTrade: Unexpected message_args format: {message_args}")
        return

    contract_id_from_event = message_args[0]
    trade_data = message_args[1]

    if not isinstance(contract_id_from_event, str) or not isinstance(trade_data, dict):
        logger.error(f"GatewayTrade: Unexpected types in message_args. ContractID: {type(contract_id_from_event)}, Data: {type(trade_data)}")
        return

    if contract_id_from_event == current_contract_testing:
        logger.info(f"--- DOC ALIGNED TRADE ({current_contract_testing}) ---")
        logger.info(pprint.pformat(trade_data, indent=1, width=100, compact=True))
    else:
        logger.warning(f"Received trade for unexpected contract: {contract_id_from_event}, expecting: {current_contract_testing}")

def handle_doc_aligned_depth(message_args: List[Any]): 
    global current_contract_testing
    # Log the raw entire message_args for GatewayDepth first
    logger.info(f"--- RAW GatewayDepth message_args ({current_contract_testing}): {message_args} ---")

    if not isinstance(message_args, list) or len(message_args) < 2:
        logger.error(f"GatewayDepth: Unexpected message_args format (not list or too short): {message_args}")
        return
    
    contract_id_from_event = message_args[0]
    raw_depth_payload = message_args[1] # This is what we need to inspect

    # Log the type and a preview of the raw_depth_payload
    logger.info(f"GatewayDepth: contract_id_from_event: '{contract_id_from_event}' (type: {type(contract_id_from_event)})")
    logger.info(f"GatewayDepth: raw_depth_payload type: {type(raw_depth_payload)}")
    
    if isinstance(raw_depth_payload, list):
        logger.info(f"GatewayDepth: raw_depth_payload (list) is a list. Length: {len(raw_depth_payload)}")
        logger.info(f"GatewayDepth: raw_depth_payload (list) preview (first 3 items if >3 items): {raw_depth_payload[:3]}")
        # Example: if each item in the list is a dict {'p': price, 'v': volume, 's': side_int (0=bid, 1=ask), 't': type_int(0=new,1=update,2=delete)}
        # You would iterate through raw_depth_payload
    elif isinstance(raw_depth_payload, dict):
         logger.info(f"GatewayDepth: raw_depth_payload (dict) keys: {list(raw_depth_payload.keys())}")
         # Example: if it's a dict like {'bids': [[price,size],...], 'asks': [[price,size],...]}
         # bids = raw_depth_payload.get('bids', [])
         # asks = raw_depth_payload.get('asks', [])
         # logger.info(f"  Top 3 Bids (from dict): {bids[:3]}")
         # logger.info(f"  Top 3 Asks (from dict): {asks[:3]}")
    else:
        logger.info(f"GatewayDepth: raw_depth_payload (other type): {str(raw_depth_payload)[:200]}") # Log a preview

    # Keep the original check for contract_id type for now
    if not isinstance(contract_id_from_event, str):
        logger.error(f"GatewayDepth: Unexpected type for contract_id_from_event: {type(contract_id_from_event)}")
        return

    # The original logic expecting raw_depth_payload to be a dict is now conditional
    if contract_id_from_event == current_contract_testing:
        if isinstance(raw_depth_payload, dict): # If it IS a dictionary as previously expected
            logger.info(f"--- DOC ALIGNED DEPTH (DICT FORMAT) ({current_contract_testing}) ---")
            logger.info(pprint.pformat(raw_depth_payload, indent=1, width=100, compact=True))
        elif isinstance(raw_depth_payload, list): # If it's a list
             logger.info(f"--- DOC ALIGNED DEPTH (LIST FORMAT) ({current_contract_testing}) ---")
             # For now, just print the list. Further processing depends on its internal structure.
             logger.info(pprint.pformat(raw_depth_payload, indent=1, width=100, compact=True))
        # Add more specific handling once the structure of raw_depth_payload (when it's a list) is known
        # e.g. if it's a list of level updates, you might iterate and print each.
    else:
        logger.warning(f"Received depth for unexpected contract: {contract_id_from_event}, expecting: {current_contract_testing}")


def on_doc_aligned_open():
    global connection_global, current_contract_testing, is_stream_connected, cli_args 
    is_stream_connected = True 
    logger.info(f"DocAlignedStream: CONNECTION OPENED. Sending subscriptions for {current_contract_testing}...")
    if connection_global and current_contract_testing and cli_args: 
        try:
            if cli_args.subscribe_quotes: 
                logger.info(f"DocAlignedStream: Sending 'SubscribeContractQuotes' for {current_contract_testing}")
                connection_global.send('SubscribeContractQuotes', [current_contract_testing]) 
                time.sleep(0.1) 
            
            if cli_args.subscribe_depth: 
                logger.info(f"DocAlignedStream: Sending 'SubscribeContractMarketDepth' for {current_contract_testing}")
                connection_global.send('SubscribeContractMarketDepth', [current_contract_testing])
                time.sleep(0.1)

            if cli_args.subscribe_trades:
                logger.warning("DocAlignedStream: JS Market Hub example doesn't show explicit trade subscription. "
                               "Relying on 'GatewayTrade' event listener only. If no trades, check API docs for Market Hub trade subscription method.")
            logger.info(f"DocAlignedStream: Subscription messages sent for {current_contract_testing}.")

        except Exception as e_sub:
            logger.error(f"DocAlignedStream: Error sending subscriptions: {e_sub}", exc_info=True)

def on_doc_aligned_close():
    global is_stream_connected 
    is_stream_connected = False 
    logger.info("DocAlignedStream: CONNECTION CLOSED.")

def on_doc_aligned_error(error: Any):
    global is_stream_connected 
    is_stream_connected = False 
    logger.error(f"DocAlignedStream: CONNECTION ERROR: {error}", exc_info=isinstance(error, Exception))

def run_doc_aligned_stream_example(
    contract_id_arg: str, 
    duration_seconds: int
):
    global current_contract_testing, connection_global, is_stream_connected, cli_args

    current_contract_testing = contract_id_arg
    
    logger.info(f"--- Example 15: Documentation-Aligned Market Data Stream ---")
    logger.info(f"Target Contract: {contract_id_arg}")
    if cli_args: 
        logger.info(f"Subscriptions: Quotes={cli_args.subscribe_quotes}, Trades={cli_args.subscribe_trades}, Depth={cli_args.subscribe_depth}")
    logger.info(f"Running for {duration_seconds} seconds.")

    initial_token: Optional[str] = None
    is_stream_connected = False 

    try:
        logger.info("Authenticating to get session token...")
        auth_token_tuple = authenticate() 
        if not auth_token_tuple or not auth_token_tuple[0]:
            logger.error("Authentication failed: No token received. Cannot proceed.")
            return
        initial_token = auth_token_tuple[0]
        logger.info("Authentication successful, token obtained.")

        base_wss_url = ""
        if MARKET_HUB_URL.startswith("https://"):
            base_wss_url = "wss://" + MARKET_HUB_URL[len("https://"):]
        elif MARKET_HUB_URL.startswith("http://"):
            base_wss_url = "ws://" + MARKET_HUB_URL[len("http://"):]
        else:
            logger.error(f"Cannot determine WSS base from MARKET_HUB_URL: {MARKET_HUB_URL}")
            return
        
        hub_url_with_token_query = f"{base_wss_url}?access_token={initial_token}"
        logger.info(f"Attempting to connect to SignalR Hub: {base_wss_url}?access_token=TOKEN_HIDDEN")

        connection_global = HubConnectionBuilder() \
            .with_url(hub_url_with_token_query, options={
                "verify_ssl": True, 
                "skip_negotiation": True, 
            }) \
            .with_automatic_reconnect({
                "type": "interval",
                "keep_alive_interval": 10,
                "intervals": [0, 2, 5, 10, 15, 30, 60, 120]
            }) \
            .build()

        logger.debug("SignalR HubConnectionBuilder configured.")

        connection_global.on_open(on_doc_aligned_open)
        connection_global.on_close(on_doc_aligned_close)
        connection_global.on_error(on_doc_aligned_error)
        
        if cli_args: 
            if cli_args.subscribe_quotes:
                connection_global.on("GatewayQuote", handle_doc_aligned_quote)
                logger.debug("Registered handler for 'GatewayQuote'")
            if cli_args.subscribe_trades:
                connection_global.on("GatewayTrade", handle_doc_aligned_trade)
                logger.debug("Registered handler for 'GatewayTrade'")
            if cli_args.subscribe_depth:
                connection_global.on("GatewayDepth", handle_doc_aligned_depth)
                logger.debug("Registered handler for 'GatewayDepth'")

        logger.info("Attempting to start SignalR connection...")
        connection_global.start()
        logger.info("SignalR connection.start() called. Connection is asynchronous.")
        
        end_time = time.monotonic() + duration_seconds
        
        while time.monotonic() < end_time:
            if is_stream_connected: 
                logger.debug(f"Main loop: Stream status for {current_contract_testing}: CONNECTED (is_stream_connected=True)")
            elif not connection_global: 
                logger.error("SignalR connection object is None after start. Exiting loop.")
                break
            time.sleep(1)
        
        logger.info("Example duration finished or connection loop exited.")

    except ConfigurationError as e_conf:
        logger.error(f"CONFIGURATION ERROR: {e_conf}")
    except AuthenticationError as e_auth:
        logger.error(f"AUTHENTICATION FAILED: {e_auth}")
    except APIResponseParsingError as e_parse: 
        logger.error(f"API RESPONSE PARSING ERROR (likely during auth): {e_parse}")
    except APIError as e_api:
        logger.error(f"API ERROR: {e_api}")
    except LibraryError as e_lib:
        logger.error(f"LIBRARY ERROR: {e_lib}")
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down...")
    except Exception as e_generic: 
        logger.error(f"AN UNEXPECTED ERROR OCCURRED: {e_generic}", exc_info=True)
    finally:
        if connection_global:
            logger.info(f"Attempting to stop SignalR connection (is_stream_connected={is_stream_connected})...")
            try:
                connection_global.stop()
                logger.info("SignalR connection.stop() called.")
            except Exception as e_stop:
                logger.error(f"Error stopping SignalR connection: {e_stop}", exc_info=True)
        logger.info("Documentation-aligned market stream example finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test real-time market data stream, aligning with API documentation examples.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--contract_id", type=str, default=DEFAULT_CONFIG_CONTRACT_ID or "CON.F.US.MNQ.M25", 
        help=f"Contract ID to subscribe to. Default from config: {DEFAULT_CONFIG_CONTRACT_ID or '(Not Set, using MNQ.M25 example)'}"
    )
    parser.add_argument(
        "--quotes", action="store_true", default=True, dest="subscribe_quotes",
        help="Subscribe to quote updates (SubscribeContractQuotes)."
    )
    parser.add_argument(
        "--no-quotes", action="store_false", dest="subscribe_quotes",
        help="Disable quote subscription."
    )
    parser.add_argument("--trades", action="store_true", dest="subscribe_trades",
                        help="Listen for GatewayTrade events (explicit Market Hub subscription method TBD from docs).")
    parser.add_argument("--depth", action="store_true", dest="subscribe_depth",
                        help="Subscribe to market depth updates (SubscribeContractMarketDepth).")
    parser.add_argument(
        "--duration", type=int, default=60, 
        help="How long (in seconds) to run the stream."
    )
    parser.add_argument(
        "--debug_signalr", action="store_true",
        help="Enable DEBUG level logging for 'signalrcore' library."
    )
    parser.add_argument(
        "--debug_all", action="store_true",
        help="Enable DEBUG level logging for all modules (includes script and tsxapipy)."
    )
    
    cli_args = parser.parse_args() 

    if cli_args.debug_all:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("DEBUG logging enabled for all modules.")
    if cli_args.debug_signalr:
        logging.getLogger("signalrcore").setLevel(logging.DEBUG)
        logger.info("DEBUG logging enabled for 'signalrcore' library.")

    if not cli_args.contract_id:
        logger.error("No Contract ID specified. Provide --contract_id or set CONTRACT_ID in .env.")
        sys.exit(1)
    
    if not (cli_args.subscribe_quotes or cli_args.subscribe_trades or cli_args.subscribe_depth):
        logger.warning("No data types selected for subscription/listening. Stream will connect but may show little activity.")

    run_doc_aligned_stream_example(
        contract_id_arg=cli_args.contract_id,
        duration_seconds=cli_args.duration
    )