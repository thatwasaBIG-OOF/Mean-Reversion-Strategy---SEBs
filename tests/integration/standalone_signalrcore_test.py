# standalone_signalrcore_test.py
import logging
import time
from signalrcore.hub_connection_builder import HubConnectionBuilder

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("STANDALONE_TEST")

# PASTE YOUR VALID JWT TOKEN HERE
JWT_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1laWRlbnRpZmllciI6IjEyNjcxNiIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL3NpZCI6IjE3NjI0MDE0LTRjZjgtNGNhMi1hMmY0LWRjNzkzZDU4NDQ0YiIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL25hbWUiOiJjYXk3bWFuIiwiaHR0cDovL3NjaGVtYXMubWljcm9zb2Z0LmNvbS93cy8yMDA4LzA2L2lkZW50aXR5L2NsYWltcy9yb2xlIjoidXNlciIsImh0dHA6Ly9zY2hlbWFzLm1pY3Jvc29mdC5jb20vd3MvMjAwOC8wNi9pZGVudGl0eS9jbGFpbXMvYXV0aGVudGljYXRpb25tZXRob2QiOiJhcGkta2V5IiwibXNkIjpbIkNNRUdST1VQX1RPQiIsIkNNRV9UT0IiXSwibWZhIjoidmVyaWZpZWQiLCJleHAiOjE3NDc3MTM3MDR9.vrjFURs8X2RcHDS7xWipNi9zHcLFKO61thCXhPbpjWk"
CONTRACT_ID = "CON.F.US.NQ.H24" # Or your test contract
# Ensure this is the WSS URL
HUB_URL = "wss://rtc.topstepx.com/hubs/market" 

def token_factory():
    logger.debug(f"Factory providing token: {JWT_TOKEN[:20]}...")
    return JWT_TOKEN

connection = HubConnectionBuilder() \
    .with_url(HUB_URL, options={
        "access_token_factory": token_factory,
        "skip_negotiation": True,
        "headers": {"User-Agent": "StandaloneSignalRCoreTest/1.0"}
    }) \
    .configure_logging(logging.DEBUG) \
    .with_automatic_reconnect({
        "type": "interval",
        "keep_alive_interval": 10,
        "intervals": [1000, 2000, 5000]
    }) \
    .build()

def on_open():
    logger.info("Standalone Connection OPEN. Sending subscription...")
    try:
        connection.send("SubscribeContractQuotes", [CONTRACT_ID])
        logger.info(f"Sent SubscribeContractQuotes for {CONTRACT_ID}")
    except Exception as e:
        logger.error(f"Error sending subscription: {e}", exc_info=True)

def on_close():
    logger.info("Standalone Connection CLOSED.")

def on_error(err):
    logger.error(f"Standalone Connection ERROR: {err}")

def on_quote(args): # Assuming payload structure if it ever arrives
    logger.info(f"Standalone Quote Received: {args}")

connection.on_open(on_open)
connection.on_close(on_close)
connection.on_error(on_error)
connection.on("GatewayQuote", on_quote) # Register handler

logger.info("Starting standalone connection...")
try:
    connection.start()
    input("Standalone connection started. Press Enter to stop and exit...\n")
except Exception as e:
    logger.error(f"Error starting connection: {e}", exc_info=True)
finally:
    logger.info("Stopping standalone connection.")
    connection.stop()