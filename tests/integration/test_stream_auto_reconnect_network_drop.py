# test_stream_auto_reconnect_network_drop.py
# This test is harder to fully automate without OS-level scripting or mocks
import pytest
import time
from collections import deque
# from tsxapipy.config import SOME_VALID_CONTRACT_ID
# from .test_utils import simulate_network_disconnect, simulate_network_reconnect # Conceptual

@pytest.mark.integration
@pytest.mark.manual_network_simulation # Mark that this might need manual steps
def test_data_stream_auto_reconnect_after_network_drop(data_stream_factory):
    SOME_VALID_CONTRACT_ID = "CON.F.US.NQ.M25" # Example
    received_quotes = deque(maxlen=10)
    initial_connection_successful = False
    reconnection_successful_after_drop = False

    def quote_handler(data):
        nonlocal initial_connection_successful, reconnection_successful_after_drop
        print(f"Test Quote Handler: Received quote - {data.get('bp')}/{data.get('ap')}")
        received_quotes.append(data)
        if not initial_connection_successful: # First batch of data
            initial_connection_successful = True
        elif initial_connection_successful and not reconnection_successful_after_drop:
             # This assumes data flow indicates successful reconnection AFTER the drop phase
             reconnection_successful_after_drop = True


    stream = data_stream_factory(SOME_VALID_CONTRACT_ID, {"quote": quote_handler})
    assert stream.start(), "Stream failed to start"
    
    print("Waiting for initial connection and data (up to 20s)...")
    for _ in range(20):
        if initial_connection_successful: break
        time.sleep(1)
    assert initial_connection_successful, "Failed to receive data on initial connection"
    
    print("Initial data received. SIMULATING NETWORK DISCONNECT (manually or via script)...")
    # simulate_network_disconnect() # Conceptual call
    print("Please manually disconnect network now. Waiting 30 seconds...")
    time.sleep(30) # Time for disconnect to be detected and retries to start
    
    print("SIMULATING NETWORK RECONNECT (manually or via script)...")
    # simulate_network_reconnect() # Conceptual call
    print("Please manually reconnect network now. Waiting for auto-reconnect and data (up to 60s)...")
    
    received_quotes.clear() # Clear old quotes
    reconnect_wait_start = time.monotonic()
    while time.monotonic() - reconnect_wait_start < 60: # Wait up to 60s for reconnect + data
        if reconnection_successful_after_drop and len(received_quotes) > 0:
            print("Data received after (simulated) network drop and auto-reconnect!")
            break
        time.sleep(1)
    
    assert reconnection_successful_after_drop and len(received_quotes) > 0, \
        "Stream did not auto-reconnect and resume data flow after simulated network drop"
    
    stream.stop()