# test_stream_reconnect_clean.py
import pytest
import time
from collections import deque
# from tsxapipy.config import SOME_VALID_CONTRACT_ID # Assume this is configured

@pytest.mark.integration
def test_data_stream_clean_reconnect_resubscribes(data_stream_factory):
    SOME_VALID_CONTRACT_ID = "CON.F.US.NQ.M25" # Example
    received_quotes = deque(maxlen=5)
    def quote_handler(data):
        print(f"Received quote: {data}")
        received_quotes.append(data)

    stream = data_stream_factory(SOME_VALID_CONTRACT_ID, {"quote": quote_handler})
    
    assert stream.start(), "Stream failed to start initially"
    time.sleep(10) # Allow time for connection and first data
    assert len(received_quotes) > 0, "No quotes received on initial connection"
    print(f"Initial connection: {len(received_quotes)} quotes received.")
    initial_quote_count = len(received_quotes)
    received_quotes.clear()

    stream.stop()
    print("Stream stopped. Waiting briefly before restart...")
    time.sleep(3)
    
    assert stream.start(), "Stream failed to restart"
    print("Stream restarted. Waiting for data after reconnect...")
    time.sleep(10) # Allow time for reconnection and new data
    
    assert len(received_quotes) > 0, "No quotes received after clean reconnect and restart"
    print(f"After reconnect: {len(received_quotes)} quotes received.")
    # Optionally assert that new quotes are different if market is active 