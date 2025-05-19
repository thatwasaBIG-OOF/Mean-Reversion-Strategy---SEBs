# test_stream_start_invalid_token.py
import pytest
import time
# from tsxapipy.config import SOME_VALID_CONTRACT_ID

@pytest.mark.integration
def test_data_stream_start_with_invalid_token(data_stream_factory):
    SOME_VALID_CONTRACT_ID = "CON.F.US.NQ.M25" # Example
    error_hit = False
    received_data = False

    def quote_handler(data):
        nonlocal received_data
        received_data = True
        print("UNEXPECTED: Quote received with invalid token!")

    def error_handler(err): # Assuming stream objects can have an on_general_error callback
        nonlocal error_hit
        error_hit = True
        print(f"Stream error_handler called: {err}")

    # Create a dummy APIClient that returns a bad token
    class MockAPIClient:
        @property
        def current_token(self): return "THIS_IS_AN_INVALID_TOKEN_STRING"
    
    # Pass this mock client if data_stream_factory uses it for token
    # Otherwise, pass the bad token directly to DataStream constructor
    stream = DataStream(
        token="THIS_IS_AN_INVALID_TOKEN_STRING",
        contract_id_to_subscribe=SOME_VALID_CONTRACT_ID,
        on_quote_callback=quote_handler
        # on_error_callback=error_handler # Add if stream class supports general error callback
    )
    # Also set the stream's _on_error if testing that internal path
    stream._on_error_user_callback = error_handler # Example if it has such an attribute for testing

    start_result = stream.start()
    
    print(f"stream.start() returned: {start_result}. Waiting a bit to see if errors or data occur (10s)...")
    time.sleep(10) # Give time for connection attempt to fail

    assert not received_data, "Data was received despite using an invalid token"
    # The assertion below depends on how failure is reported:
    # Option 1: start() returns False on immediate auth failure (less likely for async)
    # Option 2: An error callback is hit
    # Option 3: Stream just silently fails to connect/subscribe and no data flows.
    # For now, we check that no data was received. A more robust test would check error_hit or start_result.
    if start_result: # If start() didn't immediately return False
         assert error_hit, "Stream started (or appeared to) with invalid token but no error callback was triggered"
    
    stream.stop()