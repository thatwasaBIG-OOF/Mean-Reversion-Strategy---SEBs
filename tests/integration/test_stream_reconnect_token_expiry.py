# test_stream_reconnect_token_expiry.py
import pytest
import time
from datetime import timedelta
from collections import deque
# from tsxapipy.config import SOME_VALID_CONTRACT_ID, TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES, DEFAULT_TOKEN_LIFETIME_HOURS
# from tsxapipy.common.time_utils import UTC_TZ
# from .test_utils import simulate_network_disconnect, simulate_network_reconnect

@pytest.mark.integration
@pytest.mark.manual_network_simulation
def test_data_stream_reconnect_with_token_refresh(authenticated_api_client, data_stream_factory):
    SOME_VALID_CONTRACT_ID = "CON.F.US.NQ.M25" # Example
    # For this test to be effective, the APIClient's token MUST be refreshed
    # while the stream is "disconnected".

    # --- Phase 1: Initial Connection ---
    received_quotes = deque()
    def quote_handler(data):
        print(f"Quote Handler (Token Test): {data.get('bp')}/{data.get('ap')}")
        received_quotes.append(data)

    # Assume DataStream is modified to accept api_client for token factory or manual refresh
    # For now, we'll test the manual update_token path
    stream = data_stream_factory(SOME_VALID_CONTRACT_ID, {"quote": quote_handler}, api_client_override=authenticated_api_client)
    
    assert stream.start(), "Stream failed to start initially"
    print("Waiting for initial data (up to 15s)...")
    time.sleep(15)
    assert len(received_quotes) > 0, "No data on initial connection for token expiry test"
    initial_token_stream_used = stream.token # Assuming stream.token holds the active token
    print(f"Initial stream token (first 15 chars): {initial_token_stream_used[:15]}...")
    received_quotes.clear()

    # --- Phase 2: Simulate Disconnect & Token Expiry/Refresh for APIClient ---
    print("SIMULATING NETWORK DISCONNECT for stream...")
    # simulate_network_disconnect() # Manual action
    print("Please manually disconnect network now. Waiting 20 seconds for stream to notice...")
    time.sleep(20) # Allow stream to detect disconnect

    print("Forcing APIClient token to be 'stale' and then refreshing it...")
    # Modify APIClient's internal state to simulate it's very old
    # This is a bit intrusive; ideally, you'd have a mockable time or short token lifetime
    very_old_time = authenticated_api_client._token_acquired_at - timedelta(hours=authenticated_api_client._token_lifetime.total_seconds() / 3600 + 1)
    authenticated_api_client._token_acquired_at = very_old_time 
    print(f"Modified APIClient token_acquired_at to: {authenticated_api_client._token_acquired_at.isoformat()}")
    
    new_api_client_token = authenticated_api_client.current_token # This will trigger re-authentication
    print(f"APIClient has re-authenticated. New token (first 15): {new_api_client_token[:15]}...")
    assert new_api_client_token != initial_token_stream_used, "APIClient token did not change after forced refresh"

    # --- Phase 3: Simulate Network Reconnect for Stream ---
    print("SIMULATING NETWORK RECONNECT for stream...")
    # simulate_network_reconnect() # Manual action
    print("Please manually reconnect network now.")
    print("Stream should attempt to auto-reconnect. Application logic might need to update its token.")

    # HOW TO TEST THE TOKEN UPDATE:
    # 1. If accessTokenFactory: It should use `new_api_client_token` automatically. Harder to assert directly without deep logging/mocking.
    # 2. If manual update_token via on_reconnecting/on_error:
    #    This test implies the stream class *itself* would call api_client.current_token upon certain reconnect events.
    #    For this test, we'll assume the *application* (this test script) is responsible,
    #    or we explicitly call update_token IF the stream's internal logic doesn't handle it.
    
    # For this test, let's assume the stream's automatic reconnect might fail with the old token.
    # We wait to see. If it doesn't reconnect on its own with a new token, we manually update.
    print("Waiting up to 60s for stream to auto-reconnect (potentially with old token first)...")
    reconnected_with_new_data = False
    reconnect_loop_start_time = time.monotonic()
    while time.monotonic() - reconnect_loop_start_time < 60:
        if len(received_quotes) > 0:
            reconnected_with_new_data = True
            print("Data received after network restore! Verifying stream token.")
            # Check if stream.token was updated by an internal mechanism (e.g., if it had access to APIClient)
            # This part is tricky without knowing the stream's internal token refresh logic.
            # For now, we assume it might still have the old token if no advanced factory exists.
            break
        time.sleep(1)

    if not reconnected_with_new_data or stream.token == initial_token_stream_used:
        print("Stream either did not reconnect OR reconnected with the old token. Manually updating token...")
        stream.update_token(new_api_client_token) # Manually trigger the update
        print("stream.update_token() called. Waiting for data with new token (up to 20s)...")
        time.sleep(20) # Allow time for stop/start with new token
    
    assert len(received_quotes) > 0, "No data received even after potential manual token update and reconnect"
    assert stream.token == new_api_client_token, "Stream token was not updated to the new APIClient token"
    print("Successfully received data, and stream token matches refreshed APIClient token.")
    
    stream.stop()