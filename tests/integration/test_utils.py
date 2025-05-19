# conftest.py or a test_utils.py

import pytest
from tsxapipy import APIClient, authenticate
from tsxapipy.real_time import DataStream, UserHubStream
# from tsxapipy.config import SOME_VALID_CONTRACT_ID, SOME_VALID_ACCOUNT_ID

@pytest.fixture
def authenticated_api_client():
    token, acquired_at = authenticate() # Assumes .env is set up for tests
    client = APIClient(initial_token=token, token_acquired_at=acquired_at)
    return client

@pytest.fixture
def data_stream_factory(authenticated_api_client):
    def _factory(contract_id, callbacks=None, api_client_override=None):
        client_to_use = api_client_override or authenticated_api_client
        # Stream needs access to APIClient for robust token refresh
        # This implies DataStream might need an api_client parameter in __init__
        stream = DataStream(
            token=client_to_use.current_token,
            contract_id_to_subscribe=contract_id,
            on_quote_callback=callbacks.get("quote") if callbacks else None,
            on_trade_callback=callbacks.get("trade") if callbacks else None,
            # api_client=client_to_use # Hypothetical parameter
        )
        # Critical: The stream needs a reference to the APIClient for the factory pattern
        # if accessTokenFactory is implemented, or for manual refresh during reconnect.
        # For now, let's assume the token is static for the initial connection
        # and we'll test update_token separately or simulate its need.
        return stream
    return _factory

@pytest.fixture
def user_hub_stream_factory(authenticated_api_client):
    def _factory(account_id, callbacks=None, api_client_override=None):
        client_to_use = api_client_override or authenticated_api_client
        stream = UserHubStream(
            token=client_to_use.current_token,
            account_id_to_watch=account_id,
            on_order_update=callbacks.get("order") if callbacks else None,
            # api_client=client_to_use # Hypothetical parameter
        )
        return stream
    return _factory

# Helper to simulate network actions (these are conceptual and OS-dependent)
def simulate_network_disconnect():
    print("SIMULATING NETWORK DISCONNECT (manual action needed or OS-specific command)")
    # e.g., os.system("nmcli networking off") # Linux
    # e.g., os.system("netsh interface set interface name=\"Ethernet\" admin=disabled") # Windows

def simulate_network_reconnect():
    print("SIMULATING NETWORK RECONNECT (manual action needed or OS-specific command)")
    # e.g., os.system("nmcli networking on")
    # e.g., os.system("netsh interface set interface name=\"Ethernet\" admin=enabled")