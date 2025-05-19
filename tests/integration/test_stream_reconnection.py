# tests/integration/test_stream_reconnection.py
import pytest
import logging
import time
import os 
import threading
from collections import deque
from typing import Deque, Any, List, Dict, Optional

import sys
_src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from tsxapipy import (
    APIClient,
    DataStream,
    authenticate,
    DEFAULT_CONFIG_CONTRACT_ID 
)
from tsxapipy.api.exceptions import AuthenticationError

logger = logging.getLogger("tests.integration.stream_reconnection")

TEST_CONTRACT_ID = DEFAULT_CONFIG_CONTRACT_ID or "CON.F.US.NQ.M25" 
NETWORK_DISCONNECT_DURATION_SECONDS = 25
# Increased timeout for reconnection attempts by signalrcore
RECONNECT_EVENT_WAIT_TIMEOUT_SECONDS = 60 
POST_RECONNECT_DATA_WAIT_SECONDS = 15
INITIAL_CONNECTION_AND_DATA_WAIT_SECONDS = 15 

@pytest.fixture(scope="module")
def authenticated_api_client_for_stream_tests():
    logger.info("Authenticating for stream tests module...")
    try:
        token, acquired_at = authenticate()
        client = APIClient(initial_token=token, token_acquired_at=acquired_at)
        logger.info("APIClient created successfully for stream tests.")
        return client
    except Exception as e:
        logger.error(f"Critical error during module-level auth for stream tests: {e}", exc_info=True)
        pytest.fail(f"Module-level authentication/setup failed: {e}")

@pytest.mark.integration 
@pytest.mark.stream_stability 
class TestDataStreamReconnection:

    @pytest.fixture(autouse=True)
    def setup_test_logging(self, caplog):
        logging.getLogger().setLevel(logging.DEBUG)
        for handler in logging.getLogger().handlers:
            if hasattr(handler, 'setLevel'):
                 handler.setLevel(logging.DEBUG)
        caplog.set_level(logging.DEBUG, logger=logger.name) 
        caplog.set_level(logging.DEBUG, logger="tsxapipy")
        caplog.set_level(logging.DEBUG, logger="SignalRCoreClient") # Crucial for signalrcore internals

    def test_auto_reconnect_after_server_close_and_network_simulation(self, authenticated_api_client_for_stream_tests, caplog):
        api_client = authenticated_api_client_for_stream_tests
        
        received_quotes_after_reconnect: Deque[Dict[str, Any]] = deque(maxlen=10)
        
        initial_connection_event = threading.Event()
        reconnection_event = threading.Event() # Signaled when stream re-enters "connected" AFTER initial connect
        
        last_stream_state_via_callback = {"status": "uninitialized"}
        
        # --- Callbacks ---
        def quote_handler(data: Dict[str, Any]):
            logger.debug(f"TestQuoteHandler: Quote: Px={data.get('bp')}/{data.get('ap')}")
            # Only collect quotes if we believe we are in the post-reconnection phase
            if initial_connection_event.is_set() and reconnection_event.is_set():
                received_quotes_after_reconnect.append(data)
            # Not collecting initial quotes to simplify focus on reconnect

        def state_change_handler(state: str):
            logger.info(f"TestStateChangeHandler: Stream state changed to '{state}' (was '{last_stream_state_via_callback['status']}')")
            last_stream_state_via_callback["status"] = state
            if state == "connected":
                if not initial_connection_event.is_set():
                    logger.info("TestStateChangeHandler: Initial 'connected' state detected by event. Setting initial_connection_event.")
                    initial_connection_event.set()
                elif initial_connection_event.is_set(): # Must be a reconnect if initial already set
                    logger.info("TestStateChangeHandler: Reconnection to 'connected' state detected by event. Setting reconnection_event.")
                    reconnection_event.set()
        
        stream: Optional[DataStream] = None
        try:
            logger.info(f"Test: Initializing DataStream for {TEST_CONTRACT_ID}.")
            stream = DataStream(
                api_client=api_client,
                contract_id_to_subscribe=TEST_CONTRACT_ID,
                on_quote_callback=quote_handler,
                on_state_change_callback=state_change_handler
            )

            logger.info("Test: Starting stream...")
            assert stream.start(), "DataStream failed to initiate start."
            
            logger.info(f"Test: Waiting up to {INITIAL_CONNECTION_AND_DATA_WAIT_SECONDS}s for initial connection...")
            assert initial_connection_event.wait(timeout=INITIAL_CONNECTION_AND_DATA_WAIT_SECONDS), \
                f"Stream did not reach 'connected' state initially. Last state from CB: {last_stream_state_via_callback['status']}"
            
            caplog.clear() # Clear logs before we start monitoring factory calls for reconnect phase

            logger.info(f"Test: Initial connection confirmed. Status is '{stream.connection_status}'. Waiting briefly for server to potentially close connection...")
            time.sleep(5) # Wait for the server's type 7 close message
            logger.info(f"Test: After 5s wait. Status from prop: '{stream.connection_status}', from CB: '{last_stream_state_via_callback['status']}'.")
            # At this point, server has likely sent "Close type 7", signalrcore logged "Connection stop".
            # DataStream status might still be "connected" due to lack of callback from signalrcore.

            # --- Simulate Network Disruption ---
            logger.info("Test: >>> Simulating network interface DOWN <<<")
            print("\n\n>>>> PLEASE MANUALLY DISCONNECT NETWORK INTERFACE NOW <<<<\n\n")
            logger.info(f"Test: Waiting {NETWORK_DISCONNECT_DURATION_SECONDS}s (network simulated down)...")
            time.sleep(NETWORK_DISCONNECT_DURATION_SECONDS)
            logger.info(f"Test: Finished {NETWORK_DISCONNECT_DURATION_SECONDS}s of 'network down'. Status from prop: '{stream.connection_status}', CB: '{last_stream_state_via_callback['status']}'")

            # --- Simulate Network Restoration ---
            logger.info("Test: >>> Simulating network interface UP <<<")
            print("\n\n>>>> PLEASE MANUALLY RECONNECT NETWORK INTERFACE NOW <<<<\n\n")
            
            logger.info(f"Test: Network restored. Waiting up to {RECONNECT_EVENT_WAIT_TIMEOUT_SECONDS}s for stream to report 'connected' state again (via callback)...")
            
            reconnected_successfully = reconnection_event.wait(timeout=RECONNECT_EVENT_WAIT_TIMEOUT_SECONDS)
            
            # Check logs for accessTokenFactory calls AFTER initial connection logs were cleared
            factory_calls_during_reconnect_phase = [rec for rec in caplog.records if "accessTokenFactory: Providing token" in rec.message]
            logger.info(f"Test: Found {len(factory_calls_during_reconnect_phase)} accessTokenFactory calls during/after network outage.")

            assert reconnected_successfully, \
                f"Stream did not signal reconnection (reach 'connected' state again via callback after network outage). Last state from CB: '{last_stream_state_via_callback['status']}'"
            
            assert len(factory_calls_during_reconnect_phase) > 0, \
                "accessTokenFactory was NOT called during the reconnection phase. Signalrcore might not be retrying or factory not used."

            logger.info(f"Test: Stream reconnected. Waiting {POST_RECONNECT_DATA_WAIT_SECONDS}s for new data...")
            time.sleep(POST_RECONNECT_DATA_WAIT_SECONDS) 
            
            assert len(received_quotes_after_reconnect) > 0, \
                "No new quotes received after stream reconnected and subscriptions were re-sent."
            
            logger.info(f"Test: Successfully reconnected and received {len(received_quotes_after_reconnect)} new quotes.")
            assert stream.connection_status == "connected", f"Final stream status from prop is '{stream.connection_status}' (expected 'connected'). Last state from CB: '{last_stream_state_via_callback['status']}'"

        except AssertionError:
            logger.error("Test: Assertion failed.", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Test: An unexpected exception occurred: {e}", exc_info=True)
            pytest.fail(f"Unexpected exception in test: {e}")
        finally:
            logger.info("Test: Cleaning up stream...")
            if stream:
                stream.stop()
            print("\n\n>>>> IF YOU MANUALLY MODIFIED NETWORK, PLEASE ENSURE IT IS RESTORED NOW <<<<\n\n")