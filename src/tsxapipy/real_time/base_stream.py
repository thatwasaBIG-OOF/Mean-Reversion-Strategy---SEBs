import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import Optional, Callable, Any, Dict, List, Union

from tsxapipy.api import APIClient
from .stream_state import StreamConnectionState
from signalrcore.hub_connection_builder import HubConnectionBuilder

logger = logging.getLogger(__name__)

class BaseStream:
    """Base class for SignalR stream connections to TopStep hubs."""
    
    def __init__(self, api_client: APIClient, hub_url: str, on_error: Optional[Callable[[Any], None]] = None):
        """
        Initialize the base stream.
        
        Args:
            api_client: APIClient instance for authentication
            hub_url: URL of the SignalR hub
            on_error: Optional callback for error handling
        """
        self.api_client = api_client
        self.hub_url = hub_url
        self.on_error = on_error
        self.connection = None
        self.connection_state = StreamConnectionState.DISCONNECTED
        self.logger = logger
        
        # Initialize other common attributes
        self._connection_lock = threading.RLock()
        self._last_error = None
        self._reconnect_attempt = 0
        self._max_reconnect_attempts = 5
        self._reconnect_delay_seconds = 2
        self._is_shutting_down = False

    def start(self):
        """
        Start the stream connection.
        
        This method:
        1. Gets a current token from the API client
        2. Builds the hub connection with the token
        3. Sets up event handlers
        4. Starts the connection
        
        Raises:
            Exception: If there's an error starting the stream
        """
        try:
            self.logger.info("Starting hub connection...")
            
            # Get current token from API client
            token = self.api_client.token
            
            # Construct the full URL with token
            # Convert https:// to wss:// for WebSocket connections
            hub_url = self.hub_url
            if hub_url.startswith('https://'):
                hub_url = 'wss://' + hub_url[8:]
            
            full_url = f"{hub_url}?access_token={token}"
            
            # Build the hub connection
            self.connection = HubConnectionBuilder()\
                .with_url(full_url, options={"skip_negotiation": True})\
                .build()
            
            # Set up event handlers
            self._setup_connection_events()
            
            # Register hub methods
            self._register_hub_methods()
            
            # Start the connection
            self.connection.start()
            
            # Wait for connection to be established
            self._wait_for_connection()
            
            # Subscribe to data
            self._subscribe_to_data()
            
        except Exception as e:
            self.logger.error(f"Failed to start stream: {e}")
            raise

    def stop(self):
        """
        Stop the stream connection.
        
        This method closes the SignalR connection and cleans up resources.
        """
        with self._connection_lock:
            self._is_shutting_down = True
            
            if not self.connection or self.connection_state == StreamConnectionState.DISCONNECTED:
                self.logger.info("Stream is already disconnected.")
                return
                
            try:
                self.logger.info("Stopping stream connection...")
                self.connection.stop()
                self.connection_state = StreamConnectionState.DISCONNECTED
                self.logger.info("Stream connection stopped successfully")
            except Exception as e:
                self._last_error = e
                self.logger.error(f"Error stopping stream: {e}", exc_info=True)
                if self.on_error:
                    self.on_error(e)
                raise

    def _setup_connection_events(self):
        """Set up event handlers for the connection."""
        def on_open():
            self.logger.info("Connection opened.")
            self._update_state(StreamConnectionState.CONNECTED)
        
        def on_close():
            self.logger.info("Connection closed.")
            self._update_state(StreamConnectionState.DISCONNECTED)
            if self.should_reconnect:
                self._handle_reconnect()
        
        def on_error(error):
            self.logger.error(f"Connection error: {error}")
            self._update_state(StreamConnectionState.ERROR)
            self._handle_connection_error(error)
        
        def on_reconnecting():
            self.logger.info("Connection reconnecting.")
            self._update_state(StreamConnectionState.RECONNECTING_UNEXPECTED)
        
        # Set up event handlers
        if hasattr(self.connection, 'on_open'):
            self.connection.on_open(on_open)
        
        if hasattr(self.connection, 'on_close'):
            self.connection.on_close(on_close)
        
        if hasattr(self.connection, 'on_error'):
            self.connection.on_error(on_error)
        
        if hasattr(self.connection, 'on_reconnecting'):
            self.connection.on_reconnecting(on_reconnecting)

    def _setup_stream_specific(self):
        """
        Set up stream-specific handlers and subscriptions.
        
        This method should be overridden by subclasses to set up their specific
        event handlers and subscriptions.
        """
        pass  # To be implemented by subclasses

    def _set_state(self, new_state: StreamConnectionState):
        """
        Set the connection state and call the state change callback if provided.
        
        Args:
            new_state: The new connection state
        """
        old_state = self.state
        self.state = new_state
        
        if old_state != new_state and self.on_state_change_callback:
            try:
                self.on_state_change_callback(old_state, new_state)
            except Exception as e:
                self.logger.error(f"Error in state change callback: {e}")

    def _register_hub_methods(self):
        """Register hub methods for the connection."""
        # This method should be implemented by subclasses
        raise NotImplementedError("Subclasses must implement _register_hub_methods")









