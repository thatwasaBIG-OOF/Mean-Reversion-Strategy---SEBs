"""
Stream state module for tsxapipy.

This module provides the StreamConnectionState enum for tracking the state of stream connections.
"""

from enum import Enum, auto

class StreamConnectionState(Enum):
    """Enum for stream connection states."""
    NOT_INITIALIZED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    DISCONNECTED = auto()
    RECONNECTING_TOKEN = auto()
    RECONNECTING_UNEXPECTED = auto()
    ERROR = auto()
    STOPPING = auto()

