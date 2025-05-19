# topstep_data_suite/src/tsxapipy/trading/__init__.py
"""
The trading package contains modules related to trading logic,
technical indicators, and order management.
"""
from .indicators import (
    simple_moving_average,
    exponential_moving_average
    # Add other indicators here like:
    # average_true_range,
    # bollinger_bands,
)
from .logic import decide_trade
from .order_handler import (
    OrderPlacer,
    place_order_simulated,
    ORDER_TYPES_MAP,
    ORDER_SIDES_MAP,
    ORDER_STATUS_TO_STRING_MAP,
    ORDER_STATUS_PENDING_NEW,
    ORDER_STATUS_NEW,
    ORDER_STATUS_WORKING,
    ORDER_STATUS_PARTIALLY_FILLED,
    ORDER_STATUS_FILLED,
    ORDER_STATUS_PENDING_CANCEL,
    ORDER_STATUS_CANCELLED,
    ORDER_STATUS_REJECTED,
    ORDER_STATUS_EXPIRED,
    ORDER_STATUS_UNKNOWN
)

__all__ = [
    # From indicators.py
    "simple_moving_average",
    "exponential_moving_average",
    # From logic.py
    "decide_trade",
    # From order_handler.py
    "OrderPlacer",
    "place_order_simulated",
    "ORDER_TYPES_MAP",
    "ORDER_SIDES_MAP",
    "ORDER_STATUS_TO_STRING_MAP",
    "ORDER_STATUS_PENDING_NEW",
    "ORDER_STATUS_NEW",
    "ORDER_STATUS_WORKING",
    "ORDER_STATUS_PARTIALLY_FILLED",
    "ORDER_STATUS_FILLED",
    "ORDER_STATUS_PENDING_CANCEL",
    "ORDER_STATUS_CANCELLED",
    "ORDER_STATUS_REJECTED",
    "ORDER_STATUS_EXPIRED",
    "ORDER_STATUS_UNKNOWN",
]