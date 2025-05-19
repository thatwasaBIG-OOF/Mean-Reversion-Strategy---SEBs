# tsxapipy/__init__.py
"""
tsxapipy: A Python client library for the TopStep API.

This library provides tools for authentication, API interaction,
historical data management, real-time data streaming, and basic trading utilities.
"""
import logging

# Standard library imports first

# Then third-party imports (none in this specific __init__ directly)

# Then first-party/local imports
from .auth import authenticate
from .api.client import (
    APIClient,
    MAX_BARS_PER_REQUEST
)
from .api.contract_utils import get_futures_contract_details
from .api.exceptions import (
    LibraryError,
    ConfigurationError,
    APIError,
    AuthenticationError,
    APITimeoutError,
    APIHttpError,
    APIResponseError,
    ContractNotFoundError,
    InvalidParameterError,
    RateLimitExceededError,
    InsufficientFundsError,
    OrderNotFoundError,
    OrderRejectedError,
    MaxPositionLimitError,
    MarketClosedError
)
from .common.time_utils import UTC_TZ
from .config import (
    API_URL,
    MARKET_HUB_URL,
    USER_HUB_URL,
    TRADING_ENV_SETTING,
    CONTRACT_ID as DEFAULT_CONFIG_CONTRACT_ID,
    ACCOUNT_ID_TO_WATCH as DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH,
    TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES,
    DEFAULT_TOKEN_LIFETIME_HOURS
)
from .historical.gap_detector import find_missing_trading_days
from .historical.parquet_handler import (
    get_last_timestamp_from_parquet,
    append_bars_to_parquet,
    ParquetHandlerError,
    ParquetReadError,
    ParquetWriteError
)
from .historical.updater import (
    HistoricalDataUpdater,
    calculate_next_interval_start
)
from .real_time import DataStream, UserHubStream
from .trading.indicators import (
    simple_moving_average,
    exponential_moving_average
)
from .trading.logic import decide_trade
from .trading.order_handler import (
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


# Configure a NullHandler for the library's root logger.
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Version of the library - KEEP THIS UPDATED!
# Assuming a version bump for recent refactoring and error handling improvements.
__version__ = "0.4.0"

# Selective exposure of key components for easier top-level imports.
# This defines the public API of the top-level 'tsxapipy' package.


__all__ = [
    # Version
    "__version__",

    # from auth.py
    "authenticate",

    # from api.exceptions.py (via api.__init__)
    "LibraryError",
    "ConfigurationError",
    "APIError",
    "AuthenticationError",
    "APITimeoutError",
    "APIHttpError",
    "APIResponseError",
    "ContractNotFoundError",
    "InvalidParameterError",
    "RateLimitExceededError",
    "InsufficientFundsError",
    "OrderNotFoundError",
    "OrderRejectedError",
    "MaxPositionLimitError",
    "MarketClosedError",

    # from api.client.py (via api.__init__)
    "APIClient",
    "MAX_BARS_PER_REQUEST",

    # from api.contract_utils.py (via api.__init__)
    "get_futures_contract_details",

    # from historical.parquet_handler.py (via historical.__init__)
    "get_last_timestamp_from_parquet",
    "append_bars_to_parquet",
    "ParquetHandlerError",
    "ParquetReadError",
    "ParquetWriteError",

    # from historical.gap_detector.py (via historical.__init__)
    "find_missing_trading_days",

    # from historical.updater.py (via historical.__init__)
    "HistoricalDataUpdater",
    "calculate_next_interval_start",

    # from real_time package (via real_time.__init__)
    "DataStream",
    "UserHubStream",

    # from trading.indicators.py (via trading.__init__)
    "simple_moving_average",
    "exponential_moving_average",

    # from trading.logic.py (via trading.__init__)
    "decide_trade",

    # from trading.order_handler.py (via trading.__init__)
    "OrderPlacer",
    "place_order_simulated",
    "ORDER_TYPES_MAP",
    "ORDER_SIDES_MAP",
    "ORDER_STATUS_TO_STRING_MAP",
    "ORDER_STATUS_PENDING_NEW", "ORDER_STATUS_NEW", "ORDER_STATUS_WORKING",
    "ORDER_STATUS_PARTIALLY_FILLED", "ORDER_STATUS_FILLED",
    "ORDER_STATUS_PENDING_CANCEL", "ORDER_STATUS_CANCELLED",
    "ORDER_STATUS_REJECTED", "ORDER_STATUS_EXPIRED", "ORDER_STATUS_UNKNOWN",

    # from common.time_utils.py (via common.__init__)
    "UTC_TZ",

    # from config.py
    "API_URL",
    "MARKET_HUB_URL",
    "USER_HUB_URL",
    "TRADING_ENV_SETTING",
    "DEFAULT_CONFIG_CONTRACT_ID",
    "DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH",
    "TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES",
    "DEFAULT_TOKEN_LIFETIME_HOURS",
]