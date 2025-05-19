# TopStep Data Library & Trading Suite for Python (`tsxapipy`)

**Version:** 0.4.0 

## Overview

`tsxapipy` is designed for interacting with the TopStep (also referred to as ProjectX in some API documentation contexts) trading API. This suite provides a comprehensive toolkit for traders and developers looking to build automated trading strategies, perform data analysis, or manage their TopStep accounts programmatically.

## Core Features

*   **Robust Authentication & Session Management:**
    *   Supports both API key-based and authorized application authentication methods via `tsxapipy.auth.authenticate()`.
    *   **`APIClient` for REST:** Manages HTTP session, includes automatic session token revalidation for long-running applications making REST calls, ensuring tokens are fresh or renewed if expired/invalid.
*   **Comprehensive HTTP API Client (`APIClient`):**
    *   Provides intuitive Python methods for nearly all documented TopStep HTTP REST API endpoints.
    *   **Account Management:** Retrieve account details, balances, and lists of active accounts.
    *   **Contract Information:** Search for contracts by text description (e.g., "NQ") or by specific contract ID.
    *   **Historical Data Retrieval:** Fetch historical bar data (OHLCV) with flexible parameters for timeframe (seconds, minutes, hours, day, week, month) and period.
    *   **Full Order Lifecycle:**
        *   Place various order types (Market, Limit, Stop, etc.).
        *   Modify existing open orders (e.g., change price, size).
        *   Cancel open orders.
        *   Search and retrieve details of past and current orders.
    *   **Position Management:**
        *   Close entire open positions for a contract.
        *   Partially close open positions by specifying a size.
        *   Search and retrieve details of currently open positions.
    *   **Trade History:** Search and retrieve records of executed trades.
*   **Real-Time Data Streaming (via SignalR with Direct WebSocket & URL Token):**
    *   `DataStream` (Market Hub) and `UserHubStream` (User Hub) classes for live data.
    *   **Streamlined Connection:** Utilizes `skip_negotiation: True` for direct WebSocket connections, aligning with common efficient practices.
    *   **Token Management for Streams:**
        *   Stream classes (`DataStream`, `UserHubStream`) are initialized with an `APIClient` instance.
        *   The initial authentication token (obtained via `APIClient`) is embedded directly into the WebSocket connection URL's query string (e.g., `wss://hub_url?access_token=TOKEN`).
        *   **Application-Driven Token Refresh:** For long-lived stream connections, if the `APIClient`'s session token is refreshed (e.g., due to its internal revalidation logic for REST calls, or if the application explicitly re-authenticates), the application code is responsible for:
            1.  Obtaining the new token from `api_client_instance.current_token`.
            2.  Calling `stream_instance.update_token(new_api_token)` on any active stream instances.
            This `update_token` method within the stream classes handles stopping the current connection, reconfiguring its URL with the new token, and restarting the stream to ensure continued authenticated communication.
    *   Configured for automatic reconnection attempts by the underlying `signalrcore` library (using the last known valid URL with token until `update_token` is called with a new one).
    *   Clear callback mechanisms for market data (quotes, trades, depth) and user data (account, orders, positions, user trades), as well as stream state changes (connected, disconnected, reconnecting, error) and general connection errors.
*   **Advanced Historical Data Management (`HistoricalDataUpdater`):**
    *   Efficiently fetches, stores, and updates historical bar data for specified instruments.
    *   Utilizes the Parquet file format for optimized, columnar storage, enabling fast read/write operations, especially with `pandas`.
    *   Includes intelligent gap-filling logic to ensure data integrity by identifying and fetching missing periods in the downloaded historical data.
    *   Supports daily contract determination for futures and stores the source contract ID for traceability.
    *   Allows for overriding fetch windows with specific start/end dates.
*   **Practical Trading Utilities & Examples:**
    *   **`OrderPlacer`:** A higher-level class for simplified order submission, modification, and cancellation, using user-friendly parameters and managing API interactions.
    *   **Technical Indicators:** Includes foundational technical analysis indicators (e.g., Simple Moving Average - SMA, Exponential Moving Average - EMA) to aid in building trading logic.
    *   **Example Trading Logic:** Provides examples of simple trade decision logic (e.g., SMA crossover) to demonstrate strategy implementation.
*   **Enhanced Error Handling & Resilience:**
    *   Defines a hierarchy of custom, specific exceptions (e.g., `APIError`, `APITimeoutError`, `AuthenticationError`, `OrderNotFoundError`, `ContractNotFoundError`, `MarketClosedError`) for more granular error identification and robust handling by consuming applications.
    *   `APIClient` includes built-in retry mechanisms for transient network issues on REST calls.
*   **Modular and Organized Design:**
    *   Logically structured into clear sub-packages (`api`, `auth`, `common`, `config`, `historical`, `real_time`, `trading`) within `tsxapipy` for improved organization, maintainability, and ease of navigation and understanding.
*   **Configuration Driven:**
    *   Leverages `.env` files for managing API credentials, default parameters (like contract/account IDs for scripts), and environment-specific URLs (LIVE/DEMO), keeping sensitive data and configurations separate from source code.
*   **Command-Line Interface (CLI) Scripts:**
    *   Includes several ready-to-run CLI scripts for common tasks and demonstrations:
        *   Fetching and updating historical data with various options (`fetch_historical_cli.py`).
        *   Testing real-time market data streams for specific contracts (`market_data_tester_cli.py`).
        *   Monitoring real-time user-specific data streams (`user_data_tester_cli.py`).
        *   Dumping account information (`dump_accounts_cli.py`).
        *   Running a basic example trading bot with SMA crossover logic (`trading_bot_cli.py`).

## Project Structure


topstep_data_suite/
├── src/
│ └── tsxapipy/ # The core Python library
│ ├── init.py
│ ├── api/ # APIClient, contract utils, custom exceptions, error_mapper
│ ├── auth.py # Authentication logic, token management
│ ├── common/ # Shared utilities (e.g., time_utils, enums for OrderSide/OrderType)
│ ├── config.py # Configuration loading from .env, URL management
│ ├── historical/ # HistoricalDataUpdater, Parquet I/O, gap_detector
│ ├── real_time/ # DataStream (Market Hub), UserHubStream (User Hub)
│ └── trading/ # Indicators, trade decision logic, OrderPlacer
├── scripts/ # Runnable CLI applications demonstrating library use
├── examples/ # Focused code snippets and specific test cases (e.g., error code tests)
├── tests/ # Unit and integration tests (pytest based)
├── .env.example # Example environment file
├── requirements.txt # Python dependencies
├── README.md # This file
├── LICENSE # Apache 2.0 License file
└── CHANGELOG.md # Project changes log

## Installation

1.  **Prerequisites:**
    *   Python 3.8+
    *   Git (for cloning)

2.  **Clone the Repository:**
    ```bash
    git clone <your-repository-url> topstep_data_suite
    cd topstep_data_suite
    ```

3.  **Create and Activate a Virtual Environment (Recommended):**
    ```bash
    python -m venv .venv
    # Windows PowerShell:
    .\.venv\Scripts\activate
    # macOS/Linux:
    source .venv/bin/activate
    ```

4.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    Ensure your `requirements.txt` includes (example, adjust as needed):
    ```txt
    requests
    python-dotenv
    pandas
    pyarrow  # For Parquet support
    pytz
    signalrcore # Or the specific synchronous SignalR client library used (e.g., 0.9.5)
    # For development (consider a requirements-dev.txt):
    # pytest
    # pytest-mock
    # flake8
    # black
    # mypy
    ```

## Configuration

The library uses a `.env` file located in the project root (`topstep_data_suite/.env`) to manage sensitive credentials and configuration settings.

1.  Copy the example environment file:
    ```bash
    cp .env.example .env
    ```
2.  **Edit `.env`** and replace placeholder values with your actual TopStep API Key, Username, and any other relevant defaults:
    ```env
    # == Environment: "LIVE" or "DEMO" (defaults to "DEMO" if not set) ==
    TRADING_ENVIRONMENT="DEMO" 

    # == Core API Credentials (Required for API Key Auth) ==
    API_KEY="YOUR_TOPSTEP_API_KEY"
    USERNAME="YOUR_TOPSTEP_USERNAME"

    # == Optional: Default values for scripts & library components ==
    DEFAULT_CONFIG_CONTRACT_ID="CON.F.US.NQ.M25" # Example: Default contract for bot/market data tester
    DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH=""      # Example: Default account for user stream/trading bot (set your specific account ID)

    # == Optional: For Authorized Application Auth (if you use this method) ==
    # APP_PASSWORD="YOUR_APP_PASSWORD"
    # APP_ID="YOUR_APP_ID"
    # APP_VERIFY_KEY="YOUR_APP_VERIFY_KEY"

    # == Optional: For specific integration tests (e.g., test_specific_history_gap.py) ==
    # TEST_GAP_CONTRACT_ID="CON.F.US.ENQ.M25"
    # TEST_GAP_LAST_TS="2025-05-16T04:20:00+00:00"

    # == API URLs: Override only if absolutely necessary (e.g., for a private test environment) ==
    # Default LIVE/DEMO URLs are typically hardcoded as fallbacks in config.py.
    # API_BASE_URL_OVERRIDE_ENV="https://custom-gateway-api.yourdomain.com" 
    # MARKET_HUB_URL_OVERRIDE_ENV="https://custom-rtc.yourdomain.com/hubs/market" 
    # USER_HUB_URL_OVERRIDE_ENV="https://custom-rtc.yourdomain.com/hubs/user" 
    ```
    *(Note: `.env` variable names like `DEFAULT_CONFIG_CONTRACT_ID` are used to avoid clashes if the library itself directly exports `CONTRACT_ID` from `config.py`)*

## Core Library Usage

### 1. Authentication & APIClient (for REST API)

The primary way to interact with the HTTP REST API is through the `APIClient`.

```python
import logging
from tsxapipy import (
    authenticate, 
    APIClient, 
    APIError, 
    AuthenticationError,
    ConfigurationError
)

# Configure logging for your application
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s')
logger = logging.getLogger("MyApplication")

try:
    logger.info("Authenticating with TopStepX API...")
    initial_token, token_acquired_at = authenticate() # Uses credentials from .env via config.py
    
    api_client = APIClient(
        initial_token=initial_token,
        token_acquired_at=token_acquired_at
        # reauth_username/reauth_api_key are pulled from config by default if needed by APIClient
    )
    logger.info("APIClient initialized successfully.")

    # Example: Get account details
    accounts = api_client.get_accounts(only_active=True)
    if accounts:
        for account in accounts:
            logger.info(f"Active Account: {account.get('name')} (ID: {account.get('id')}), Balance: {account.get('balance')}")
    else:
        logger.info("No active accounts found.")

except ConfigurationError as e:
    logger.error(f"SETUP ERROR: {e}. Ensure .env file is correct and has API_KEY/USERNAME.")
except AuthenticationError as e:
    logger.error(f"AUTHENTICATION FAILED: {e}")
except APIError as e:
    logger.error(f"API operation failed: {e}")
except Exception as e:
    logger.error(f"An unexpected error occurred: {e}", exc_info=True)
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
IGNORE_WHEN_COPYING_END

The APIClient automatically handles session token revalidation for its HTTP REST API calls.

2. Fetching Historical Data (HistoricalDataUpdater)

Use the HistoricalDataUpdater class for managing local Parquet datasets.

from tsxapipy import HistoricalDataUpdater
# ... (assuming api_client is initialized as above) ...

try:
    if api_client: # Ensure api_client was initialized
        updater = HistoricalDataUpdater(
            api_client=api_client, 
            symbol_root="NQ",      
            main_parquet_file="data/nq_1min_data.parquet", # Example path
            temp_file_suffix="_temp_update",
            api_bar_unit=2, # 2 for Minute
            api_bar_unit_number=1, # For 1-minute bars
            fetch_days_if_new=90 
        )
        logger.info("Attempting to update historical data for NQ...")
        updater.update_data()
        logger.info("Historical data update process complete for NQ.")
except APIError as e: # Catch errors specific to historical data fetching if needed
    logger.error(f"API error during historical data update: {e}")
except Exception as e:
    logger.error(f"Unexpected error during historical data update: {e}", exc_info=True)
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Python
IGNORE_WHEN_COPYING_END
3. Real-Time Market Data Streaming (DataStream)

The DataStream class connects to the Market Hub.

import time
from tsxapipy import DataStream # Assuming api_client already initialized

# Define your callback functions
def my_quote_handler(quote_data):
    logger.info(f"Market Quote: {quote_data.get('bp')}@{quote_data.get('bs')} / {quote_data.get('ap')}@{quote_data.get('as')}")

def my_market_trade_handler(trade_data):
    logger.info(f"Market Trade: Px={trade_data.get('price')}, Sz={trade_data.get('size')}")

def my_market_stream_state_handler(state: str):
    logger.info(f"Market Stream State: {state}")

market_stream_instance: Optional[DataStream] = None
CONTRACT_TO_STREAM_MARKET = "CON.F.US.NQ.M25" # Example

try:
    if api_client_instance: # Ensure api_client is available from previous steps
        market_stream_instance = DataStream(
            api_client=api_client_instance, 
            contract_id_to_subscribe=CONTRACT_TO_STREAM_MARKET,
            on_quote_callback=my_quote_handler,
            on_trade_callback=my_market_trade_handler, # Optional
            on_state_change_callback=my_market_stream_state_handler
        )
        if market_stream_instance.start():
            logger.info(f"Market stream started for {CONTRACT_TO_STREAM_MARKET}. Monitoring... (Press Ctrl+C to stop)")
            # Keep alive loop for the example
            while True: 
                # Application is responsible for calling market_stream_instance.update_token() if APIClient token changes
                time.sleep(10) 
        else:
            logger.error(f"Failed to start market stream for {CONTRACT_TO_STREAM_MARKET}.")
except KeyboardInterrupt:
    logger.info("Market stream monitoring interrupted.")
except Exception as e:
    logger.error(f"Error in market stream example: {e}", exc_info=True)
finally:
    if market_stream_instance:
        logger.info("Stopping market stream...")
        market_stream_instance.stop()
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Python
IGNORE_WHEN_COPYING_END
4. Real-Time User Data Streaming (UserHubStream)

The UserHubStream class connects to the User Hub for account-specific events.

from tsxapipy import UserHubStream # Assuming api_client already initialized
# from tsxapipy.config import DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH # Or get from .env

# Assuming ACCOUNT_ID_TO_WATCH is loaded
ACCOUNT_TO_MONITOR = DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH or 12345 # Fallback example

def my_order_update_handler(order_data):
    logger.info(f"My Order Update (Acct: {ACCOUNT_TO_MONITOR}): ID={order_data.get('id')}, Status={order_data.get('status')}")
# ... other user-specific callbacks (account, position, trade)

def my_user_stream_state_handler(state: str):
    logger.info(f"User Stream (Acct: {ACCOUNT_TO_MONITOR}) State: {state}")

user_stream_instance: Optional[UserHubStream] = None
try:
    if api_client_instance and ACCOUNT_TO_MONITOR:
        user_stream_instance = UserHubStream(
            api_client=api_client_instance,
            account_id_to_watch=ACCOUNT_TO_MONITOR,
            on_order_update=my_order_update_handler,
            # on_account_update=..., on_position_update=..., on_user_trade_update=...
            on_state_change_callback=my_user_stream_state_handler
        )
        if user_stream_instance.start():
            logger.info(f"User Hub stream started for Account {ACCOUNT_TO_MONITOR}. Monitoring... (Press Ctrl+C to stop)")
            while True: time.sleep(10) # Keep alive
        else:
            logger.error(f"Failed to start User Hub stream for Account {ACCOUNT_TO_MONITOR}.")
# ... (exception handling and finally block similar to DataStream example) ...
except KeyboardInterrupt: logger.info("User stream monitoring interrupted.")
except Exception as e: logger.error(f"Error in user stream example: {e}", exc_info=True)
finally:
    if user_stream_instance: logger.info("Stopping user stream..."); user_stream_instance.stop()
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Python
IGNORE_WHEN_COPYING_END

Important Note on Stream Token Refresh (Application Responsibility):

The DataStream and UserHubStream classes are initialized using an APIClient instance. They use this client to fetch their initial authentication token, which is then embedded in the WebSocket connection URL (when using skip_negotiation: True).

If the APIClient's session token is refreshed during the lifetime of an active stream (e.g., due to APIClient's internal revalidation logic for REST calls, or if the application explicitly re-authenticates), the application code is responsible for propagating this new token to the stream instances.

This is done by:

Obtaining the latest token from your api_client_instance.current_token.

Calling your_stream_instance.update_token(new_api_token) on any active DataStream or UserHubStream objects.

The update_token() method within the stream classes will then handle stopping the current connection, reconfiguring its connection URL with the new token, and restarting the stream. The underlying signalrcore library's automatic reconnection feature will attempt to use the last configured URL (with its then-current token) until update_token() provides it with a new one for a fresh connection cycle.

5. Placing and Managing Orders (OrderPlacer)

Use the OrderPlacer class for a simplified interface to trading operations.

from tsxapipy import OrderPlacer
# from tsxapipy.common.enums import OrderSide # If you implement enums

# Assuming api_client_instance and relevant ACCOUNT_ID, CONTRACT_ID are available
# ACCOUNT_ID_FOR_TRADING = DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH
# CONTRACT_FOR_TRADING = DEFAULT_CONFIG_CONTRACT_ID

try:
    if api_client_instance and ACCOUNT_ID_FOR_TRADING and CONTRACT_FOR_TRADING:
        order_placer = OrderPlacer(
            api_client=api_client_instance,
            account_id=ACCOUNT_ID_FOR_TRADING,
            default_contract_id=CONTRACT_FOR_TRADING
        )
        logger.info("OrderPlacer initialized.")

        # Place a market buy order (ensure OrderSide.BUY maps to your internal representation "BUY" or 0)
        buy_order_id = order_placer.place_market_order(side="BUY", size=1) 
        if buy_order_id:
            logger.info(f"Market buy order submitted, API Order ID: {buy_order_id}")
            # time.sleep(2) # Allow order to process
            # cancelled = order_placer.cancel_order(order_id=buy_order_id)
            # logger.info(f"Attempt to cancel order {buy_order_id}: {cancelled}")
        else:
            logger.error("Failed to place market buy order.")
except Exception as e:
    logger.error(f"Error with OrderPlacer example: {e}", exc_info=True)
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Python
IGNORE_WHEN_COPYING_END

Refer to tsxapipy/trading/order_handler.py for more OrderPlacer methods.

Error Handling

The library defines custom exceptions in tsxapipy.api.exceptions (e.g., APIError, AuthenticationError). Always wrap library calls in try...except blocks.

from tsxapipy import APIError, APITimeoutError, AuthenticationError, ConfigurationError
import requests # For lower-level network errors if not caught by APIClient's retries

try:
    # ... library call ...
    pass
except ConfigurationError as e:
    logger.error(f"Configuration Error: {e}")
except AuthenticationError as e:
    logger.error(f"Authentication Failed: {e}")
except APITimeoutError:
    logger.error("API request timed out.")
except APIError as e: # General tsxapipy API error
    logger.error(f"tsxapipy API Error: Code {e.error_code if hasattr(e, 'error_code') else 'N/A'}, Message: {e}")
except requests.exceptions.RequestException as e: # Lower-level network
    logger.error(f"Network request failed: {e}")
except Exception as e:
    logger.error(f"An unexpected error: {e}", exc_info=True)
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Python
IGNORE_WHEN_COPYING_END
CLI Scripts

The scripts/ directory contains several useful command-line tools:

dump_accounts_cli.py: Fetches and displays details of your trading accounts.

fetch_historical_cli.py: Downloads historical bar data and stores it in Parquet files.

market_data_tester_cli.py: Connects to the Market Hub for real-time quotes, trades, depth.

user_data_tester_cli.py (formerly order_watcher_cli.py): Connects to the User Hub for account, order, position, and trade updates.

trading_bot_cli.py: An example trading bot demonstrating library integration.

Running Scripts:
Navigate to the topstep_data_suite root directory. Ensure your virtual environment is activated and .env is configured.

python scripts/<script_name.py> [arguments]
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Bash
IGNORE_WHEN_COPYING_END

Example:

python scripts/fetch_historical_cli.py --symbol_root NQ --api_period 1min --output_dir data/historical_nq --debug
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Bash
IGNORE_WHEN_COPYING_END

Use the --help flag for each script to see its available arguments.

Development and Testing

Tests: Located in the tests/ directory (unit/ and integration/).

Running Tests: From the project root, use pytest:

pytest
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Bash
IGNORE_WHEN_COPYING_END

Linters/Formatters: We recommend flake8 and black. mypy for type checking.

Future Enhancements (Planned/Considered)

Full implementation for all User Hub and Market Hub event subscriptions (e.g., market depth parsing).

More sophisticated order/position tracking in OrderPlacer and TradingBot.

Pydantic models for API request/response validation and stream message structuring.

Advanced examples, strategy backtesting tools.

Potential asyncio version of the library.

Full packaging for pip installation from PyPI.

Enhanced contract roll logic in HistoricalDataUpdater.

Contributing

We welcome contributions! Please see CONTRIBUTING.md (if available) or open an issue to discuss your ideas. General guidelines:

Fork the repository.

Create a feature or bugfix branch.

Install dependencies, including development tools (e.g., from requirements-dev.txt).

Write clean, PEP 8 compliant code with docstrings and comments.

Add unit tests for new functionality or bug fixes. Ensure all tests pass.

Update README.md or other documentation if your changes affect usage.

Format code with black and lint with flake8.

Submit a Pull Request with a clear description of your changes.

License

This project is licensed under the Apache License 2.0. A copy of the license can be found in the LICENSE file in the root directory of this source tree.
