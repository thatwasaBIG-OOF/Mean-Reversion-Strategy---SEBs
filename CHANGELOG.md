All notable changes to the `tsxapipy` project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - YYYY-MM-DD 
### Added
- Placeholder for future changes.

## [0.4.0] - 2025-05-18
*(This version reflects significant changes to Real-Time Stream authentication and stability, plus ongoing enhancements)*

### Added
- **Real-Time Streams (`DataStream`, `UserHubStream`):**
    - Added `on_state_change_callback` and `on_error_callback` parameters to stream constructors for better external monitoring of stream health.
    - Stream classes now include more detailed internal connection status tracking (e.g., "connecting", "reconnecting", "error").
    - `_prepare_websocket_url_base()` method added to derive `ws://` or `wss://` URLs.
    - `_reinitialize_token_and_url()` method added to fetch a fresh token from the passed `APIClient` and construct the full WebSocket URL with the token embedded as a query parameter.
- **Configuration (`config.py`):**
    - Implemented environment selection (`TRADING_ENVIRONMENT`: "LIVE" or "DEMO") to dynamically set API/Hub URLs.
    - Added proactive checks to raise `ConfigurationError` if essential `API_KEY` or `USERNAME` are missing.
    - `config.py` now logs effective runtime configuration values.
    - Added `TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES` and `DEFAULT_TOKEN_LIFETIME_HOURS` for `APIClient` token management.
- **Historical Data (`historical/updater.py` - `HistoricalDataUpdater`):**
    - `HistoricalDataUpdater` now performs daily contract determination using `get_futures_contract_details`.
    - Attempts to use numeric contract IDs for history API calls if resolved, falling back to string IDs, and stores `contract_id_source` in Parquet data.
    - Implemented `overall_start_date_override` and `overall_end_date_override`.
    - More robust logic for iterating through daily contract fetch windows and handling weekend skips.
- **Error Handling (`api/exceptions.py`, `api/error_mapper.py`):**
    - Introduced `error_mapper.py` for mapping API `errorCode` and `errorMessage` to granular custom exceptions.
    - Added new specific exceptions like `MarketClosedError`, `MaxPositionLimitError`.
- **Trading Utilities (`trading/order_handler.py` - `OrderPlacer`):**
    - Added `place_limit_order`, `place_stop_market_order`.
    - `get_order_details` improved with configurable search window.
- **Examples & CLI Scripts:** Updated to reflect new stream initialization and APIClient usage. `trading_bot_cli.py` includes position reconciliation.
- **General:** `tsxapipy/__init__.py` exports more components and configuration variables.

### Changed
- **MAJOR CHANGE - Real-Time Stream Authentication (`DataStream`, `UserHubStream`):**
    - Stream classes (`DataStream`, `UserHubStream`) now **require an `APIClient` instance** in their `__init__` constructor instead of a raw token string.
    - **Token Embedding in URL with `skip_negotiation: True`**:
        - Streams now construct the full WebSocket URL themselves, embedding the `access_token` as a query parameter (e.g., `wss://hub_url?access_token=TOKEN`).
        - The `HubConnectionBuilder` is configured with `skip_negotiation: True` and this full URL.
        - The `accessTokenFactory` option is **no longer used** for `_build_connection` in this mode, as `signalrcore 0.9.5` did not appear to use the factory to correctly embed the token in the URL for direct WebSocket connections.
    - **Application-Driven Token Refresh for Streams:** The `update_token(new_token_from_api_client)` method on stream classes is now the primary mechanism for refreshing a stream's connection token. The application is responsible for monitoring `APIClient`'s token and calling this method on active streams when the `APIClient` token has been refreshed. This will trigger the stream to stop, rebuild its connection with the new tokenized URL, and restart.
- **API Client (`APIClient`):**
    - `APIClient._post_request` error handling for HTTP 400 (ASP.NET validation errors) and 401 (triggering re-auth) refined. Resolved a duplicated `try-except HTTPError` block.
    - `get_historical_bars` docstring clarifies caller's responsibility for `contract_id` type.
- **Historical Data (`historical/parquet_handler.py`):**
    - `append_bars_to_parquet` improved for data type consistency and UTC timestamp handling.
- **Removed `_is_access_token_factory_supported()`** from stream classes as the direct URL token embedding approach is now standard.
- **Removed `_get_latest_token_for_connection_factory()`** from stream classes.

### Fixed
- Resolved `TypeError` in CLI scripts caused by passing `token` argument to stream constructors instead of `api_client`.
- Addressed `NameError` for `argparse` in `order_watcher_cli.py`.
- Improved robustness of `.env` loading in `config.py`.
- More reliable `_on_close` and `_on_error` status updates in stream classes based on `signalrcore` behavior.

## [0.3.0] - YYYY-MM-DD 
*(This version represents the major refactor to APIClient and related changes from original 0.2.0 example changelog)*

### Added
- Implemented `APIClient` class in `api/client.py` as the primary interface for HTTP REST API calls, managing token state and revalidation.
- `APIClient` now includes automatic session token revalidation:
    - Checks for token expiry before most API calls.
    - Attempts to validate token via `/api/Auth/validate`.
    - Performs full re-authentication if validation fails or token is near expiry.
    - Retries an API call once if a 401 Unauthorized is received (after attempting re-auth).
- Added implementations for most documented HTTP API endpoints as methods within `APIClient`.
- `OrderPlacer` in `trading/order_handler.py` now uses `APIClient` instance.
- `OrderPlacer` methods return more meaningful values.
- Added `get_order_details` method to `OrderPlacer`.
- Defined example `ORDER_STATUS_*` constants (requires API verification).
- `UserHubStream` and `DataStream` include an `update_token(new_token)` method.
- `TradingBot` in `scripts/trading_bot_cli.py` significantly enhanced.
- `__init__.py` files updated; `tsxapipy/__init__.py` with `NullHandler` and `__version__`.
- `tests/conftest.py` with `sys.path` fixture.

### Changed
- `auth.authenticate()` now returns `(token_str, acquired_at_datetime_utc)`.
- `api/client.py` refactored: standalone API functions are now methods of `APIClient`.
- `api/contract_utils.py::get_futures_contract_details` takes an `APIClient`.
- `historical/updater.py::HistoricalDataUpdater` takes an `APIClient`.
- CLI scripts updated to use `APIClient` and new auth signature.
- `config.py` no longer calls `logging.basicConfig()`.
- Renamed `real_time/order_stream.py` to `real_time/user_hub_stream.py` (class `OrderStream` to `UserHubStream`).
- Stream classes: more robust `__init__` and callback error handling.
- Refined logging messages.

### Fixed
- Corrected `.env` loading in `config.py`.
- Addressed potential `TypeError` in `config.py` logging.
- Ensured `sys.path` in scripts points to `src`.

## [0.1.0] - YYYY-MM-DD 
*(Your Initial Version Date)*

### Added
- Initial project structure.
- Core modules: `auth`, `config`, `api/client`, `api/contract_utils`, `api/exceptions`.
- Historical data modules.
- Real-time streaming modules (basic).
- Trading utilities (simulated orders).
- Common utilities.
- Basic CLI scripts.
- Initial tests and `README.md`.

