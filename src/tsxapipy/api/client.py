# tsxapipy/api/client.py
"""
This module provides the APIClient class for interacting with the TopStepX HTTP REST API,
including authentication, request handling, and error mapping.
"""
import json
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta

import requests
from urllib3.util.retry import Retry # type: ignore[import-untyped]
from requests.adapters import HTTPAdapter # type: ignore[import-untyped]


# Import __version__ from the main tsxapipy package
try:
    from .. import __version__ as library_version
except ImportError:
    try:
        from tsxapipy import __version__ as library_version # pylint: disable=import-outside-toplevel
    except ImportError:
        library_version = "0.0.0-unknown" # Default fallback
        logging.getLogger(__name__).warning(
            "Could not import __version__ from tsxapipy. Using fallback version."
        )

from tsxapipy.config import (
    API_URL,
    USERNAME as CONFIG_USERNAME,
    API_KEY as CONFIG_API_KEY,
    DEFAULT_TOKEN_LIFETIME_HOURS,
    TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES
)
from tsxapipy.common.time_utils import UTC_TZ
from tsxapipy.api.exceptions import (
    APITimeoutError, APIHttpError, APIResponseError, APIError, AuthenticationError,
    InvalidParameterError, RateLimitExceededError,
    # These are currently unused directly in this file but might be mapped by error_mapper
    # InsufficientFundsError, OrderNotFoundError, OrderRejectedError, ContractNotFoundError,
    # MaxPositionLimitError, MarketClosedError
)
from .error_mapper import map_api_error_response

logger = logging.getLogger(__name__)

MAX_BARS_PER_REQUEST = 1000

def _perform_initial_authentication(username: str, api_key: str,
                                    api_url_override: Optional[str] = None) -> Dict[str, Any]:
    """Performs initial key-based authentication with /api/Auth/loginKey endpoint."""
    payload = {"userName": username, "apiKey": api_key}
    base_url = api_url_override if api_url_override else API_URL
    request_url = f"{base_url}/api/Auth/loginKey"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    action_desc = "Standalone Initial Key Authentication"
    logger.debug("%s: URL='%s'", action_desc, request_url)
    try:
        response = requests.post(request_url, headers=headers, json=payload, timeout=15)
        response.raise_for_status() # Raises HTTPError for 4xx/5xx
        response_data = response.json() # Can raise JSONDecodeError

        if response_data.get("success") and response_data.get("token") is not None:
            logger.info("%s successful.", action_desc)
            return response_data
        # else:
        api_err_msg = response_data.get('errorMessage')
        api_err_code = response_data.get('errorCode')
        default_msg = (
            f"Key-based login failed (API errorCode: {api_err_code}) "
            "or token missing/null in response."
        )
        final_err_msg = api_err_msg if api_err_msg is not None else default_msg
        logger.error("%s failed: %s. API Response: %s", action_desc, final_err_msg, response_data)
        raise AuthenticationError(final_err_msg)

    except requests.exceptions.HTTPError as e:
        err_text = e.response.text if e.response is not None else "No response body"
        logger.error(
            "HTTP error during %s: %s - %s",
            action_desc, e.response.status_code, err_text
        )
        raise AuthenticationError(
            f"HTTP error {e.response.status_code} during authentication: {err_text or str(e)}"
        ) from e
    except requests.exceptions.JSONDecodeError as e:
        response_text = response.text if 'response' in locals() and response else 'No response'
        logger.error(
            "Failed to decode JSON response during %s: %s. Response text: %s",
            action_desc, e, response_text,
            exc_info=True
        )
        raise AuthenticationError(f"Invalid JSON response received during authentication: {e}") from e
    except requests.exceptions.RequestException as e: # Catches Timeout, ConnectionError etc.
        logger.error("Network or request exception during %s: %s", action_desc, e, exc_info=True)
        raise AuthenticationError(f"Network or request exception during authentication: {e}") from e


# pylint: disable=too-many-instance-attributes, too-many-arguments
class APIClient:
    """
    A client for interacting with the TopStep (ProjectX) HTTP REST API.
    Handles authentication, token management (renewal), request retries,
    and consistent error handling by mapping API errors to custom exceptions.
    """
    def __init__(self, # pylint: disable=too-many-locals
                 initial_token: str,
                 token_acquired_at: datetime,
                 base_url: str = API_URL,
                 token_lifetime_hours: float = DEFAULT_TOKEN_LIFETIME_HOURS,
                 reauth_username: Optional[str] = None,
                 reauth_api_key: Optional[str] = None,
                 connect_timeout: float = 5.0,
                 read_timeout: float = 20.0,
                 total_retries: int = 3,
                 backoff_factor: float = 0.5,
                 retry_status_forcelist: Optional[List[int]] = None,
                 pool_connections: int = 10,
                 pool_maxsize: int = 10):
        if not initial_token:
            raise ValueError("APIClient requires a valid initial authentication token.")
        if not isinstance(token_acquired_at, datetime):
            raise ValueError("token_acquired_at must be a datetime object.")

        self.base_url = base_url.rstrip('/')
        self._token: str = initial_token
        if token_acquired_at.tzinfo is None:
            self._token_acquired_at: datetime = UTC_TZ.localize(token_acquired_at)
            logger.warning(
                "APIClient: initial token_acquired_at was naive, localized to UTC."
            )
        else:
            self._token_acquired_at: datetime = token_acquired_at.astimezone(UTC_TZ)

        self._token_lifetime: timedelta = timedelta(hours=token_lifetime_hours)
        self._reauth_username: Optional[str] = reauth_username or CONFIG_USERNAME
        self._reauth_api_key: Optional[str] = reauth_api_key or CONFIG_API_KEY

        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

        self.session = requests.Session()
        status_list = retry_status_forcelist if retry_status_forcelist is not None \
            else [429, 500, 502, 503, 504]

        retry_strategy = Retry(
            total=total_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_list,
            allowed_methods=["POST"] # Typically only retry POST for idempotency safety
        )
        adapter = HTTPAdapter(pool_connections=pool_connections,
                              pool_maxsize=pool_maxsize, max_retries=retry_strategy)
        if self.base_url.startswith("https://"):
            self.session.mount("https://", adapter)
        elif self.base_url.startswith("http://"):
            self.session.mount("http://", adapter)
        else:
            logger.warning("Base URL '%s' lacks scheme, defaulting to HTTPS.", self.base_url)
            self.session.mount("https://", adapter)

        self._update_headers()
        next_reval_check = (self._token_acquired_at + self._token_lifetime -
                            timedelta(minutes=TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES))
        logger.info(
            "APIClient initialized. Token acquired: %s. Next reval check: %s",
            self._token_acquired_at.isoformat(),
            next_reval_check.isoformat()
        )

    def _update_headers(self):
        self.session.headers.update({
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": f"tsxapipy/{library_version}"
        })

    @property
    def current_token(self) -> str:
        """Returns the current valid token, re-authenticating if necessary."""
        self._ensure_valid_token()
        return self._token

    def _is_token_nearing_expiry(self) -> bool:
        safety_margin = timedelta(minutes=TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES)
        effective_expiry_time = self._token_acquired_at + self._token_lifetime - safety_margin
        is_nearing = datetime.now(UTC_TZ) >= effective_expiry_time
        if is_nearing:
            logger.info(
                "Token nearing expiry. Current: %s, Effective Check: %s",
                datetime.now(UTC_TZ).isoformat(),
                effective_expiry_time.isoformat()
            )
        return is_nearing

    def _perform_re_authentication_internal(self):
        logger.info("Attempting APIClient re-authentication...")
        if not self._reauth_username or not self._reauth_api_key:
            raise AuthenticationError(
                "Re-auth credentials (USERNAME, API_KEY) not available to APIClient."
            )
        try:
            auth_response = _perform_initial_authentication(
                self._reauth_username, self._reauth_api_key, self.base_url
            )
            new_token = auth_response.get("token")
            if not new_token:
                raise AuthenticationError("Re-auth response missing token or token is null.")
            self._token, self._token_acquired_at = new_token, datetime.now(UTC_TZ)
            self._update_headers()
            logger.info(
                "APIClient re-authenticated. New token acquired: %s",
                self._token_acquired_at.isoformat()
            )
        except AuthenticationError as e:
            logger.error("APIClient re-authentication failed: %s", e)
            raise

    def _validate_current_token_internal(self) -> bool:
        logger.info("Validating current token via /api/Auth/validate...")
        try:
            response_data = self._post_request("/api/Auth/validate", {}, True,
                                               (self.connect_timeout, 10.0))
            if response_data.get("success"):
                new_token = response_data.get("newToken")
                if new_token and new_token != self._token:
                    logger.info("New token from /validate. Updating.")
                    self._token, self._token_acquired_at = new_token, datetime.now(UTC_TZ)
                    self._update_headers()
                else:
                    self._token_acquired_at = datetime.now(UTC_TZ) # Refresh acquisition time
                return True
            logger.warning(
                "Token validation success=false: %s",
                response_data.get('errorMessage')
            )
            return False
        except APIHttpError as e:
            logger.warning(
                "Token validation HTTP error %s. Assuming invalid. Details: %s",
                e.status_code, e.response_text
            )
            return False
        except APIError as e:
            logger.warning("Token validation API call failed: %s. Assuming invalid.", e)
            return False
        except Exception as e: # pylint: disable=broad-except
            logger.error("Unexpected error during token validation: %s", e, exc_info=True)
            return False

    def _ensure_valid_token(self):
        if self._is_token_nearing_expiry():
            logger.info("Token nearing expiry. Validating/Re-authenticating...")
            if not self._validate_current_token_internal():
                logger.info("Token validation failed. Re-authenticating fully.")
                self._perform_re_authentication_internal()
            else:
                logger.info("Token validated.")
        else:
            logger.debug("Token current.")

    # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    def _post_request(self, endpoint: str, payload: Dict[str, Any],
                      is_validation_or_reauth_call: bool = False,
                      per_request_timeout_override: Optional[Tuple[float, float]] = None
                     ) -> Dict[str, Any]:
        """
        Internal method to make POST requests, handling token management and error mapping.
        """
        if not is_validation_or_reauth_call:
            self._ensure_valid_token()

        request_url = f"{self.base_url}{endpoint}"
        action_desc = f"APIClient POST to {endpoint}"
        effective_timeout = per_request_timeout_override or \
                            (self.connect_timeout, self.read_timeout)

        log_auth_hdr = self.session.headers.get("Authorization","")
        if log_auth_hdr:
            log_auth_hdr = log_auth_hdr[:25] + "..."

        logger.debug(
            "%s: URL='%s', TokenUsed: %s, Payload(keys):'%s'",
            action_desc, request_url, log_auth_hdr or "None", list(payload.keys())
        )

        response: Optional[requests.Response] = None
        try:
            response = self.session.post(request_url, json=payload, timeout=effective_timeout)
            response.raise_for_status()
            response_data = response.json()

            if response_data.get("success") is True:
                logger.debug("%s successful (API success:true).", action_desc)
                return response_data
            # else: # HTTP 200 but success:false
            err_msg_api = response_data.get('errorMessage')
            err_code_api = response_data.get('errorCode')
            logger.error(
                "%s reported API failure (success:false): Msg='%s', Code='%s'. Full Response: %s",
                action_desc, err_msg_api or 'N/A', str(err_code_api), response_data
            )
            specific_exception = map_api_error_response(endpoint, response_data)
            if specific_exception:
                raise specific_exception
            raise APIResponseError(
                err_msg_api or f"API call to {endpoint} returned success=false "
                               f"with unmapped errorCode: {str(err_code_api)}",
                err_code_api,
                response_data
            )

        except requests.exceptions.HTTPError as e:
            err_text = e.response.text if e.response is not None else "No response body"
            resp_hdrs = dict(e.response.headers) if e.response is not None else {}
            logger.error(
                "HTTP error during %s: %s - %s",
                action_desc, e.response.status_code, err_text
            )

            if e.response.status_code == 400:
                try:
                    error_data = e.response.json()
                    if "errors" in error_data and "title" in error_data and \
                       "validation errors occurred" in error_data["title"].lower():
                        error_messages = [
                            f"Field '{field}': {'; '.join(messages)}"
                            for field, messages in error_data.get("errors", {}).items()
                        ]
                        detailed_error_message = "API Validation Error: " + " | ".join(error_messages)
                        raise InvalidParameterError(
                            detailed_error_message,
                            error_code=e.response.status_code, raw_response=error_data
                        ) from e
                except (json.JSONDecodeError, TypeError, KeyError):
                    pass # Fall through to generic APIHttpError

            if e.response.status_code == 401 and not is_validation_or_reauth_call:
                logger.warning(
                    "Received 401 Unauthorized on a regular API call. Forcing re-authentication."
                )
                try:
                    self._perform_re_authentication_internal()
                    logger.info("Retrying %s once after successful re-authentication.", action_desc)
                    return self._post_request(endpoint, payload, True,
                                              per_request_timeout_override=effective_timeout)
                except AuthenticationError as reauth_err:
                    logger.error("Re-authentication after 401 failed: %s", reauth_err)
                    raise APIHttpError(
                        e.response.status_code,
                        f"Original 401 error, re-authentication also failed: {reauth_err}",
                        err_text, headers=resp_hdrs
                    ) from e

            elif e.response.status_code == 429:
                retry_after_header = resp_hdrs.get('Retry-After')
                retry_secs = int(retry_after_header) \
                    if retry_after_header and retry_after_header.isdigit() else None
                raise RateLimitExceededError(
                    f"Rate limit (HTTP 429) for {action_desc}.", 429,
                    None, resp_hdrs, retry_secs
                ) from e

            raise APIHttpError(e.response.status_code, str(e), err_text, headers=resp_hdrs) from e

        except requests.exceptions.RetryError as e:
            original_cause = e.args[0] if e.args else e
            logger.error(
                "Max retries exceeded for %s. Last underlying error: %s",
                action_desc, original_cause
            )
            raise APIError(
                f"Max retries exceeded for {action_desc}. Last error: {original_cause}"
            ) from e
        except requests.exceptions.Timeout as e:
            logger.error("Timeout during %s to %s: %s", action_desc, request_url, e)
            raise APITimeoutError(f"Timeout during {action_desc}.") from e
        except requests.exceptions.RequestException as e:
            logger.error("Request exception during %s: %s", action_desc, e, exc_info=True)
            raise APIError(f"Request exception during {action_desc}: {e}") from e
        except json.JSONDecodeError as e:
            resp_text = response.text if response and hasattr(response, "text") else "N/A"
            status_code = response.status_code if response else 'N/A'
            logger.error(
                "Failed to decode JSON response from %s (HTTP status was %s): %s. Response Text: %s",
                action_desc, status_code, e, resp_text[:500]
            )
            raise APIError(
                f"Invalid JSON response from {action_desc} despite successful HTTP status."
            ) from e
        except APIError:
            raise # Re-raise it as is
        except Exception as e: # pylint: disable=broad-except
            logger.error(
                "An unexpected Python exception occurred internally during %s: %s",
                action_desc, e, exc_info=True
            )
            raise APIError(
                f"An unexpected Python error occurred while processing API call for {action_desc}: {e}"
            ) from e

    def get_accounts(self, only_active: bool = True) -> List[Dict[str, Any]]:
        """Fetches a list of accounts associated with the authenticated user."""
        response_data = self._post_request("/api/Account/search",
                                           {"onlyActiveAccounts": only_active})
        return response_data.get("accounts", [])

    def search_contracts(self, search_text: str, live: bool = False) -> List[Dict[str, Any]]:
        """Searches for contracts based on a text query."""
        response_data = self._post_request("/api/Contract/search",
                                           {"searchText": search_text, "live": live})
        return response_data.get("contracts", [])

    def search_contract_by_id(self, contract_id: str) -> List[Dict[str, Any]]:
        """Fetches contract details for a specific contract ID."""
        response_data = self._post_request("/api/Contract/searchById",
                                           {"contractId": contract_id})
        return response_data.get("contracts", [])

    def get_historical_bars(self, contract_id: Any, start_time_iso: str, end_time_iso: str,
                            unit: int, unit_number: int, limit: int,
                            include_partial_bar: bool = False, live: bool = False
                           ) -> List[Dict[str, Any]]:
        """Fetches historical bar data for a contract (/api/History/retrieveBars).
        (Full docstring as previously provided)
        """
        payload = {"contractId": contract_id, "live": live, "startTime": start_time_iso,
                   "endTime": end_time_iso, "unit": unit, "unitNumber": unit_number,
                   "limit": min(limit, MAX_BARS_PER_REQUEST),
                   "includePartialBar": include_partial_bar}
        response_data = self._post_request("/api/History/retrieveBars", payload,
                                           per_request_timeout_override=(self.connect_timeout,
                                                                         45.0))
        bars = response_data.get('bars', [])
        if bars:
            bars.reverse() # API sends newest first, client wants oldest first
        return bars

    def place_order(self, order_details: Dict[str, Any]) -> Dict[str, Any]:
        """Places a new order."""
        return self._post_request("/api/Order/place", order_details)

    def cancel_order(self, account_id: int, order_id: int) -> Dict[str, Any]:
        """Cancels an existing open order. Uses "orderId" in payload."""
        return self._post_request("/api/Order/cancel",
                                  {"accountId": account_id, "orderId": order_id})

    def modify_order(self, modification_details: Dict[str, Any]) -> Dict[str, Any]:
        """Modifies an existing open order."""
        if "orderId" not in modification_details:
            if "orderld" in modification_details: # Legacy key
                logger.warning(
                    "Modify_order received 'orderld', automatically using it as 'orderId'. "
                    "Update calling code to use 'orderId'."
                )
                modification_details["orderId"] = modification_details.pop("orderld")
            else:
                logger.error("modify_order payload missing the required 'orderId' field.")
        return self._post_request("/api/Order/modify", modification_details)

    def search_orders(self, account_id: int, start_timestamp_iso: str,
                      end_timestamp_iso: Optional[str] = None) -> List[Dict[str, Any]]:
        """Searches orders for an account within a time range."""
        payload: Dict[str, Any] = {"accountId": account_id,
                                   "startTimestamp": start_timestamp_iso}
        if end_timestamp_iso:
            payload["endTimestamp"] = end_timestamp_iso
        response_data = self._post_request("/api/Order/search", payload)
        return response_data.get("orders", [])

    def close_contract_position(self, account_id: int, contract_id: str) -> Dict[str, Any]:
        """Closes entire position for a contract."""
        return self._post_request("/api/Position/closeContract",
                                  {"accountId": account_id, "contractId": contract_id})

    def partial_close_contract_position(self, account_id: int, contract_id: str,
                                        size: int) -> Dict[str, Any]:
        """Partially closes a position by specified size."""
        if not isinstance(size, int) or size <= 0:
            raise InvalidParameterError(
                f"Partial close size must be positive integer, got {size}",
                raw_response={"size":size}
            )
        return self._post_request("/api/Position/partialCloseContract",
                                  {"accountId": account_id, "contractId": contract_id,
                                   "size": size})

    def search_open_positions(self, account_id: int) -> List[Dict[str, Any]]:
        """Retrieves currently open positions for an account."""
        response_data = self._post_request("/api/Position/searchOpen",
                                           {"accountId": account_id})
        return response_data.get("positions", [])

    def search_trades(self, account_id: int, start_timestamp_iso: str,
                      end_timestamp_iso: Optional[str] = None) -> List[Dict[str, Any]]:
        """Searches for trade executions for an account."""
        payload: Dict[str, Any] = {"accountId": account_id,
                                   "startTimestamp": start_timestamp_iso}
        if end_timestamp_iso:
            payload["endTimestamp"] = end_timestamp_iso
        response_data = self._post_request("/api/Trade/search", payload)
        return response_data.get("trades", [])

    @staticmethod
    def initial_authenticate_app(username: str, password: str, app_id: str, verify_key: str,
                                 api_url: Optional[str] = None) -> Dict[str, Any]:
        """Authenticates as an authorized application."""
        payload = {"userName": username, "password": password,
                   "appId": app_id, "verifyKey": verify_key}
        base_url = api_url if api_url else API_URL
        request_url = f"{base_url}/api/Auth/loginApp"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        action_desc = "Static Initial App Authentication"
        logger.debug("%s: URL='%s'", action_desc, request_url)
        response = None # Initialize for potential use in except block
        try:
            response = requests.post(request_url, headers=headers, json=payload, timeout=15)
            response.raise_for_status()
            response_data = response.json()
            if response_data.get("success") and "token" in response_data:
                logger.info("%s successful.", action_desc)
                return response_data
            # else:
            api_err_msg = response_data.get('errorMessage')
            api_err_code = response_data.get('errorCode')
            default_msg = (
                f"App login failed (API errorCode: {api_err_code}) "
                "or token missing in response."
            )
            final_err_msg = api_err_msg if api_err_msg is not None else default_msg
            logger.error("%s failed: %s. Response: %s", action_desc, final_err_msg, response_data)
            raise AuthenticationError(final_err_msg)
        except requests.exceptions.HTTPError as e:
            err_text = e.response.text if e.response is not None else "No response body"
            http_err_msg = f"HTTP error during {action_desc}: {e.response.status_code}"
            if err_text:
                http_err_msg += f" - {err_text}"
            raise AuthenticationError(http_err_msg) from e
        except requests.exceptions.JSONDecodeError as e:
            response_text = response.text if response else 'No response'
            logger.error(
                "Failed to decode JSON response during %s for app auth: %s. Response text: %s",
                action_desc, e, response_text,
                exc_info=True
            )
            raise AuthenticationError(
                f"Invalid JSON response received during app authentication: {e}"
            ) from e
        except Exception as e: # pylint: disable=broad-except
            raise AuthenticationError(f"Unexpected error during {action_desc}: {e}") from e