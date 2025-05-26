"""
API client module for tsxapipy.

This module provides the APIClient class for making API requests.
"""

import logging
import time
import json # Added import for json
from datetime import datetime, timedelta, timezone # Added timezone for _check_token_expiry
from typing import Dict, Any, Optional, Tuple, List, Union

import requests
from pydantic import ValidationError # ADDED: Import Pydantic's ValidationError

# Assuming library_version is defined somewhere, e.g., in __init__.py or hardcoded
try:
    from tsxapipy import __version__ as library_version
except ImportError:
    library_version = "0.0.0-unknown"


from tsxapipy.config import (
    API_URL,
    # MARKET_HUB_URL, # Not used directly in APIClient for REST
    # USER_HUB_URL,   # Not used directly in APIClient for REST
    TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES,
    DEFAULT_TOKEN_LIFETIME_HOURS,
    USERNAME as CONFIG_USERNAME, # For re-authentication fallback
    API_KEY as CONFIG_API_KEY    # For re-authentication fallback
)
from tsxapipy.common.time_utils import UTC_TZ
from tsxapipy.api.exceptions import (
    APIError, AuthenticationError, APIHttpError, APIResponseParsingError,
    InvalidParameterError, RateLimitExceededError, LibraryError, ConfigurationError,
    APITimeoutError, APIResponseError # Ensure all needed exceptions are imported
)
from .error_mapper import map_api_error_response
# Ensure schemas is imported correctly relative to client.py
# If client.py is in tsxapipy/api/, and schemas.py is also in tsxapipy/api/, then:
from . import schemas
# If client.py is in tsxapipy/ and schemas.py is in tsxapipy/api/, then:
# from .api import schemas (this was in your original prompt, looks correct if client.py is one level up)
# Given the traceback, schemas seems to be in tsxapipy/api/schemas.py, and client.py is tsxapipy/api/client.py
# So, `from . import schemas` or `from tsxapipy.api import schemas` should work if structure is standard.
# Let's assume `from . import schemas` is correct if client.py is inside the `api` package.
# For robustness, if your project structure places client.py outside `api` but `schemas` inside `api`:
# from tsxapipy.api import schemas as api_schemas_module # then use api_schemas_module.MyModel

logger = logging.getLogger(__name__)

MAX_BARS_PER_REQUEST = 1000

# REMOVED: Standalone _perform_initial_authentication(self) function.
# The APIClient constructor now expects an initial token.
# Re-authentication is handled by _perform_re_authentication_internal.

# pylint: disable=too-many-instance-attributes, too-many-arguments
class APIClient:
    """
    A client for interacting with the TopStep (ProjectX) HTTP REST API.
    Manages session token, re-authentication, request retries, and error mapping.
    Uses Pydantic models for request/response validation and data structuring.
    """
    def __init__(self, initial_token: str, token_acquired_at: datetime,
                 api_base_url: Optional[str] = None,
                 connect_timeout_sec: float = 10.0, # Default connect timeout
                 read_timeout_sec: float = 30.0,    # Default read timeout
                 reauth_username: Optional[str] = None,
                 reauth_api_key: Optional[str] = None,
                 default_token_lifetime_hours: float = DEFAULT_TOKEN_LIFETIME_HOURS
                ):
        """
        Initializes the APIClient.

        Args:
            initial_token (str): The initial authentication token.
            token_acquired_at (datetime): UTC datetime when the initial_token was acquired.
            api_base_url (Optional[str]): Override for the API base URL. Defaults to config.
            connect_timeout_sec (float): Timeout for establishing a connection.
            read_timeout_sec (float): Timeout for receiving data after connection.
            reauth_username (Optional[str]): Username to use for re-authentication. Falls back to config.
            reauth_api_key (Optional[str]): API key for re-authentication. Falls back to config.
            default_token_lifetime_hours (float): Assumed lifetime of a token if not otherwise known.
        """
        if not initial_token:
            raise ValueError("APIClient requires an initial_token.")
        if not isinstance(token_acquired_at, datetime):
            raise ValueError("APIClient requires token_acquired_at to be a datetime object.")
        if token_acquired_at.tzinfo is None or token_acquired_at.tzinfo.utcoffset(token_acquired_at) != timedelta(0):
            # Ensure it's UTC. If naive, assume UTC and localize. If aware but not UTC, convert.
            if token_acquired_at.tzinfo is None:
                logger.warning("APIClient: token_acquired_at was naive, localizing to UTC.")
                token_acquired_at = UTC_TZ.localize(token_acquired_at)
            else:
                logger.warning(f"APIClient: token_acquired_at was {token_acquired_at.tzinfo}, converting to UTC.")
                token_acquired_at = token_acquired_at.astimezone(UTC_TZ)

        self.base_url = api_base_url or API_URL # Use API_URL from config
        self.session = requests.Session()
        
        self.connect_timeout = connect_timeout_sec
        self.read_timeout = read_timeout_sec

        self._token = initial_token
        self._token_acquired_at = token_acquired_at
        self._token_lifetime = timedelta(hours=default_token_lifetime_hours)

        self._reauth_username = reauth_username or CONFIG_USERNAME
        self._reauth_api_key = reauth_api_key or CONFIG_API_KEY
        
        self._update_headers()

        logger.info("APIClient initialized. Base URL: %s. Token acquired: %s",
                    self.base_url, self._token_acquired_at.isoformat())

    def _check_token_expiry(self): # This method seems to be from an older version/approach
        """
        DEPRECATED or INTERNAL USE ONLY if still relevant to a specific auth flow.
        Prefer _is_token_nearing_expiry and _ensure_valid_token for main logic.
        Check if the token is expired and refresh it if necessary.
        """
        # This method's original implementation in the prompt uses self.token_expiry_time
        # which is not set up by the new __init__. The new __init__ uses _token_lifetime.
        # If this method is still called, it needs to be aligned with _is_token_nearing_expiry logic.
        # For now, let's assume it's not the primary token check mechanism.
        # If it is, it needs to use self._token_acquired_at and self._token_lifetime.
        now = datetime.now(timezone.utc) # Use timezone.utc for direct comparison if needed
        
        # Calculate effective expiry based on acquisition time and lifetime
        # This is similar to _is_token_nearing_expiry logic
        safety_margin = timedelta(minutes=TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES)
        effective_expiry_time = self._token_acquired_at + self._token_lifetime - safety_margin

        if now >= effective_expiry_time:
            logger.info("Token is considered expired or nearing expiry by _check_token_expiry. Refreshing...")
            try:
                # This part needs to call the correct re-authentication mechanism
                # _perform_re_authentication_internal() is the Pydantic-aware one.
                self._perform_re_authentication_internal()
                logger.info(f"Token refreshed by _check_token_expiry. New expiry check time based on lifetime.")
            except Exception as e:
                logger.error(f"Failed to refresh token via _check_token_expiry: {e}", exc_info=True)
                raise AuthenticationError(f"Failed to refresh token: {e}") from e


    def _update_headers(self):
        """Updates session headers with the current token and standard headers."""
        self.session.headers.update({
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": f"tsxapipy/{library_version}"
        })

    @property # CHANGED: Made this a property
    def current_token(self) -> str:
        """
        Returns the current API token, ensuring it's valid and refreshed if necessary.
        This is the preferred way for external components (like DataStream) to get the token.
        """
        self._ensure_valid_token() # This handles expiry check and re-authentication
        return self._token

    def _is_token_nearing_expiry(self) -> bool:
        """Checks if the current token is nearing its expiry time."""
        safety_margin = timedelta(minutes=TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES)
        # Ensure _token_lifetime is a timedelta, which it is from __init__
        effective_expiry_time = self._token_acquired_at + self._token_lifetime - safety_margin
        is_nearing = datetime.now(UTC_TZ) >= effective_expiry_time
        if is_nearing:
            logger.info(
                "Token nearing expiry. Current time: %s, Effective token check time: %s",
                datetime.now(UTC_TZ).isoformat(),
                effective_expiry_time.isoformat()
            )
        return is_nearing

    def _perform_re_authentication_internal(self):
        """Performs full re-authentication using /api/Auth/loginKey."""
        logger.info("Attempting APIClient re-authentication using /api/Auth/loginKey...")
        if not self._reauth_username or not self._reauth_api_key:
            raise AuthenticationError(
                "Re-authentication credentials (USERNAME, API_KEY) not available to APIClient."
            )
        try:
            # Use the static method for initial authentication, assuming it's appropriate for re-auth with key
            # Or, if /loginKey is a simple POST, call _post_request directly.
            # Let's assume _post_request is the way for /loginKey.
            login_key_payload = schemas.AuthLoginKeyRequest(
                userName=self._reauth_username,
                apiKey=self._reauth_api_key
            ).model_dump(by_alias=True)

            response_data = self._post_request(
                "/api/Auth/loginKey",
                login_key_payload,
                is_validation_or_reauth_call=True # To bypass token check for this call
            )
            auth_response_model = schemas.AuthResponse.model_validate(response_data)

            if not auth_response_model.success or not auth_response_model.token:
                err_msg = auth_response_model.error_message or "Re-authentication with key failed or token missing."
                logger.error(f"APIClient re-authentication with key failed: {err_msg} (Code: {auth_response_model.error_code})")
                raise AuthenticationError(err_msg)
            
            self._token = auth_response_model.token
            self._token_acquired_at = datetime.now(UTC_TZ)
            self._update_headers()
            logger.info(
                "APIClient re-authenticated successfully with key. New token acquired at: %s",
                self._token_acquired_at.isoformat()
            )
        except (APIError, ValidationError) as e: # Catch API or Pydantic validation errors
            logger.error("APIClient re-authentication with key failed: %s", e)
            raise AuthenticationError(f"Re-authentication with key failed: {e}") from e


    def _validate_current_token_internal(self) -> bool:
        """Validates the current token via `/api/Auth/validate`."""
        logger.info("Validating current token via /api/Auth/validate...")
        response_dict: Optional[Dict[str, Any]] = None
        try:
            response_dict = self._post_request("/api/Auth/validate", {},
                                               is_validation_or_reauth_call=True,
                                               per_request_timeout_override=(self.connect_timeout, 10.0))
            
            validate_response_model = schemas.AuthValidateResponse.model_validate(response_dict)

            if validate_response_model.success:
                if validate_response_model.new_token and validate_response_model.new_token != self._token:
                    logger.info("New token received from /validate. Updating client token.")
                    self._token = validate_response_model.new_token
                    # Update acquired_at only if a new token is actually set
                    self._token_acquired_at = datetime.now(UTC_TZ) 
                    self._update_headers()
                else:
                    # Even if no new token, if validation is successful,
                    # consider the token "refreshed" in terms of its last known good state.
                    # However, this doesn't extend its actual expiry from the server's perspective.
                    # For simplicity, we might just update _token_acquired_at to reset the _is_token_nearing_expiry timer.
                    logger.debug("Token confirmed valid by /validate, no new token issued. Refreshing client-side acquisition time.")
                    self._token_acquired_at = datetime.now(UTC_TZ)
                return True
            
            logger.warning(
                "Token validation via /validate returned success=false. Error: %s",
                validate_response_model.error_message or 'Unknown validation error'
            )
            return False
        except ValidationError as e_val: # Pydantic validation error
            raw_text = str(response_dict) if response_dict else "N/A"
            logger.error("Failed to validate /Auth/validate response: %s. Raw response: %s", e_val, raw_text[:500])
            return False 
        except APIHttpError as e: 
            # Assuming APIHttpError has response_text attribute if it's a custom one
            response_text_detail = getattr(e, 'response_text', str(e))
            logger.warning(
                "Token validation failed with HTTP error %s. Assuming token is invalid. Details: %s",
                e.status_code, response_text_detail
            )
            return False
        except APIError as e: 
            logger.warning("Token validation API call failed: %s. Assuming token is invalid.", e)
            return False
        except Exception as e: 
            logger.error("Unexpected error during token validation process: %s", e, exc_info=True)
            return False 

    def _ensure_valid_token(self):
        """Ensures the current token is valid by checking expiry and re-authenticating if needed."""
        if self._is_token_nearing_expiry():
            logger.info("Token is nearing expiry. Proceeding with validation/re-authentication cycle...")
            # Attempt to validate first. If validation itself provides a new token or confirms validity, great.
            # If validation fails or indicates the token is bad, then do a full re-auth.
            if not self._validate_current_token_internal():
                logger.info("Token validation failed or indicated invalid. Performing full re-authentication with API key.")
                self._perform_re_authentication_internal() # This uses API key
            else:
                logger.info("Token successfully validated or refreshed via /validate endpoint.")
        else:
            logger.debug("Token is current and not nearing expiry. No validation/re-auth cycle triggered now.")


    def _post_request(self, endpoint: str, payload_dict: Dict[str, Any],
                      is_validation_or_reauth_call: bool = False,
                      per_request_timeout_override: Optional[Tuple[float, float]] = None
                     ) -> Dict[str, Any]: 
        """Internal method to make POST requests. Payload is already a dict."""
        if not is_validation_or_reauth_call:
            self._ensure_valid_token()

        request_url = f"{self.base_url}{endpoint}"
        action_desc = f"APIClient POST to {endpoint}"
        
        # Use instance's default timeouts if no override is provided
        effective_timeout = per_request_timeout_override if per_request_timeout_override is not None \
                            else (self.connect_timeout, self.read_timeout)

        log_auth_hdr_display = self.session.headers.get("Authorization","")
        if log_auth_hdr_display and len(log_auth_hdr_display) > 25 : 
            log_auth_hdr_display = log_auth_hdr_display[:25] + "..."

        logger.debug(
            "%s: URL='%s', Timeout=(%s, %s), TokenUsed: %s, PayloadKeys:'%s'",
            action_desc, request_url, effective_timeout[0], effective_timeout[1],
            log_auth_hdr_display or "None", list(payload_dict.keys())
        )

        response: Optional[requests.Response] = None
        try:
            response = self.session.post(request_url, json=payload_dict, timeout=effective_timeout)
            response.raise_for_status() 
            response_data = response.json() 

            if response_data.get("success") is True:
                logger.debug("%s successful (API indicated success:true).", action_desc)
                return response_data
            
            # Handle API returning success=false
            err_msg_api = response_data.get('errorMessage')
            err_code_api = response_data.get('errorCode')
            logger.error(
                "%s reported API failure (success:false): Msg='%s', Code='%s'. Full Response: %s",
                action_desc, err_msg_api or 'N/A', str(err_code_api), response_data
            )
            specific_exception = map_api_error_response(endpoint, response_data)
            if specific_exception:
                raise specific_exception
            # Fallback to generic APIResponseError if no specific mapping
            raise APIResponseError(
                err_msg_api or f"API call to {endpoint} returned success=false with unmapped errorCode: {str(err_code_api)}",
                error_code=err_code_api, # Pass error_code if available
                raw_response=response_data # Pass raw_response if available
            )
        except requests.exceptions.HTTPError as e:
            # Ensure err_text and resp_hdrs are defined before use in exception raising
            err_text = e.response.text if e.response is not None else "No response body"
            resp_hdrs = dict(e.response.headers) if e.response is not None else {}
            logger.error(
                "HTTP error during %s: Status Code %s - Response Text: %s",
                action_desc, e.response.status_code if e.response is not None else 'N/A', err_text
            )
            
            # Handle ASP.NET validation errors (HTTP 400)
            if e.response is not None and e.response.status_code == 400:
                try:
                    error_data_dict = e.response.json()
                    if "errors" in error_data_dict and "title" in error_data_dict and \
                       "validation errors occurred" in error_data_dict["title"].lower():
                        error_messages = [
                            f"Field '{field}': {'; '.join(messages)}"
                            for field, messages in error_data_dict.get("errors", {}).items()
                        ]
                        detailed_error_message = "API Validation Error: " + " | ".join(error_messages)
                        # Pass raw_response to InvalidParameterError
                        raise InvalidParameterError(
                            detailed_error_message,
                            error_code=e.response.status_code, raw_response=error_data_dict
                        ) from e
                    else: 
                        logger.warning("APIClient: HTTP 400 for %s, but not a recognized ASP.NET validation error structure. Body: %s",
                                       action_desc, err_text[:200])
                except (json.JSONDecodeError, TypeError, KeyError): # Added json.JSONDecodeError
                    logger.warning("APIClient: HTTP 400 for %s, JSON parsing of body failed or unexpected structure. Body: %s",
                                   action_desc, err_text[:200])

            # Handle 401 Unauthorized with re-authentication attempt
            if e.response is not None and e.response.status_code == 401 and not is_validation_or_reauth_call:
                logger.warning("Received 401 Unauthorized for %s. Attempting re-authentication.", action_desc)
                try:
                    self._perform_re_authentication_internal()
                    logger.info("Retrying %s once after successful re-authentication.", action_desc)
                    # After re-auth, call _post_request again for the original request
                    return self._post_request(endpoint, payload_dict, 
                                              is_validation_or_reauth_call=False, # It's the original request now
                                              per_request_timeout_override=effective_timeout)
                except AuthenticationError as reauth_err:
                    logger.error("Re-authentication after 401 failed for %s: %s", action_desc, reauth_err)
                    # Raise APIHttpError for the original 401, indicating re-auth also failed
                    raise APIHttpError(
                        e.response.status_code,
                        f"Original 401 error for {action_desc}, re-authentication also failed: {reauth_err}",
                        # Pass attributes to custom APIHttpError
                        # response_text=err_text, headers=resp_hdrs # Assuming APIHttpError accepts these
                    ) from e
            elif e.response is not None and e.response.status_code == 429: # Rate Limit
                retry_after_header = resp_hdrs.get('Retry-After')
                retry_secs = int(retry_after_header) if retry_after_header and retry_after_header.isdigit() else None
                raise RateLimitExceededError(
                    f"Rate limit (HTTP 429) for {action_desc}.",
                    error_code=429, # Pass error_code
                    # raw_response=None, headers=resp_hdrs, retry_after_seconds=retry_secs # Assuming RateLimitExceededError accepts these
                ) from e
            # General HTTP error if not specifically handled above
            raise APIHttpError(e.response.status_code if e.response is not None else 0, str(e), 
                               # response_text=err_text, headers=resp_hdrs # Assuming APIHttpError accepts these
                              ) from e
        except requests.exceptions.RetryError as e: # Should be rare if session doesn't have retries configured
            original_cause = e.args[0] if e.args else e 
            logger.error("Max retries exceeded for %s. Last error: %s", action_desc, original_cause)
            raise APIError(f"Max retries exceeded for {action_desc}. Last error: {original_cause}") from e
        except requests.exceptions.Timeout as e:
            logger.error("Timeout during %s to %s: %s", action_desc, request_url, e)
            raise APITimeoutError(f"Timeout during {action_desc}.") from e
        except requests.exceptions.RequestException as e: 
            logger.error("Request exception during %s: %s", action_desc, e, exc_info=True)
            raise APIError(f"Request exception during {action_desc}: {e}") from e
        except json.JSONDecodeError as e:
            resp_text = response.text if response and hasattr(response, "text") else "N/A"
            status_code_str = str(response.status_code) if response else 'N/A'
            logger.error(
                "Failed to decode JSON from %s (HTTP status %s): %s. Resp: %s",
                action_desc, status_code_str, e, resp_text[:500]
            )
            raise APIResponseParsingError( 
                f"Invalid JSON response from {action_desc} (HTTP status {status_code_str}).",
                raw_response_text=resp_text
            ) from e
        except APIError: # Re-raise specific APIErrors already raised
            raise 
        except Exception as e: # Catch-all for other unexpected errors
            logger.error("Unexpected Python error during %s: %s", action_desc, e, exc_info=True)
            raise APIError(f"Unexpected Python error processing API call for {action_desc}: {e}") from e

    # --- Public API Methods (Example: get_historical_bars with corrected timeout usage) ---

    def get_historical_bars(self, 
                            contract_id: Union[str, int], 
                            start_time_iso: str, 
                            end_time_iso: str,
                            unit: int, 
                            unit_number: int, 
                            limit: int,
                            account_id: Optional[int] = None, # <--- ADDED account_id parameter
                            include_partial_bar: bool = False, 
                            live: bool = False
                           ) -> schemas.HistoricalBarsResponse: 
        """Fetches historical bars, returning a Pydantic HistoricalBarsResponse model."""
        response_dict_hist_bars: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.HistoricalBarsRequest(
                accountId=account_id, # <--- PASS account_id
                contractId=contract_id, 
                startTime=start_time_iso, 
                endTime=end_time_iso,
                unit=unit, 
                unitNumber=unit_number, 
                limit=min(limit, MAX_BARS_PER_REQUEST),
                includePartialBar=include_partial_bar, 
                live=live
            )
            # Use model_dump with exclude_none=True to omit accountId if it's None
            payload_dict = request_model.model_dump(by_alias=True, exclude_none=True) 
            
            history_read_timeout = 45.0 # seconds
            response_dict_hist_bars = self._post_request(
                "/api/History/retrieveBars", 
                payload_dict,
                per_request_timeout_override=(self.connect_timeout, history_read_timeout)
            )
            
            response_model = schemas.HistoricalBarsResponse.model_validate(response_dict_hist_bars)
            if response_model.bars: 
                response_model.bars.reverse() 
            return response_model
        except ValidationError as e_val: 
            raw_text_for_error = str(response_dict_hist_bars) if response_dict_hist_bars else "Response dictionary not available"
            raise APIResponseParsingError(f"Failed to parse HistoricalBarsResponse: {e_val}", raw_response_text=raw_text_for_error) from e_val
        except APIError: 
            raise
        except Exception as e_generic: 
            logger.error(f"Unexpected error in get_historical_bars: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in get_historical_bars: {e_generic}") from e_generic
        
    # Ensure all other public methods (get_accounts, search_contracts, place_order, etc.)
    # also have `except ValidationError as e_val:` where they call `model_validate`
    # and pass `raw_response_text` to APIResponseParsingError correctly.

    # Placeholder for other methods from the original file to maintain line count:
    def get_accounts(self, only_active: bool = True) -> List[schemas.Account]:
        response_dict: Optional[Dict[str,Any]] = None
        try:
            # ... (request model creation and _post_request call) ...
            request_model = schemas.AccountSearchRequest(onlyActiveAccounts=only_active)
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict = self._post_request("/api/Account/search", payload_dict)
            response_model = schemas.AccountSearchResponse.model_validate(response_dict)
            return response_model.accounts
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse AccountSearchResponse: {e_val}", raw_response_text=str(response_dict)) from e_val
        except APIError: raise
        except Exception as e_generic: logger.error(f"Err get_accounts: {e_generic}"); raise APIError(f"Err: {e_generic}")

    def search_contracts(self, search_text: str, live: bool = False) -> List[schemas.Contract]:
        response_dict: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.ContractSearchRequest(searchText=search_text, live=live)
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict = self._post_request("/api/Contract/search", payload_dict)
            response_model = schemas.ContractSearchResponse.model_validate(response_dict)
            return response_model.contracts
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse ContractSearchResponse: {e_val}", raw_response_text=str(response_dict)) from e_val
        except APIError: raise
        except Exception as e_generic: logger.error(f"Err search_contracts: {e_generic}"); raise APIError(f"Err: {e_generic}")

    def search_contract_by_id(self, contract_id: str) -> List[schemas.Contract]:
        response_dict: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.ContractSearchByIdRequest(contractId=contract_id)
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict = self._post_request("/api/Contract/searchById", payload_dict)
            response_model = schemas.ContractSearchResponse.model_validate(response_dict)
            return response_model.contracts
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse ContractSearchResponse (by ID): {e_val}", raw_response_text=str(response_dict)) from e_val
        except APIError: raise
        except Exception as e_generic: logger.error(f"Err search_contract_by_id: {e_generic}"); raise APIError(f"Err: {e_generic}")
    
    def place_order(self, order_payload_model: schemas.OrderBase) -> schemas.OrderPlacementResponse:
        if not isinstance(order_payload_model, schemas.OrderBase):
            raise TypeError("place_order expects a Pydantic OrderBase derived model instance.")
        response_dict: Optional[Dict[str,Any]] = None
        try:
            payload_dict = order_payload_model.model_dump(by_alias=True, exclude_none=True)
            response_dict = self._post_request("/api/Order/place", payload_dict)
            return schemas.OrderPlacementResponse.model_validate(response_dict)
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse OrderPlacementResponse: {e_val}", raw_response_text=str(response_dict)) from e_val
        except APIError: raise
        except Exception as e_generic: logger.error(f"Err place_order: {e_generic}"); raise APIError(f"Err: {e_generic}")

    def cancel_order(self, account_id: int, order_id: int) -> schemas.CancelOrderResponse:
        response_dict: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.CancelOrderRequest(accountId=account_id, orderId=order_id)
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict = self._post_request("/api/Order/cancel", payload_dict)
            return schemas.CancelOrderResponse.model_validate(response_dict)
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse CancelOrderResponse: {e_val}", raw_response_text=str(response_dict)) from e_val
        except APIError: raise
        except Exception as e_generic: logger.error(f"Err cancel_order: {e_generic}"); raise APIError(f"Err: {e_generic}")

    def modify_order(self, modification_request_model: schemas.ModifyOrderRequest) -> schemas.ModifyOrderResponse:
        if not isinstance(modification_request_model, schemas.ModifyOrderRequest):
             raise TypeError("modify_order expects a Pydantic ModifyOrderRequest model instance.")
        response_dict: Optional[Dict[str,Any]] = None
        try:
            payload_dict = modification_request_model.model_dump(by_alias=True, exclude_none=True)
            response_dict = self._post_request("/api/Order/modify", payload_dict)
            return schemas.ModifyOrderResponse.model_validate(response_dict)
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse ModifyOrderResponse: {e_val}", raw_response_text=str(response_dict)) from e_val
        except APIError: raise
        except Exception as e_generic: logger.error(f"Err modify_order: {e_generic}"); raise APIError(f"Err: {e_generic}")

    def search_orders(self, account_id: int, start_timestamp_iso: str,
                      end_timestamp_iso: Optional[str] = None) -> List[schemas.OrderDetails]:
        response_dict: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.OrderSearchRequest(
                accountId=account_id, startTimestamp=start_timestamp_iso, endTimestamp=end_timestamp_iso
            )
            payload_dict = request_model.model_dump(by_alias=True, exclude_none=True)
            response_dict = self._post_request("/api/Order/search", payload_dict)
            response_model = schemas.OrderSearchResponse.model_validate(response_dict)
            return response_model.orders
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse OrderSearchResponse: {e_val}", raw_response_text=str(response_dict)) from e_val
        except APIError: raise
        except Exception as e_generic: logger.error(f"Err search_orders: {e_generic}"); raise APIError(f"Err: {e_generic}")

    def close_contract_position(self, account_id: int, contract_id: str) -> schemas.PositionManagementResponse:
        response_dict: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.CloseContractPositionRequest(accountId=account_id, contractId=contract_id)
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict = self._post_request("/api/Position/closeContract", payload_dict)
            return schemas.PositionManagementResponse.model_validate(response_dict)
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse PositionManagementResponse (close): {e_val}", raw_response_text=str(response_dict)) from e_val
        except APIError: raise
        except Exception as e_generic: logger.error(f"Err close_contract_position: {e_generic}"); raise APIError(f"Err: {e_generic}")

    def partial_close_contract_position(self, account_id: int, contract_id: str,
                                        size: int) -> schemas.PositionManagementResponse:
        response_dict: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.PartialCloseContractPositionRequest(
                accountId=account_id, contractId=contract_id, size=size
            )
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict = self._post_request("/api/Position/partialCloseContract", payload_dict)
            return schemas.PositionManagementResponse.model_validate(response_dict)
        except ValidationError as e_val: 
            if "size" in str(e_val).lower(): 
                raise InvalidParameterError(f"Invalid size for partial close: {size}. Details: {e_val}") from e_val
            raise APIResponseParsingError(f"Failed to parse PositionManagementResponse (partial close): {e_val}", raw_response_text=str(response_dict)) from e_val
        except APIError: raise
        except Exception as e_generic: logger.error(f"Err partial_close_contract_position: {e_generic}"); raise APIError(f"Err: {e_generic}")

    def search_open_positions(self, account_id: int) -> List[schemas.Position]:
        response_dict: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.SearchOpenPositionsRequest(accountId=account_id)
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict = self._post_request("/api/Position/searchOpen", payload_dict)
            response_model = schemas.SearchOpenPositionsResponse.model_validate(response_dict)
            return response_model.positions
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse SearchOpenPositionsResponse: {e_val}", raw_response_text=str(response_dict)) from e_val
        except APIError: raise
        except Exception as e_generic: logger.error(f"Err search_open_positions: {e_generic}"); raise APIError(f"Err: {e_generic}")

    def search_trades(self, account_id: int, start_timestamp_iso: str,
                      end_timestamp_iso: Optional[str] = None) -> List[schemas.Trade]:
        response_dict: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.TradeSearchRequest(
                accountId=account_id, startTimestamp=start_timestamp_iso, endTimestamp=end_timestamp_iso
            )
            payload_dict = request_model.model_dump(by_alias=True, exclude_none=True)
            response_dict = self._post_request("/api/Trade/search", payload_dict)
            response_model = schemas.TradeSearchResponse.model_validate(response_dict)
            return response_model.trades
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse TradeSearchResponse: {e_val}", raw_response_text=str(response_dict)) from e_val
        except APIError: raise
        except Exception as e_generic: logger.error(f"Err search_trades: {e_generic}"); raise APIError(f"Err: {e_generic}")

    # REMOVED: Old get_candles and _make_request methods as they were causing 404s
    # and are superseded by the Pydantic-based get_historical_bars and _post_request.
    # REMOVED: Old _ensure_authenticated as APIClient is initialized with a token.

    @staticmethod
    def initial_authenticate_app(username: str, password: str, app_id: str, verify_key: str,
                                 api_url: Optional[str] = None) -> schemas.AuthResponse:
        """Authenticates as an authorized application, returning a Pydantic AuthResponse model."""
        # (Code for this method remains as it was, assuming it's correct for its purpose
        # and that schemas.AuthLoginAppRequest and schemas.AuthResponse are correct)
        try:
            request_model = schemas.AuthLoginAppRequest(
                userName=username, password=password, appId=app_id, verifyKey=verify_key
            )
        except ValidationError as e_val: # Pydantic validation for request model
            logger.error("Validation error creating AuthLoginAppRequest: %s", e_val)
            raise AuthenticationError(f"Invalid parameters for app authentication request: {e_val}") from e_val

        payload_dict = request_model.model_dump(by_alias=True)
        base_url = api_url if api_url else API_URL # Use API_URL from config
        request_url = f"{base_url}/api/Auth/loginApp"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        action_desc = "Static Initial App Authentication"
        logger.debug("%s: URL='%s'", action_desc, request_url)
        response: Optional[requests.Response] = None
        try:
            # This is a direct requests.post, not using self.session or self._post_request
            # because it's a static method, possibly called before an APIClient instance exists.
            response = requests.post(request_url, headers=headers, json=payload_dict, timeout=15)
            response.raise_for_status()
            response_data_dict = response.json()
            
            auth_response_model = schemas.AuthResponse.model_validate(response_data_dict)

            if auth_response_model.success and auth_response_model.token:
                logger.info("%s successful.", action_desc)
                return auth_response_model
            
            default_msg = (f"App login failed (API errorCode: {auth_response_model.error_code}) "
                           "or token missing in response.")
            final_err_msg = auth_response_model.error_message or default_msg
            logger.error("%s failed: %s. Response: %s", action_desc, final_err_msg, auth_response_model)
            raise AuthenticationError(final_err_msg)
        except ValidationError as e_resp_val: # Pydantic validation for response model
            raw_text_for_error = response.text[:500] if response and response.text else "No response text available"
            logger.error("Failed to validate app authentication response: %s. Raw: %s", e_resp_val, raw_text_for_error)
            raise AuthenticationError(f"Invalid app authentication response structure: {e_resp_val}") from e_resp_val
        except requests.exceptions.HTTPError as e:
            err_text = e.response.text if e.response is not None else "No response body"
            http_err_msg = f"HTTP error during {action_desc}: {e.response.status_code if e.response is not None else 'N/A'}"
            if err_text: http_err_msg += f" - {err_text}"
            raise AuthenticationError(http_err_msg) from e
        except requests.exceptions.JSONDecodeError as e: # Corrected to requests.exceptions
            response_text = response.text if response else 'No response'
            logger.error("Failed to decode JSON response during %s: %s. Text: %s", action_desc, e, response_text, exc_info=True)
            raise AuthenticationError(f"Invalid JSON response during app authentication: {e}") from e
        except Exception as e_auth_app: # pylint: disable=broad-except
            raise AuthenticationError(f"Unexpected error during {action_desc}: {e_auth_app}") from e_auth_app