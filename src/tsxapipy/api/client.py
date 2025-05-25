# tsxapipy/api/client.py
"""
This module provides the APIClient class for interacting with the TopStepX HTTP REST API,
including authentication, request handling, error mapping, and Pydantic-based
request/response validation.
"""
import json
import logging
from typing import Dict, Any, Optional, List, Tuple, Union # Added Union
from datetime import datetime, timedelta

import requests
from urllib3.util.retry import Retry # type: ignore[import-untyped]
from requests.adapters import HTTPAdapter # type: ignore[import-untyped]
from pydantic import ValidationError # For handling Pydantic validation errors


# Import __version__ from the main tsxapipy package
try:
    from .. import __version__ as library_version
except ImportError:
    library_version = "0.0.0-unknown" # Default fallback
    logging.getLogger(__name__).warning("Could not import __version__ from tsxapipy. Using fallback version.")

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
    InvalidParameterError, RateLimitExceededError, APIResponseParsingError # Added APIResponseParsingError
)
from .error_mapper import map_api_error_response
from ..api import schemas # Import Pydantic schemas

logger = logging.getLogger(__name__)

MAX_BARS_PER_REQUEST = 1000

def _perform_initial_authentication(username: str, api_key: str,
                                    api_url_override: Optional[str] = None) -> schemas.AuthResponse:
    """
    Performs initial key-based authentication with the /api/Auth/loginKey endpoint.
    Returns a Pydantic AuthResponse model.
    """
    try:
        request_model = schemas.AuthLoginKeyRequest(userName=username, apiKey=api_key)
    except ValidationError as e_val:
        logger.error("Validation error creating AuthLoginKeyRequest: %s", e_val)
        raise AuthenticationError(f"Invalid parameters for authentication request: {e_val}") from e_val

    payload = request_model.model_dump(by_alias=True) 

    base_url = api_url_override if api_url_override else API_URL
    request_url = f"{base_url}/api/Auth/loginKey"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    action_desc = "Standalone Initial Key Authentication"
    logger.debug("%s: URL='%s'", action_desc, request_url)
    response: Optional[requests.Response] = None
    try:
        response = requests.post(request_url, headers=headers, json=payload, timeout=15)
        response.raise_for_status() 
        response_data_dict = response.json() 

        auth_response_model = schemas.AuthResponse.model_validate(response_data_dict)

        if auth_response_model.success and auth_response_model.token:
            logger.info("%s successful.", action_desc)
            return auth_response_model
        
        default_msg = (
            f"Key-based login failed (API errorCode: {auth_response_model.error_code}) "
            "or token missing/null in response."
        )
        final_err_msg = auth_response_model.error_message or default_msg
        logger.error("%s failed: %s. API Response Model: %s", action_desc, final_err_msg, auth_response_model)
        raise AuthenticationError(final_err_msg)

    except ValidationError as e_resp_val:
        logger.error("Failed to validate authentication response against Pydantic model: %s. Raw response: %s",
                     e_resp_val, response.text[:500] if response else "N/A")
        raise AuthenticationError(f"Invalid authentication response structure: {e_resp_val}") from e_resp_val
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
        response_text = response.text if response else 'No response'
        logger.error(
            "Failed to decode JSON response during %s: %s. Response text: %s",
            action_desc, e, response_text,
            exc_info=True
        )
        raise AuthenticationError(f"Invalid JSON response received during authentication: {e}") from e
    except requests.exceptions.RequestException as e: 
        logger.error("Network or request exception during %s: %s", action_desc, e, exc_info=True)
        raise AuthenticationError(f"Network or request exception during authentication: {e}") from e


# pylint: disable=too-many-instance-attributes, too-many-arguments
class APIClient:
    """
    A client for interacting with the TopStep (ProjectX) HTTP REST API.
    Manages session token, re-authentication, request retries, and error mapping.
    Uses Pydantic models for request/response validation and data structuring.
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
            allowed_methods=["POST"] 
        )
        adapter = HTTPAdapter(pool_connections=pool_connections,
                              pool_maxsize=pool_maxsize, max_retries=retry_strategy)
        if self.base_url.startswith("https://"):
            self.session.mount("https://", adapter)
        elif self.base_url.startswith("http://"):
            self.session.mount("http://", adapter)
        else:
            logger.warning("Base URL '%s' lacks scheme, defaulting to HTTPS for session adapter.", self.base_url)
            self.session.mount("https://", adapter) 

        self._update_headers()
        next_reval_check = (self._token_acquired_at + self._token_lifetime -
                            timedelta(minutes=TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES))
        logger.info(
            "APIClient initialized. Token acquired: %s. Next reval check (approx): %s",
            self._token_acquired_at.isoformat(),
            next_reval_check.isoformat()
        )

    def _update_headers(self):
        """Updates session headers with the current token and standard headers."""
        self.session.headers.update({
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": f"tsxapipy/{library_version}"
        })

    @property
    def current_token(self) -> str:
        """Returns the current valid session token, re-authenticating if necessary."""
        self._ensure_valid_token()
        return self._token

    def _is_token_nearing_expiry(self) -> bool:
        """Checks if the current token is nearing its expiry time."""
        safety_margin = timedelta(minutes=TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES)
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
        """Performs full re-authentication."""
        logger.info("Attempting APIClient re-authentication...")
        if not self._reauth_username or not self._reauth_api_key:
            raise AuthenticationError(
                "Re-authentication credentials (USERNAME, API_KEY) not available to APIClient."
            )
        try:
            auth_response_model = _perform_initial_authentication(
                self._reauth_username, self._reauth_api_key, self.base_url
            )
            if not auth_response_model.token: 
                raise AuthenticationError("Re-authentication response model missing token or token is null.")
            
            self._token = auth_response_model.token
            self._token_acquired_at = datetime.now(UTC_TZ)
            self._update_headers()
            logger.info(
                "APIClient re-authenticated successfully. New token acquired at: %s",
                self._token_acquired_at.isoformat()
            )
        except AuthenticationError as e:
            logger.error("APIClient re-authentication failed: %s", e)
            raise

    def _validate_current_token_internal(self) -> bool:
        """Validates the current token via `/api/Auth/validate`."""
        logger.info("Validating current token via /api/Auth/validate...")
        response_dict: Optional[Dict[str, Any]] = None # Initialize for potential use in except block
        try:
            response_dict = self._post_request("/api/Auth/validate", {},
                                               is_validation_or_reauth_call=True,
                                               per_request_timeout_override=(self.connect_timeout, 10.0))
            
            validate_response_model = schemas.AuthValidateResponse.model_validate(response_dict)

            if validate_response_model.success:
                if validate_response_model.new_token and validate_response_model.new_token != self._token:
                    logger.info("New token received from /validate. Updating client token.")
                    self._token = validate_response_model.new_token
                    self._token_acquired_at = datetime.now(UTC_TZ)
                    self._update_headers()
                else:
                    logger.debug("Token confirmed valid by /validate, no new token issued. Refreshing acquisition time.")
                    self._token_acquired_at = datetime.now(UTC_TZ)
                return True
            
            logger.warning(
                "Token validation via /validate returned success=false. Error: %s",
                validate_response_model.error_message or 'Unknown validation error'
            )
            return False
        except ValidationError as e_val:
            raw_text = str(response_dict) if response_dict else "N/A"
            logger.error("Failed to validate /Auth/validate response: %s. Raw response: %s", e_val, raw_text[:500])
            return False 
        except APIHttpError as e: 
            logger.warning(
                "Token validation failed with HTTP error %s. Assuming token is invalid. Details: %s",
                e.status_code, e.response_text
            )
            return False
        except APIError as e: 
            logger.warning("Token validation API call failed: %s. Assuming token is invalid.", e)
            return False
        except Exception as e: 
            logger.error("Unexpected error during token validation process: %s", e, exc_info=True)
            return False 

    def _ensure_valid_token(self):
        """Ensures the current token is valid."""
        if self._is_token_nearing_expiry():
            logger.info("Token is nearing expiry. Proceeding with validation/re-authentication cycle...")
            if not self._validate_current_token_internal():
                logger.info("Token validation failed or indicated invalid. Performing full re-authentication.")
                self._perform_re_authentication_internal()
            else:
                logger.info("Token successfully validated or refreshed via /validate.")
        else:
            logger.debug("Token is current and not nearing expiry. No validation cycle triggered.")

    def _post_request(self, endpoint: str, payload_dict: Dict[str, Any],
                      is_validation_or_reauth_call: bool = False,
                      per_request_timeout_override: Optional[Tuple[float, float]] = None
                     ) -> Dict[str, Any]: 
        """Internal method to make POST requests. Payload is already a dict."""
        if not is_validation_or_reauth_call:
            self._ensure_valid_token()

        request_url = f"{self.base_url}{endpoint}"
        action_desc = f"APIClient POST to {endpoint}"
        effective_timeout = per_request_timeout_override or \
                            (self.connect_timeout, self.read_timeout)

        log_auth_hdr_display = self.session.headers.get("Authorization","")
        if log_auth_hdr_display and len(log_auth_hdr_display) > 25 : 
            log_auth_hdr_display = log_auth_hdr_display[:25] + "..."

        logger.debug(
            "%s: URL='%s', TokenUsed: %s, PayloadKeys:'%s'",
            action_desc, request_url, log_auth_hdr_display or "None", list(payload_dict.keys())
        )

        response: Optional[requests.Response] = None
        try:
            response = self.session.post(request_url, json=payload_dict, timeout=effective_timeout)
            response.raise_for_status() 
            response_data = response.json() 

            if response_data.get("success") is True:
                logger.debug("%s successful (API indicated success:true).", action_desc)
                return response_data
            
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
                "HTTP error during %s: Status Code %s - Response Text: %s",
                action_desc, e.response.status_code, err_text
            )
            if e.response.status_code == 400:
                try:
                    error_data_dict = e.response.json()
                    if "errors" in error_data_dict and "title" in error_data_dict and \
                       "validation errors occurred" in error_data_dict["title"].lower():
                        error_messages = [
                            f"Field '{field}': {'; '.join(messages)}"
                            for field, messages in error_data_dict.get("errors", {}).items()
                        ]
                        detailed_error_message = "API Validation Error: " + " | ".join(error_messages)
                        raise InvalidParameterError(
                            detailed_error_message,
                            error_code=e.response.status_code, raw_response=error_data_dict
                        ) from e
                    else: 
                        logger.warning("APIClient: HTTP 400 for %s, but not a recognized ASP.NET validation error structure. Body: %s",
                                       action_desc, err_text[:200])
                except (json.JSONDecodeError, TypeError, KeyError):
                    logger.warning("APIClient: HTTP 400 for %s, JSON parsing of body failed or unexpected structure. Body: %s",
                                   action_desc, err_text[:200])

            if e.response.status_code == 401 and not is_validation_or_reauth_call:
                logger.warning("Received 401 Unauthorized. Attempting re-authentication.")
                try:
                    self._perform_re_authentication_internal()
                    logger.info("Retrying %s once after re-authentication.", action_desc)
                    return self._post_request(endpoint, payload_dict, True,
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
            status_code = response.status_code if response else 'N/A'
            logger.error(
                "Failed to decode JSON from %s (HTTP status %s): %s. Resp: %s",
                action_desc, status_code, e, resp_text[:500]
            )
            raise APIResponseParsingError( 
                f"Invalid JSON response from {action_desc} (HTTP status {status_code}).",
                raw_response_text=resp_text
            ) from e
        except APIError: raise 
        except Exception as e: 
            logger.error("Unexpected Python error during %s: %s", action_desc, e, exc_info=True)
            raise APIError(f"Unexpected Python error processing API call for {action_desc}: {e}") from e

    # --- Public API Methods ---

    def get_accounts(self, only_active: bool = True) -> List[schemas.Account]:
        """Fetches accounts, returning a list of Pydantic Account models."""
        response_dict_get_acc: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.AccountSearchRequest(onlyActiveAccounts=only_active)
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict_get_acc = self._post_request("/api/Account/search", payload_dict)
            response_model = schemas.AccountSearchResponse.model_validate(response_dict_get_acc)
            return response_model.accounts
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse AccountSearchResponse: {e_val}", raw_response_text=str(response_dict_get_acc)) from e_val
        except APIError: 
            raise
        except Exception as e_generic: 
            logger.error(f"Unexpected error in get_accounts: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in get_accounts: {e_generic}") from e_generic


    def search_contracts(self, search_text: str, live: bool = False) -> List[schemas.Contract]:
        """Searches contracts, returning a list of Pydantic Contract models."""
        response_dict_search_cont: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.ContractSearchRequest(searchText=search_text, live=live)
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict_search_cont = self._post_request("/api/Contract/search", payload_dict)
            response_model = schemas.ContractSearchResponse.model_validate(response_dict_search_cont)
            return response_model.contracts
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse ContractSearchResponse: {e_val}", raw_response_text=str(response_dict_search_cont)) from e_val
        except APIError: raise
        except Exception as e_generic:
            logger.error(f"Unexpected error in search_contracts: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in search_contracts: {e_generic}") from e_generic

    def search_contract_by_id(self, contract_id: str) -> List[schemas.Contract]:
        """Fetches a specific contract by ID, returning a list of Pydantic Contract models."""
        response_dict_search_id: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.ContractSearchByIdRequest(contractId=contract_id)
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict_search_id = self._post_request("/api/Contract/searchById", payload_dict)
            response_model = schemas.ContractSearchResponse.model_validate(response_dict_search_id)
            return response_model.contracts
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse ContractSearchResponse (by ID): {e_val}", raw_response_text=str(response_dict_search_id)) from e_val
        except APIError: raise
        except Exception as e_generic:
            logger.error(f"Unexpected error in search_contract_by_id: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in search_contract_by_id: {e_generic}") from e_generic

    def get_historical_bars(self, contract_id: Union[str, int], start_time_iso: str, end_time_iso: str,
                            unit: int, unit_number: int, limit: int,
                            include_partial_bar: bool = False, live: bool = False
                           ) -> schemas.HistoricalBarsResponse: 
        """Fetches historical bars, returning a Pydantic HistoricalBarsResponse model."""
        response_dict_hist_bars: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.HistoricalBarsRequest(
                contractId=contract_id, startTime=start_time_iso, endTime=end_time_iso,
                unit=unit, unitNumber=unit_number, limit=min(limit, MAX_BARS_PER_REQUEST),
                includePartialBar=include_partial_bar, live=live
            )
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict_hist_bars = self._post_request("/api/History/retrieveBars", payload_dict,
                                               per_request_timeout_override=(self.connect_timeout, 45.0))
            response_model = schemas.HistoricalBarsResponse.model_validate(response_dict_hist_bars)
            if response_model.bars: # APIClient is responsible for reversing if API sends newest first
                response_model.bars.reverse() 
            return response_model
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse HistoricalBarsResponse: {e_val}", raw_response_text=str(response_dict_hist_bars)) from e_val
        except APIError: raise
        except Exception as e_generic:
            logger.error(f"Unexpected error in get_historical_bars: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in get_historical_bars: {e_generic}") from e_generic

    def place_order(self, order_payload_model: schemas.OrderBase) -> schemas.OrderPlacementResponse:
        """Places an order using a Pydantic order request model."""
        if not isinstance(order_payload_model, schemas.OrderBase):
            # This check could be more specific if place_order should only accept certain subclasses
            raise TypeError("place_order expects a Pydantic OrderBase derived model instance.")
        response_dict_place_order: Optional[Dict[str,Any]] = None
        try:
            payload_dict = order_payload_model.model_dump(by_alias=True, exclude_none=True)
            response_dict_place_order = self._post_request("/api/Order/place", payload_dict)
            return schemas.OrderPlacementResponse.model_validate(response_dict_place_order)
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse OrderPlacementResponse: {e_val}", raw_response_text=str(response_dict_place_order)) from e_val
        except APIError: raise
        except Exception as e_generic:
            logger.error(f"Unexpected error in place_order: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in place_order: {e_generic}") from e_generic


    def cancel_order(self, account_id: int, order_id: int) -> schemas.CancelOrderResponse:
        """Cancels an order."""
        response_dict_cancel: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.CancelOrderRequest(accountId=account_id, orderId=order_id)
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict_cancel = self._post_request("/api/Order/cancel", payload_dict)
            return schemas.CancelOrderResponse.model_validate(response_dict_cancel)
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse CancelOrderResponse: {e_val}", raw_response_text=str(response_dict_cancel)) from e_val
        except APIError: raise
        except Exception as e_generic:
            logger.error(f"Unexpected error in cancel_order: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in cancel_order: {e_generic}") from e_generic

    def modify_order(self, modification_request_model: schemas.ModifyOrderRequest) -> schemas.ModifyOrderResponse:
        """Modifies an order using a Pydantic ModifyOrderRequest model."""
        if not isinstance(modification_request_model, schemas.ModifyOrderRequest):
             raise TypeError("modify_order expects a Pydantic ModifyOrderRequest model instance.")
        response_dict_modify: Optional[Dict[str,Any]] = None
        try:
            payload_dict = modification_request_model.model_dump(by_alias=True, exclude_none=True)
            response_dict_modify = self._post_request("/api/Order/modify", payload_dict)
            return schemas.ModifyOrderResponse.model_validate(response_dict_modify)
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse ModifyOrderResponse: {e_val}", raw_response_text=str(response_dict_modify)) from e_val
        except APIError: raise
        except Exception as e_generic:
            logger.error(f"Unexpected error in modify_order: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in modify_order: {e_generic}") from e_generic

    def search_orders(self, account_id: int, start_timestamp_iso: str,
                      end_timestamp_iso: Optional[str] = None) -> List[schemas.OrderDetails]:
        """Searches orders, returning a list of Pydantic OrderDetails models."""
        response_dict_search_ord: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.OrderSearchRequest(
                accountId=account_id, startTimestamp=start_timestamp_iso, endTimestamp=end_timestamp_iso
            )
            payload_dict = request_model.model_dump(by_alias=True, exclude_none=True)
            response_dict_search_ord = self._post_request("/api/Order/search", payload_dict)
            response_model = schemas.OrderSearchResponse.model_validate(response_dict_search_ord)
            return response_model.orders
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse OrderSearchResponse: {e_val}", raw_response_text=str(response_dict_search_ord)) from e_val
        except APIError: raise
        except Exception as e_generic:
            logger.error(f"Unexpected error in search_orders: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in search_orders: {e_generic}") from e_generic

    def close_contract_position(self, account_id: int, contract_id: str) -> schemas.PositionManagementResponse:
        """Closes a contract position."""
        response_dict_close_pos: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.CloseContractPositionRequest(accountId=account_id, contractId=contract_id)
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict_close_pos = self._post_request("/api/Position/closeContract", payload_dict)
            return schemas.PositionManagementResponse.model_validate(response_dict_close_pos)
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse PositionManagementResponse (close): {e_val}", raw_response_text=str(response_dict_close_pos)) from e_val
        except APIError: raise
        except Exception as e_generic:
            logger.error(f"Unexpected error in close_contract_position: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in close_contract_position: {e_generic}") from e_generic

    def partial_close_contract_position(self, account_id: int, contract_id: str,
                                        size: int) -> schemas.PositionManagementResponse:
        """Partially closes a contract position."""
        response_dict_partial_close: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.PartialCloseContractPositionRequest(
                accountId=account_id, contractId=contract_id, size=size
            )
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict_partial_close = self._post_request("/api/Position/partialCloseContract", payload_dict)
            return schemas.PositionManagementResponse.model_validate(response_dict_partial_close)
        except ValidationError as e_val: 
            if "size" in str(e_val).lower(): 
                raise InvalidParameterError(f"Invalid size for partial close: {size}. Details: {e_val}") from e_val
            raise APIResponseParsingError(f"Failed to parse PositionManagementResponse (partial close): {e_val}", raw_response_text=str(response_dict_partial_close)) from e_val
        except APIError: raise
        except Exception as e_generic:
            logger.error(f"Unexpected error in partial_close_contract_position: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in partial_close_contract_position: {e_generic}") from e_generic


    def search_open_positions(self, account_id: int) -> List[schemas.Position]:
        """Searches open positions, returning a list of Pydantic Position models."""
        response_dict_open_pos: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.SearchOpenPositionsRequest(accountId=account_id)
            payload_dict = request_model.model_dump(by_alias=True)
            response_dict_open_pos = self._post_request("/api/Position/searchOpen", payload_dict)
            response_model = schemas.SearchOpenPositionsResponse.model_validate(response_dict_open_pos)
            return response_model.positions
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse SearchOpenPositionsResponse: {e_val}", raw_response_text=str(response_dict_open_pos)) from e_val
        except APIError: raise
        except Exception as e_generic:
            logger.error(f"Unexpected error in search_open_positions: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in search_open_positions: {e_generic}") from e_generic

    def search_trades(self, account_id: int, start_timestamp_iso: str,
                      end_timestamp_iso: Optional[str] = None) -> List[schemas.Trade]:
        """Searches trades, returning a list of Pydantic Trade models."""
        response_dict_search_trades: Optional[Dict[str,Any]] = None
        try:
            request_model = schemas.TradeSearchRequest(
                accountId=account_id, startTimestamp=start_timestamp_iso, endTimestamp=end_timestamp_iso
            )
            payload_dict = request_model.model_dump(by_alias=True, exclude_none=True)
            response_dict_search_trades = self._post_request("/api/Trade/search", payload_dict)
            response_model = schemas.TradeSearchResponse.model_validate(response_dict_search_trades)
            return response_model.trades
        except ValidationError as e_val:
            raise APIResponseParsingError(f"Failed to parse TradeSearchResponse: {e_val}", raw_response_text=str(response_dict_search_trades)) from e_val
        except APIError: raise
        except Exception as e_generic:
            logger.error(f"Unexpected error in search_trades: {e_generic}", exc_info=True)
            raise APIError(f"Unexpected error in search_trades: {e_generic}") from e_generic

    @staticmethod
    def initial_authenticate_app(username: str, password: str, app_id: str, verify_key: str,
                                 api_url: Optional[str] = None) -> schemas.AuthResponse:
        """Authenticates as an authorized application, returning a Pydantic AuthResponse model."""
        try:
            request_model = schemas.AuthLoginAppRequest(
                userName=username, password=password, appId=app_id, verifyKey=verify_key
            )
        except ValidationError as e_val:
            logger.error("Validation error creating AuthLoginAppRequest: %s", e_val)
            raise AuthenticationError(f"Invalid parameters for app authentication request: {e_val}") from e_val

        payload_dict = request_model.model_dump(by_alias=True)
        base_url = api_url if api_url else API_URL
        request_url = f"{base_url}/api/Auth/loginApp"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        action_desc = "Static Initial App Authentication"
        logger.debug("%s: URL='%s'", action_desc, request_url)
        response: Optional[requests.Response] = None
        try:
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
        except ValidationError as e_resp_val:
            logger.error("Failed to validate app authentication response: %s. Raw: %s", e_resp_val, response.text[:500] if response else "N/A")
            raise AuthenticationError(f"Invalid app authentication response structure: {e_resp_val}") from e_resp_val
        except requests.exceptions.HTTPError as e:
            err_text = e.response.text if e.response is not None else "No response body"
            http_err_msg = f"HTTP error during {action_desc}: {e.response.status_code}"
            if err_text: http_err_msg += f" - {err_text}"
            raise AuthenticationError(http_err_msg) from e
        except requests.exceptions.JSONDecodeError as e:
            response_text = response.text if response else 'No response'
            logger.error("Failed to decode JSON response during %s: %s. Text: %s", action_desc, e, response_text, exc_info=True)
            raise AuthenticationError(f"Invalid JSON response during app authentication: {e}") from e
        except Exception as e_auth_app: # pylint: disable=broad-except
            raise AuthenticationError(f"Unexpected error during {action_desc}: {e_auth_app}") from e_auth_app