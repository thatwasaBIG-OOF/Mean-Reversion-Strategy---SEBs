# tsxapipy/api/contract_utils.py
"""
Utilities for determining and managing futures contract identifiers,
including roll logic and API searches.
"""
import logging
from datetime import date
from typing import Dict, Tuple, Optional, List, Any

from tsxapipy.api.client import APIClient
from tsxapipy.api.exceptions import APIError, APIResponseParsingError # Added APIResponseParsingError
from tsxapipy.api import schemas # Import Pydantic schemas

logger = logging.getLogger(__name__)

CME_MONTH_CODES: Dict[int, str] = {
    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
}
CONTRACT_ID_CACHE: Dict[Tuple[str, int, int], Tuple[str, Optional[int]]] = {}

# API_NUMERIC_ID_FIELD_HYPOTHESIS:
# This constant in the Pydantic schema ('instrument_id' aliased to 'instrumentId')
# represents a field in the API response from `/api/Contract/search`
# that might contain an integer-based contract identifier suitable for the
# `/api/History/retrieveBars` endpoint. The TopStep API documentation for
# historical bars suggests `contractId` can be an integer. However, the
# `/api/Contract/search` endpoint documentation (page 7-8) does not
# explicitly list a field guaranteed to be this integer ID.
#
# **ACTION REQUIRED:** This hypothesis MUST be verified by inspecting actual API
# responses from `/api/Contract/search` for various futures contracts.
# If a reliable integer ID field exists (and is mapped correctly in schemas.py),
# this logic should work. If not, `instrument_id` will be None.
API_NUMERIC_ID_FIELD_PYTHONIC = "instrument_id" # Pythonic field name in schemas.Contract


if API_NUMERIC_ID_FIELD_PYTHONIC == "instrument_id": # Log initial assumption clearly
    logger.info(
        "ContractUtils: Using Pydantic model field '%s' (corresponds to API alias 'instrumentId') "
        "for numeric contract IDs from contract search results. "
        "**This relies on the Pydantic schema and underlying API field being correct.** "
        "If this field is incorrect or not present in API response, numeric ID resolution may yield None.",
        API_NUMERIC_ID_FIELD_PYTHONIC
    )

# pylint: disable=too-many-locals, too-many-branches, too-many-statements, too-many-nested-blocks
def get_futures_contract_details(api_client: APIClient,
                                 processing_date: date,
                                 symbol_root: str) -> Optional[Tuple[str, Optional[int]]]:
    """
    Determines the active futures contract string ID and a potential numeric ID
    for a given symbol root and processing date.

    This function implements a common futures roll logic for quarterly contracts
    (ES, NQ, etc.) and attempts to find a matching contract via API search.
    The API search now returns Pydantic `schemas.Contract` models.
    It also tries to extract a hypothesized numeric contract ID from these models.

    Core Assumptions and Limitations:
    1.  **Roll Logic:** Assumes standard CME quarterly expiries (March, June,
        September, December coded as H, M, U, Z). For symbols explicitly listed
        in `quarterly_symbols`, it rolls to the next quarter appropriately.
        For symbols *not* in `quarterly_symbols`, it defaults to using the
        `processing_date`'s current month. This default behavior might not be
        correct for non-quarterly contracts with different roll conventions
        (e.g., monthly rolls for CL, GC). Users should verify or adapt for such cases.
    2.  **API Search Strategy:**
        *   It tries a series of `searchText` patterns based on the symbol root,
            derived month code, and year (e.g., "NQU4", "NQU2024", "NQ24", "NQ").
        *   It prioritizes "live" contracts from the API search first.
    3.  **Contract ID Matching:**
        *   **Primary Match:** Looks for an API contract ID string (from `schemas.Contract.id`)
            that *ends with* a suffix like ".SYMBOL_ROOT.MONTH_CODEYEAR_TWO_DIGITS"
            (e.g., ".NQ.U24"). This is considered the strongest indicator.
        *   **Secondary Match:** If no primary match, it looks for an API contract
            `name` field (from `schemas.Contract.name`) that exactly matches one
            of the generated ticker styles (e.g., "NQU4"). This is a weaker match.
        *   The function may be brittle if the API changes its contract ID or name
            formatting conventions, or if its search behavior for these patterns changes.
    4.  **Numeric ID Retrieval:**
        *   It attempts to get a numeric ID from the `schemas.Contract.instrument_id`
            field of the Pydantic model. This field corresponds to the API response
            field aliased as `instrumentId` (or whatever the Pydantic schema defines).
            Its reliability depends on the API providing this field and the schema mapping
            it correctly.
    5.  **Fallback:** If no suitable contract is found via API search, a string ID is
        constructed based on the derived month/year (e.g., "CON.F.US.NQ.U24") and
        returned with a None numeric ID. This fallback ID may not be valid for
        API operations.
    6.  **Caching:** Results are cached based on `(symbol_root, target_contract_month, year_for_contract)`
        to reduce redundant API calls.

    Potential Future Enhancements (Not Implemented):
    *   Allowing users to pass custom search patterns or validation functions.
    *   Utilizing a more direct API endpoint if one exists for fetching the "current"
        or "front month" contract for a symbol root.

    Args:
        api_client (APIClient): An initialized `APIClient` instance.
        processing_date (date): The date for which to determine the active contract.
                                This influences month and year calculation.
        symbol_root (str): The root symbol of the futures contract (e.g., "ES", "NQ", "CL").
                           Case-insensitive, will be converted to uppercase.

    Returns:
        Optional[Tuple[str, Optional[int]]]:
            A tuple containing:
            - The string contract ID (e.g., "CON.F.US.NQ.H24") if found or constructed.
            - The numeric contract ID (integer) if found in the `schemas.Contract.instrument_id`
              field of the Pydantic model, otherwise None.
            Returns None if critical errors occur (e.g., invalid month code).
            If API search fails, a fallback string ID is constructed, and numeric ID will be None.
    """
    month_to_check = processing_date.month
    year_for_contract = processing_date.year
    target_contract_month: int

    quarterly_symbols = ["ES", "NQ", "EP", "ENQ", "YM", "RTY", "MES", "MNQ"]

    if symbol_root.upper() in quarterly_symbols:
        if 1 <= month_to_check <= 2: target_contract_month = 3
        elif 3 <= month_to_check <= 5: target_contract_month = 6
        elif 6 <= month_to_check <= 8: target_contract_month = 9
        elif 9 <= month_to_check <= 11: target_contract_month = 12
        elif month_to_check == 12:
            target_contract_month = 3
            year_for_contract += 1
        else:
            logger.error(
                "Internal logic error: Invalid month_to_check '%s' for quarterly symbol '%s'.",
                month_to_check, symbol_root
            )
            return None
    else:
        logger.warning(
            "Symbol '%s' not in predefined quarterly list. "
            "Using current month (%s) and year %s for contract determination. "
            "This may not be correct for non-quarterly or custom-rolled contracts. "
            "Verify roll logic.",
            symbol_root, month_to_check, year_for_contract
        )
        target_contract_month = month_to_check

    month_code = CME_MONTH_CODES.get(target_contract_month)
    if not month_code:
        logger.error(
            "No CME month code for target month %s (derived for symbol '%s', date '%s').",
            target_contract_month, symbol_root, processing_date
        )
        return None

    year_last_digit = str(year_for_contract)[-1:]
    year_two_digits = str(year_for_contract)[-2:]
    upper_symbol_root = symbol_root.upper()
    api_search_symbol_root = upper_symbol_root
    if upper_symbol_root == "EP": api_search_symbol_root = "ES"
    elif upper_symbol_root == "ENQ": api_search_symbol_root = "NQ"

    cache_key = (upper_symbol_root, target_contract_month, year_for_contract)
    if cache_key in CONTRACT_ID_CACHE:
        cached_str_id, cached_int_id = CONTRACT_ID_CACHE[cache_key]
        logger.debug(
            "Using cached contract for %s %s%s: StrID='%s', IntID=%s",
            upper_symbol_root, month_code, year_two_digits,
            cached_str_id, cached_int_id if cached_int_id is not None else 'N/A'
        )
        return cached_str_id, cached_int_id

    search_ticker_style1 = f"{api_search_symbol_root}{month_code}{year_last_digit}"
    search_ticker_style2 = f"{api_search_symbol_root}{month_code}{str(year_for_contract)}"
    search_ticker_style3 = f"{api_search_symbol_root}{month_code}{year_two_digits}"
    expected_api_id_suffix = f".{api_search_symbol_root}.{month_code}{year_two_digits}"
    
    found_contract_model: Optional[schemas.Contract] = None 
    
    search_texts_to_try: List[str] = [
        search_ticker_style3, search_ticker_style2,
        search_ticker_style1, api_search_symbol_root
    ]

    for search_text in search_texts_to_try:
        logger.info(
            "ContractUtils: Searching API with searchText='%s' (target: %s %s%s)",
            search_text, upper_symbol_root, month_code, year_two_digits
        )
        try:
            # api_client.search_contracts now returns List[schemas.Contract]
            contracts_from_api: List[schemas.Contract] = api_client.search_contracts(search_text=search_text, live=True)
            
            if not contracts_from_api and search_text == api_search_symbol_root:
                logger.info(
                    "ContractUtils: No live contracts for broad search '%s'. Trying non-live.",
                    search_text
                )
                contracts_from_api = api_client.search_contracts(search_text=search_text,
                                                                 live=False)

            if contracts_from_api:
                for contract_model in contracts_from_api:
                    # Access attributes from the Pydantic model
                    api_str_id_candidate = contract_model.id if contract_model.id is not None else ""
                    api_name_candidate = contract_model.name.upper() if contract_model.name is not None else ""

                    is_primary_match = api_str_id_candidate.upper().endswith(
                        expected_api_id_suffix.upper()
                    )
                    ticker_styles_upper = [s.upper() for s in [search_ticker_style1,
                                                              search_ticker_style2,
                                                              search_ticker_style3]]
                    is_secondary_match = api_name_candidate in ticker_styles_upper

                    if is_primary_match or is_secondary_match:
                        if is_primary_match:
                            found_contract_model = contract_model
                            logger.debug(
                                "Primary match (suffix): Model ID='%s', Name='%s', NumericID (from model)=%s",
                                found_contract_model.id, 
                                found_contract_model.name,
                                found_contract_model.instrument_id if found_contract_model.instrument_id is not None else 'N/A'
                            )
                            break # Best match for this search_text
                        if is_secondary_match and not found_contract_model: # Only take secondary if no primary yet
                            found_contract_model = contract_model
                            logger.debug(
                                "Secondary match (name): Model ID='%s', Name='%s', NumericID (from model)=%s. Weaker match.",
                                found_contract_model.id, 
                                found_contract_model.name,
                                found_contract_model.instrument_id if found_contract_model.instrument_id is not None else 'N/A'
                            )
                
                if found_contract_model and \
                   (found_contract_model.id if found_contract_model.id else "").upper().endswith(expected_api_id_suffix.upper()):
                    break # Stop trying other search_texts if primary match found
        
        except APIResponseParsingError as e_parse: 
            logger.error("Error parsing contract search response for '%s': %s. Raw text: %s", 
                         search_text, e_parse, e_parse.raw_response_text[:200] if e_parse.raw_response_text else "N/A")
        except APIError as e:
            logger.warning(
                "APIError during contract search for '%s': %s. Proceeding.",
                search_text, e
            )
        except Exception as e_gen: 
            logger.error(
                "Unexpected error during contract search for '%s': %s",
                search_text, e_gen, exc_info=True
            )
            CONTRACT_ID_CACHE[cache_key] = ("", None) 
            return None

    if found_contract_model and found_contract_model.id:
        final_str_id = found_contract_model.id
        # Numeric ID is now directly from the Pydantic model's instrument_id field
        # The Pydantic model (schemas.Contract) ensures instrument_id is Optional[int]
        final_int_id = found_contract_model.instrument_id 

        logger.info(
            "SUCCESS: Determined contract for %s %s%s -> String ID: '%s', Integer ID: %s",
            upper_symbol_root, month_code, year_two_digits, final_str_id,
            final_int_id if final_int_id is not None else 'Not Found/Available'
        )
        CONTRACT_ID_CACHE[cache_key] = (final_str_id, final_int_id)
        return final_str_id, final_int_id

    # Fallback if no match found
    constructed_str_id = f"CON.F.US.{api_search_symbol_root}.{month_code}{year_two_digits}"
    logger.warning(
        "API search did not find contract for %s %s%s (date: %s). "
        "Using fallback String ID: '%s'. Integer ID will be None. "
        "This contract may not be tradable or queryable for history.",
        upper_symbol_root, month_code, year_two_digits,
        processing_date.isoformat(), constructed_str_id
    )
    CONTRACT_ID_CACHE[cache_key] = (constructed_str_id, None)
    return constructed_str_id, None