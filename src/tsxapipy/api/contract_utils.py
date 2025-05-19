# tsxapipy/api/contract_utils.py
"""
Utilities for determining and managing futures contract identifiers,
including roll logic and API searches.
"""
import logging
from datetime import date
from typing import Dict, Tuple, Optional, List, Any

from tsxapipy.api.client import APIClient
from tsxapipy.api.exceptions import APIError # ContractNotFoundError removed as it's not raised here

logger = logging.getLogger(__name__)

CME_MONTH_CODES: Dict[int, str] = {
    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
}
CONTRACT_ID_CACHE: Dict[Tuple[str, int, int], Tuple[str, Optional[int]]] = {}
API_NUMERIC_ID_FIELD_HYPOTHESIS = "instrumentId"

if API_NUMERIC_ID_FIELD_HYPOTHESIS == "instrumentId":
    logger.info(
        "ContractUtils: Using '%s' as a hypothetical field name "
        "for numeric contract IDs from contract search results. "
        "**This MUST be verified against actual API responses.**",
        API_NUMERIC_ID_FIELD_HYPOTHESIS
    )

# pylint: disable=too-many-locals, too-many-branches, too-many-statements, too-many-nested-blocks
def get_futures_contract_details(api_client: APIClient,
                                 processing_date: date,
                                 symbol_root: str) -> Optional[Tuple[str, Optional[int]]]:
    """
    Determines active futures contract string ID and numeric ID for a symbol and date.
    (Full docstring as previously provided, shortened for brevity here)
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
    found_str_id: Optional[str] = None
    found_int_id: Optional[int] = None
    api_str_id_candidate: str = "" # Initialize for use after loop

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
            contracts_from_api = api_client.search_contracts(search_text=search_text, live=True)
            if not contracts_from_api and search_text == api_search_symbol_root:
                logger.info(
                    "ContractUtils: No live contracts for broad search '%s'. Trying non-live.",
                    search_text
                )
                contracts_from_api = api_client.search_contracts(search_text=search_text,
                                                                 live=False)

            if contracts_from_api:
                for contract_data in contracts_from_api:
                    api_str_id_candidate = contract_data.get("id", "")
                    api_name_candidate = contract_data.get("name", "").upper()

                    is_primary_match = api_str_id_candidate.upper().endswith(
                        expected_api_id_suffix.upper()
                    )
                    ticker_styles_upper = [s.upper() for s in [search_ticker_style1,
                                                              search_ticker_style2,
                                                              search_ticker_style3]]
                    is_secondary_match = api_name_candidate in ticker_styles_upper

                    if is_primary_match or is_secondary_match:
                        potential_int_id_val = contract_data.get(API_NUMERIC_ID_FIELD_HYPOTHESIS)
                        potential_int_id: Optional[int] = None
                        if potential_int_id_val is not None:
                            try:
                                potential_int_id = int(potential_int_id_val)
                            except (ValueError, TypeError):
                                logger.warning(
                                    "Contract '%s' (Name: %s) has non-int value for '%s': '%s'.",
                                    api_str_id_candidate, api_name_candidate,
                                    API_NUMERIC_ID_FIELD_HYPOTHESIS, potential_int_id_val
                                )
                        if is_primary_match:
                            found_str_id, found_int_id = api_str_id_candidate, potential_int_id
                            logger.debug(
                                "Primary match (suffix): StrID='%s', IntID=%s, Name='%s'",
                                found_str_id, found_int_id, contract_data.get('name')
                            )
                            break # Best match for this search_text
                        if is_secondary_match and not found_str_id:
                            found_str_id, found_int_id = api_str_id_candidate, potential_int_id
                            logger.debug(
                                "Secondary match (name): StrID='%s', IntID=%s, Name='%s'.",
                                found_str_id, found_int_id, contract_data.get('name')
                            )
                if found_str_id and api_str_id_candidate.upper().endswith(
                    expected_api_id_suffix.upper()
                ):
                    break # Stop trying other search_texts if primary match found

        except APIError as e:
            logger.warning(
                "APIError during contract search for '%s': %s. Proceeding.",
                search_text, e
            )
        except Exception as e_gen: # pylint: disable=broad-except
            logger.error(
                "Unexpected error during contract search for '%s': %s",
                search_text, e_gen, exc_info=True
            )
            CONTRACT_ID_CACHE[cache_key] = ("", None)
            return None

    if found_str_id:
        logger.info(
            "SUCCESS: Determined contract for %s %s%s -> String ID: '%s', Integer ID: %s",
            upper_symbol_root, month_code, year_two_digits, found_str_id,
            found_int_id if found_int_id is not None else 'Not Found/Available'
        )
        CONTRACT_ID_CACHE[cache_key] = (found_str_id, found_int_id)
        return found_str_id, found_int_id

    # Fallback
    constructed_str_id = f"CON.F.US.{api_search_symbol_root}.{month_code}{year_two_digits}"
    logger.warning(
        "API search did not find contract for %s %s%s (date: %s). "
        "Using fallback String ID: '%s'. Integer ID will be None.",
        upper_symbol_root, month_code, year_two_digits,
        processing_date.isoformat(), constructed_str_id
    )
    CONTRACT_ID_CACHE[cache_key] = (constructed_str_id, None)
    return constructed_str_id, None