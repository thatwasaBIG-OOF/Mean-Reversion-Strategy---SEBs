# tsxapipy/historical/updater.py
"""
Module for updating historical market data using Parquet files.
"""
import logging
from datetime import datetime, timedelta, time as dt_time, date
import time
import os
from typing import Optional, Any, Tuple, List # Added List

import pandas as pd  # pylint: disable=import-error

from tsxapipy.common.time_utils import UTC_TZ
from tsxapipy.api.client import APIClient, MAX_BARS_PER_REQUEST
from tsxapipy.api.contract_utils import get_futures_contract_details
from tsxapipy.historical.parquet_handler import (
    get_last_timestamp_from_parquet,
    append_bars_to_parquet
)
from .gap_detector import find_missing_trading_days
from tsxapipy.api.exceptions import APIError, APIResponseParsingError # Added APIResponseParsingError
from tsxapipy.api import schemas # Import Pydantic schemas

logger = logging.getLogger(__name__)

# Local constant for _fetch_and_append_data_for_window
_MAX_STALLED_FETCHES_TO_BREAK_WINDOW = 3

def calculate_next_interval_start(last_timestamp_utc: datetime,
                                  api_bar_unit: int,
                                  api_bar_unit_number: int) -> datetime:
    """
    Calculates the start datetime for the next data fetching interval.

    This is used to determine the `startTime` for subsequent API calls when
    paginating through historical data. It effectively adds the duration of
    one bar to the `last_timestamp_utc`.

    Args:
        last_timestamp_utc (datetime): The UTC timestamp of the last successfully
                                       fetched bar. Must be timezone-aware UTC.
        api_bar_unit (int): The API code for the bar unit (e.g., 2 for Minute, 3 for Hour).
        api_bar_unit_number (int): The number of units for the bar (e.g., 1 for 1-minute).

    Returns:
        datetime: The UTC datetime for the start of the next interval.

    Raises:
        TypeError: If `last_timestamp_utc` is not a datetime object.
    """
    if not isinstance(last_timestamp_utc, datetime):
        raise TypeError("last_timestamp_utc must be a datetime object.")

    # Ensure last_timestamp_utc is UTC-aware
    if last_timestamp_utc.tzinfo is None:
        last_timestamp_utc = UTC_TZ.localize(last_timestamp_utc)
        logger.warning("calculate_next_interval_start received a naive timestamp. Localized to UTC: %s",
                       last_timestamp_utc.isoformat())
    elif last_timestamp_utc.tzinfo.utcoffset(last_timestamp_utc) != timedelta(0):
        last_timestamp_utc = last_timestamp_utc.astimezone(UTC_TZ)
        logger.warning("calculate_next_interval_start received a non-UTC timestamp. Converted to UTC: %s",
                       last_timestamp_utc.isoformat())

    if api_bar_unit == 2: # Minute
        delta = timedelta(minutes=api_bar_unit_number)
    elif api_bar_unit == 3: # Hour
        delta = timedelta(hours=api_bar_unit_number)
    elif api_bar_unit == 4: # Day
        # For daily bars, the next interval starts at the beginning of the next day
        return (last_timestamp_utc.replace(hour=0, minute=0, second=0, microsecond=0) +
                timedelta(days=api_bar_unit_number))
    else: # Includes unit 1 (Second) or unsupported units
        logger.warning("Unsupported api_bar_unit %s for precise next interval calculation. "
                       "Defaulting to adding 1 second. Verify if this is appropriate.",
                       api_bar_unit)
        # For seconds or other units, a simple timedelta might be best if API behavior is known
        # If api_bar_unit == 1 (Second), this would be:
        # delta = timedelta(seconds=api_bar_unit_number)
        # For now, a generic +1 second to ensure progress if unit is unexpected.
        return last_timestamp_utc + timedelta(seconds=1)
    return last_timestamp_utc + delta


class HistoricalDataUpdater:
    """
    Manages fetching, storing, and updating historical bar data for futures contracts.

    This class orchestrates the process of:
    1. Determining the appropriate contract ID for each day within a target fetch window,
       considering potential contract rolls (primarily for quarterly contracts).
    2. Fetching historical bar data from the API for the determined contracts.
    3. Appending new data to a temporary Parquet file.
    4. Detecting and filling any gaps (missing trading days) in the temporary file.
    5. Merging the updated data from the temporary file into a main Parquet file.

    It supports overriding the contract determination with a fixed contract ID and
    can also override the overall start and end dates for the data update process.
    Numeric contract IDs are used for API history calls if resolved, with the
    original string contract ID stored for traceability.
    """
    # pylint: disable=too-many-instance-attributes, too-many-arguments
    def __init__(self, api_client: APIClient,
                 symbol_root: str, main_parquet_file: str,
                 temp_file_suffix: str, api_bar_unit: int, api_bar_unit_number: int,
                 contract_override: Optional[str] = None,
                 fetch_days_if_new: int = 90,
                 sleep_between_requests: float = 0.25, max_gap_fill_passes: int = 3,
                 temp_file_dir: Optional[str] = None,
                 overall_start_date_override: Optional[str] = None,  # Expected format: YYYY-MM-DD
                 overall_end_date_override: Optional[str] = None):  # Expected format: YYYY-MM-DD
        """
        Initializes the HistoricalDataUpdater.

        Args:
            api_client (APIClient): An initialized `APIClient` instance.
            symbol_root (str): The root symbol of the instrument (e.g., "ES", "NQ").
            main_parquet_file (str): Path to the main Parquet file where final data is stored.
            temp_file_suffix (str): Suffix for the temporary Parquet file (e.g., "_update_temp").
            api_bar_unit (int): API code for the bar unit (e.g., 2 for Minute, 3 for Hour).
            api_bar_unit_number (int): Number of units for the bar (e.g., 1 for 1-minute).
            contract_override (Optional[str], optional): If provided, use this specific
                contract ID for all fetches, bypassing daily contract determination.
                Defaults to None.
            fetch_days_if_new (int, optional): If `main_parquet_file` is new or empty,
                number of past days of data to fetch. Defaults to 90.
            sleep_between_requests (float, optional): Seconds to sleep between paginated
                API requests during historical data fetching. Defaults to 0.25.
            max_gap_fill_passes (int, optional): Maximum number of passes for iterative
                gap filling. Defaults to 3.
            temp_file_dir (Optional[str], optional): Directory for temporary Parquet files.
                If None, defaults to the same directory as `main_parquet_file`.
            overall_start_date_override (Optional[str], optional): If provided (YYYY-MM-DD),
                overrides the start date for fetching data. Defaults to None.
            overall_end_date_override (Optional[str], optional): If provided (YYYY-MM-DD),
                overrides the end date for fetching data (inclusive). Defaults to None (current day).

        Raises:
            TypeError: If `api_client` is not an instance of `APIClient`.
        """
        if not isinstance(api_client, APIClient):
            raise TypeError("api_client must be an instance of APIClient.")
        self.api_client = api_client
        self.symbol_root = symbol_root.upper()
        self.main_parquet_file = main_parquet_file
        self.api_bar_unit, self.api_bar_unit_number = api_bar_unit, api_bar_unit_number
        self.contract_override_str: Optional[str] = contract_override
        self.contract_override_int: Optional[int] = None # For numeric ID if override is used
        self.fetch_days_if_new = fetch_days_if_new
        self.sleep_between_requests = sleep_between_requests
        self.max_gap_fill_passes = max_gap_fill_passes

        self.temp_parquet_file = self._determine_temp_filepath(
            main_parquet_file, temp_file_suffix, temp_file_dir
        )
        logger.info("HistoricalDataUpdater initialized for Symbol Root: '%s'. Main Parquet: '%s', Temp Parquet: '%s'",
                    self.symbol_root, self.main_parquet_file, self.temp_parquet_file)

        # Determine the overall target end datetime for this update run
        self.overall_target_end_dt_utc = self._parse_overall_end_date(overall_end_date_override)
        # Parse start date override if provided
        self.overall_start_date_override_obj = self._parse_overall_start_date(
            overall_start_date_override
        )

        self.total_new_bars_appended_this_run: int = 0
        self._override_logged_this_instance: bool = False # To log override usage once

        if self.contract_override_str:
            self._resolve_contract_override_id() # Attempt to find numeric ID for override

    def _determine_temp_filepath(self, main_file: str, suffix: str, temp_dir: Optional[str]) -> str:
        """Determines the full path for the temporary Parquet file."""
        main_d = os.path.dirname(main_file) or "." # Use current dir if main_file has no path
        main_base, main_ext = os.path.splitext(os.path.basename(main_file))
        effective_ext = main_ext if main_ext and main_ext.lower() == ".parquet" else ".parquet"
        temp_filename = f"{main_base}{suffix}{effective_ext}"

        final_temp_dir = temp_dir or main_d
        if not os.path.exists(final_temp_dir):
            try:
                os.makedirs(final_temp_dir, exist_ok=True)
                logger.info("Created temporary directory for Parquet: %s", final_temp_dir)
            except OSError as e:
                logger.error("Could not create temporary directory '%s': %s. Using main file's directory: %s",
                             final_temp_dir, e, main_d)
                final_temp_dir = main_d # Fallback to main file's directory
        return os.path.join(final_temp_dir, temp_filename)

    def _parse_overall_end_date(self, override_str: Optional[str]) -> datetime:
        """Parses the overall end date override string to a UTC datetime."""
        target_end_dt = datetime.now(UTC_TZ)  # Default to current time
        if override_str:
            try:
                dt_obj = datetime.strptime(override_str, "%Y-%m-%d")
                # Use end of the specified day for inclusivity (23:59:59.999999 UTC)
                target_end_dt = UTC_TZ.localize(datetime.combine(dt_obj.date(), dt_time.max))
                logger.info("  Overall data fetch end date overridden to: %s (UTC)", target_end_dt.isoformat())
            except ValueError:
                logger.error("Invalid overall_end_date_override format '%s'. Must be YYYY-MM-DD. Using current time as end.",
                             override_str)
        return target_end_dt

    def _parse_overall_start_date(self, override_str: Optional[str]) -> Optional[date]:
        """Parses the overall start date override string to a date object."""
        start_date_obj = None
        if override_str:
            try:
                start_date_obj = datetime.strptime(override_str, "%Y-%m-%d").date()
                logger.info("  Overall data fetch start date overridden to: %s", start_date_obj.isoformat())
            except ValueError:
                logger.error("Invalid overall_start_date_override format '%s'. Must be YYYY-MM-DD. Ignoring override.",
                             override_str)
        return start_date_obj

    def _resolve_contract_override_id(self) -> None:
        """
        Attempts to resolve a numeric ID if a string contract_override is provided.

        This method is called during initialization if `contract_override_str` is set.
        It tries to interpret the override as an integer directly. If that fails,
        it logs the situation. The actual API lookup for a numeric ID from a string
        override typically happens within `_get_current_contract_ids_for_fetch` if
        the `HistoricalDataUpdater` is not using a contract override (which it is, in this case).
        Therefore, this method primarily focuses on the direct integer interpretation.
        The `contract_override_int` will be used if available, otherwise the string override
        will be used for API calls that might accept string IDs.
        """
        if not self.contract_override_str: return

        logger.info("Contract override provided: '%s'. Attempting to resolve its details.",
                    self.contract_override_str)
        try:
            # Check if the override string can be directly interpreted as an integer
            self.contract_override_int = int(self.contract_override_str) # type: ignore[arg-type]
            logger.info("  Interpreted contract_override '%s' as an integer ID: %s.",
                        self.contract_override_str, self.contract_override_int)
            return # Successfully interpreted as int
        except ValueError:
            # Not a simple integer string.
            logger.info("  Contract_override '%s' is a string. Numeric ID not directly parsed. "
                        "If history API requires an integer ID, ensure the string form is acceptable "
                        "or that the API can resolve it. Otherwise, a specific numeric form might be needed.",
                        self.contract_override_str)
            # If `get_futures_contract_details` were to be used here for a *fixed* override,
            # it would contradict the purpose of an override. The assumption is the user
            # knows the string ID provided is what they want to use, or it's a numeric string.
            # The `_get_current_contract_ids_for_fetch` will correctly return this pair.
            pass


    def _get_current_contract_ids_for_fetch(self,
                                          for_date_obj: date
                                          ) -> Optional[Tuple[str, Optional[int]]]:
        """
        Determines the string and (potential) integer contract IDs for fetching data
        for a specific date. Uses the instance's `contract_override` if set,
        otherwise calls `get_futures_contract_details`.

        Args:
            for_date_obj (date): The date for which to determine contract IDs.

        Returns:
            Optional[Tuple[str, Optional[int]]]: A tuple (string_id, numeric_id)
                                                 or None if resolution fails.
        """
        if self.contract_override_str:
            # If an override is set, use it.
            # self.contract_override_int would have been set in __init__ if the override string was numeric.
            if not self._override_logged_this_instance: # Log only once per updater instance
                log_int_id = self.contract_override_int if self.contract_override_int is not None else 'N/A (string override)'
                logger.info("Updater: Using fixed contract_override (String: '%s', Resolved Int: %s). Daily contract determination is bypassed.",
                            self.contract_override_str, log_int_id)
                self._override_logged_this_instance = True
            return self.contract_override_str, self.contract_override_int

        # No override, determine dynamically
        return get_futures_contract_details(
            api_client=self.api_client,
            processing_date=for_date_obj,
            symbol_root=self.symbol_root
        )

    def _fetch_and_append_data_for_window(self,
                                           string_contract_id_for_source_col: str,
                                           numeric_contract_id_for_api: Optional[int],
                                           window_start_dt_utc: datetime,
                                           window_end_dt_utc: datetime,
                                           target_file_to_append_to: str) -> int:
        """
        Fetches historical bar data for a given contract and time window,
        appending it to the specified Parquet file.

        Uses `numeric_contract_id_for_api` for the API call if available,
        otherwise uses `string_contract_id_for_source_col`. The
        `string_contract_id_for_source_col` is always stored in the Parquet
        file for traceability.

        Args:
            string_contract_id_for_source_col (str): The string representation of the
                contract ID (e.g., "CON.F.US.NQ.H24") to be stored in the data.
            numeric_contract_id_for_api (Optional[int]): The numeric contract ID,
                if resolved, to be used for the API history call.
            window_start_dt_utc (datetime): The UTC start datetime of the fetch window.
            window_end_dt_utc (datetime): The UTC end datetime of the fetch window.
            target_file_to_append_to (str): Path to the Parquet file to append data to.

        Returns:
            int: The number of new bars appended to the Parquet file for this window.
        """
        # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        param_for_history_api: Union[str, int] # Type hint for clarity
        log_id_type_used: str
        if numeric_contract_id_for_api is not None:
            param_for_history_api = numeric_contract_id_for_api
            log_id_type_used = "Numeric"
        else:
            param_for_history_api = string_contract_id_for_source_col
            log_id_type_used = "String (numeric not resolved/available)"
            if not self.contract_override_str: 
                logger.warning("Numeric contract ID not found for dynamically determined contract '%s'. "
                               "Using string ID for history API. Verify API compatibility if issues occur.",
                               string_contract_id_for_source_col)

        logger.info("--- Fetching Data for Window ---")
        logger.info("  Contract (for storage): '%s'", string_contract_id_for_source_col)
        logger.info("  Contract ID for API Call: '%s' (Type: %s)", param_for_history_api, log_id_type_used)
        logger.info("  Time Window (UTC): %s to %s", window_start_dt_utc.isoformat(), window_end_dt_utc.isoformat())
        logger.info("  Target File: %s", os.path.basename(target_file_to_append_to))

        current_fetch_point_utc = window_start_dt_utc
        appended_total_for_window = 0
        previous_newest_bar_ts_in_chunk: Optional[datetime] = None
        consecutive_stalled_fetches_for_window = 0

        while current_fetch_point_utc < window_end_dt_utc:
            start_iso = current_fetch_point_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_iso = window_end_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            logger.debug("  API Call: Fetching bars from %s to %s (limit %d)",
                         start_iso, end_iso, MAX_BARS_PER_REQUEST)
            try:
                # APIClient.get_historical_bars now returns schemas.HistoricalBarsResponse
                historical_bars_response: schemas.HistoricalBarsResponse = self.api_client.get_historical_bars(
                    contract_id=param_for_history_api,
                    start_time_iso=start_iso, end_time_iso=end_iso,
                    unit=self.api_bar_unit, unit_number=self.api_bar_unit_number,
                    limit=MAX_BARS_PER_REQUEST, include_partial_bar=False
                )
                # The `bars` attribute is List[schemas.BarData], already reversed (oldest first by APIClient)
                chunk_of_bar_models: List[schemas.BarData] = historical_bars_response.bars

            except APIResponseParsingError as e_parse:
                logger.error("  Error parsing historical bars response for contract '%s' (API param '%s') starting %s: %s. "
                             "Raw text: %s. Ending fetch for this window.",
                             string_contract_id_for_source_col, param_for_history_api, start_iso, e_parse,
                             e_parse.raw_response_text[:200] if e_parse.raw_response_text else "N/A")
                break
            except APIError as e:
                logger.error("  API error fetching bars for contract '%s' (API param '%s') starting %s: %s. "
                             "Ending fetch for this window.",
                             string_contract_id_for_source_col, param_for_history_api, start_iso, e)
                break 
            
            if not chunk_of_bar_models:
                logger.info("  No bars returned by API for contract '%s' (API param '%s') in period starting %s. "
                            "Window fetch likely complete.",
                            string_contract_id_for_source_col, param_for_history_api, start_iso)
                break 

            # Convert Pydantic BarData models to dictionaries for append_bars_to_parquet
            bars_to_add_dicts: List[Dict[str, Any]] = []
            for bar_model in chunk_of_bar_models:
                # append_bars_to_parquet expects dicts with 't', 'o', 'h', 'l', 'c', 'v', and 'contract_id_source'.
                # Pydantic bar_model.t is already a datetime object.
                bar_dict = {
                    't': bar_model.t, # Pass datetime object directly
                    'o': bar_model.o,
                    'h': bar_model.h,
                    'l': bar_model.l,
                    'c': bar_model.c,
                    'v': bar_model.v,
                    'contract_id_source': string_contract_id_for_source_col
                }
                bars_to_add_dicts.append(bar_dict)
            
            try:
                appended_now_count = append_bars_to_parquet(bars_to_add_dicts, target_file_to_append_to)
                if appended_now_count > 0:
                    appended_total_for_window += appended_now_count
                    consecutive_stalled_fetches_for_window = 0 
                elif bars_to_add_dicts: 
                    logger.debug("  API returned %d bars, but 0 net new bars were appended (likely duplicates or older).",
                                 len(bars_to_add_dicts))
                    consecutive_stalled_fetches_for_window += 1
            except Exception as e_append: # pylint: disable=broad-except
                logger.error("  Error appending %d bars to Parquet file %s: %s. Ending fetch for this window.",
                             len(bars_to_add_dicts), target_file_to_append_to, e_append, exc_info=True)
                break 

            try:
                # The last BarData model in the list (APIClient method ensures oldest first)
                newest_bar_model_in_chunk = chunk_of_bar_models[-1]
                # newest_bar_model_in_chunk.t is already a datetime object from Pydantic parsing
                newest_utc_dt_in_chunk = newest_bar_model_in_chunk.t 
                # Ensure it's UTC, though Pydantic model should enforce this if schema is `datetime[tz=UTC]`
                if newest_utc_dt_in_chunk.tzinfo is None: 
                    newest_utc_dt_in_chunk = UTC_TZ.localize(newest_utc_dt_in_chunk)
                elif newest_utc_dt_in_chunk.tzinfo != UTC_TZ: # Check object identity
                    newest_utc_dt_in_chunk = newest_utc_dt_in_chunk.astimezone(UTC_TZ)
            except IndexError: # If chunk_of_bar_models was empty after all (should be caught earlier)
                logger.error("  Internal error: chunk_of_bar_models is empty, cannot get newest timestamp.")
                break
            except Exception as e_ts_access: # pylint: disable=broad-except
                logger.error("  Error accessing timestamp from last bar model for contract '%s': %s. "
                             "Cannot determine next fetch point. Ending window.",
                             string_contract_id_for_source_col, e_ts_access)
                break

            if previous_newest_bar_ts_in_chunk == newest_utc_dt_in_chunk and \
               len(chunk_of_bar_models) >= MAX_BARS_PER_REQUEST and appended_now_count == 0 :
                logger.error("  Fetch stalled for contract '%s': Newest bar timestamp %s same as previous full fetch, and no new data appended. "
                             "Ending fetch for this window to prevent infinite loop.",
                             string_contract_id_for_source_col, newest_utc_dt_in_chunk.isoformat())
                break
            if consecutive_stalled_fetches_for_window >= _MAX_STALLED_FETCHES_TO_BREAK_WINDOW and \
               len(chunk_of_bar_models) >= MAX_BARS_PER_REQUEST: 
                logger.error("  Fetch halting for contract '%s': %d consecutive full API calls yielded no net new bars. "
                             "Ending fetch for this window.",
                             string_contract_id_for_source_col, _MAX_STALLED_FETCHES_TO_BREAK_WINDOW)
                break

            previous_newest_bar_ts_in_chunk = newest_utc_dt_in_chunk
            current_fetch_point_utc = calculate_next_interval_start(
                newest_utc_dt_in_chunk, self.api_bar_unit, self.api_bar_unit_number
            )
            
            if len(chunk_of_bar_models) < MAX_BARS_PER_REQUEST:
                logger.debug("  API returned %d bars (less than limit of %d). Assuming end of data for this window for contract '%s'.",
                             len(chunk_of_bar_models), MAX_BARS_PER_REQUEST, string_contract_id_for_source_col)
                break 
            
            if self.sleep_between_requests > 0 and current_fetch_point_utc < window_end_dt_utc:
                time.sleep(self.sleep_between_requests)

        logger.info("--- Finished Fetch for Window (Contract: '%s'). Appended %d bars to %s. ---",
                    string_contract_id_for_source_col, appended_total_for_window,
                    os.path.basename(target_file_to_append_to))
        return appended_total_for_window

    def _determine_initial_fill_start_utc(self,
                                          last_known_ts_main: Optional[datetime]) -> datetime:
        """
        Determines the starting UTC timestamp for the initial data fill operation.

        Considers `overall_start_date_override_obj`, `last_known_ts_main` from
        the main Parquet file, and `fetch_days_if_new`.

        Args:
            last_known_ts_main (Optional[datetime]): The last timestamp found in the
                                                     main Parquet file, or None if empty/new.

        Returns:
            datetime: The calculated UTC start timestamp for the initial fill.
        """
        if self.overall_start_date_override_obj:
            # User provided an explicit start date for the entire update process
            base_start_dt = UTC_TZ.localize(
                datetime.combine(self.overall_start_date_override_obj, dt_time.min) # Start of that day UTC
            )
            if last_known_ts_main and last_known_ts_main >= base_start_dt:
                # Existing data is more recent than or same as override; start after existing data
                initial_fill_start_utc = calculate_next_interval_start(
                    last_known_ts_main, self.api_bar_unit, self.api_bar_unit_number
                )
                logger.info("Overall start date override '%s' is effective, but existing data in main file "
                            "extends to %s. Adjusted initial fill start to: %s (UTC)",
                            self.overall_start_date_override_obj.isoformat(),
                            last_known_ts_main.isoformat(), initial_fill_start_utc.isoformat())
            else:
                # Override is earlier than existing data, or no existing data; use override
                initial_fill_start_utc = base_start_dt
                logger.info("Overall start date override is being used. Initial fill starts from: %s (UTC)",
                            initial_fill_start_utc.isoformat())
        elif last_known_ts_main:
            # No override, but existing data found; start after the last known timestamp
            initial_fill_start_utc = calculate_next_interval_start(
                last_known_ts_main, self.api_bar_unit, self.api_bar_unit_number
            )
            logger.info("Last known timestamp in main Parquet: %s. Initial fill starts from: %s (UTC)",
                        last_known_ts_main.isoformat(), initial_fill_start_utc.isoformat())
        else:  # No override, and no existing data in main Parquet file
            initial_fill_start_utc = self.overall_target_end_dt_utc - \
                                     timedelta(days=self.fetch_days_if_new)
            # For daily bars, align to start of day to avoid partial day fetches if not intended
            if self.api_bar_unit == 4: # API unit 4 == Day
                initial_fill_start_utc = UTC_TZ.localize(
                    datetime.combine(initial_fill_start_utc.date(), dt_time.min)
                )
            logger.info("Main Parquet file is new or empty. Initial fill for the last %d days starts from: %s (UTC)",
                        self.fetch_days_if_new, initial_fill_start_utc.isoformat())
        return initial_fill_start_utc

    def _perform_initial_fill(self, initial_fill_start_utc: datetime) -> None:
        """
        Performs the initial forward fill of data, iterating day by day,
        determining the contract for each day, and fetching data to the temporary Parquet file.

        Args:
            initial_fill_start_utc (datetime): The UTC datetime from which to start fetching.
        """
        logger.info("--- Step 1: Performing Initial Forward Fill to Temp File: %s ---", self.temp_parquet_file)
        current_day_for_contract_logic = initial_fill_start_utc.date()
        fetch_start_utc_for_current_contract = initial_fill_start_utc
        self.total_new_bars_appended_this_run = 0 # Reset for this run

        while (fetch_start_utc_for_current_contract < self.overall_target_end_dt_utc and
               current_day_for_contract_logic <= self.overall_target_end_dt_utc.date()):

            if current_day_for_contract_logic.weekday() >= 5:  # Monday is 0, Sunday is 6; skip Sat, Sun
                logger.debug("Skipping weekend day: %s", current_day_for_contract_logic.isoformat())
                current_day_for_contract_logic += timedelta(days=1)
                # Ensure fetch_start_utc_for_current_contract also advances past the weekend
                fetch_start_utc_for_current_contract = max(
                    fetch_start_utc_for_current_contract,
                    UTC_TZ.localize(datetime.combine(current_day_for_contract_logic, dt_time.min))
                )
                continue

            contract_details_for_day = self._get_current_contract_ids_for_fetch(
                current_day_for_contract_logic
            )
            if not contract_details_for_day or not contract_details_for_day[0]: # String ID is mandatory
                logger.error("No contract details could be resolved for date %s and symbol %s. Skipping day for initial fill.",
                             current_day_for_contract_logic.isoformat(), self.symbol_root)
                current_day_for_contract_logic += timedelta(days=1)
                fetch_start_utc_for_current_contract = max(
                    fetch_start_utc_for_current_contract,
                    UTC_TZ.localize(datetime.combine(current_day_for_contract_logic, dt_time.min))
                )
                continue

            string_id, numeric_id = contract_details_for_day
            # Determine the end of the current day for fetching this specific contract,
            # but don't exceed the overall target end time for the entire update run.
            day_end_boundary_for_fetch = min(
                UTC_TZ.localize(datetime.combine(current_day_for_contract_logic, dt_time.max)), # End of current day
                self.overall_target_end_dt_utc # Overall limit for this run
            )

            if fetch_start_utc_for_current_contract < day_end_boundary_for_fetch:
                logger.debug("Initial fill: Processing Day %s, Resolved Contract ID '%s', Fetch Window (UTC) %s - %s",
                             current_day_for_contract_logic.isoformat(), string_id,
                             fetch_start_utc_for_current_contract.isoformat(),
                             day_end_boundary_for_fetch.isoformat())
                
                appended_for_day = self._fetch_and_append_data_for_window(
                    string_contract_id_for_source_col=string_id,
                    numeric_contract_id_for_api=numeric_id,
                    window_start_dt_utc=fetch_start_utc_for_current_contract,
                    window_end_dt_utc=day_end_boundary_for_fetch,
                    target_file_to_append_to=self.temp_parquet_file
                )
                self.total_new_bars_appended_this_run += appended_for_day
            else:
                logger.debug("Initial fill: Skipping day %s as fetch window is invalid (start %s >= end %s).",
                             current_day_for_contract_logic.isoformat(),
                             fetch_start_utc_for_current_contract.isoformat(),
                             day_end_boundary_for_fetch.isoformat())

            # Advance to the next day for contract determination logic
            current_day_for_contract_logic += timedelta(days=1)
            # Ensure the next fetch_start_utc_for_current_contract is at least the start of the new day,
            # or continues from where the previous day's contract fetch ended if that's later.
            fetch_start_utc_for_current_contract = max(
                fetch_start_utc_for_current_contract, # Maintain continuity if previous day's fetch didn't complete the day
                UTC_TZ.localize(datetime.combine(current_day_for_contract_logic, dt_time.min))
            )

            if self.sleep_between_requests > 0 and fetch_start_utc_for_current_contract < self.overall_target_end_dt_utc:
                time.sleep(self.sleep_between_requests) # Sleep if more fetching is expected

        logger.info("--- Step 1: Initial Forward Fill to Temp File Complete. Appended approximately %d bars. ---",
                    self.total_new_bars_appended_this_run)


    def _perform_gap_filling(self, gap_check_start_date: date,
                             gap_check_end_date: date) -> int:
        """
        Checks for and fills gaps (missing trading days) in the temporary Parquet file.

        Iterates multiple passes if necessary, as filling one gap might reveal others
        if contract rolls occurred within the gap period.

        Args:
            gap_check_start_date (date): The start date (inclusive) for gap checking.
            gap_check_end_date (date): The end date (inclusive) for gap checking.

        Returns:
            int: Total number of bars added during all gap filling passes.
        """
        logger.info("--- Step 2: Checking and Filling Gaps in Temp File: %s ---", self.temp_parquet_file)
        total_gaps_filled_bars_all_passes = 0

        if not (os.path.exists(self.temp_parquet_file) and
                os.path.getsize(self.temp_parquet_file) > 0):
            logger.info("Temporary Parquet file '%s' is empty or non-existent. Skipping gap check.", self.temp_parquet_file)
            return 0

        for i_pass in range(self.max_gap_fill_passes):
            logger.info("Starting Gap Fill Pass %d of %d (Range: %s to %s)...",
                        i_pass + 1, self.max_gap_fill_passes,
                        gap_check_start_date.isoformat(), gap_check_end_date.isoformat())
            
            missing_trading_days = find_missing_trading_days(
                parquet_file_to_check=self.temp_parquet_file,
                overall_start_date=gap_check_start_date,
                overall_end_date=gap_check_end_date
            )
            
            if not missing_trading_days:
                logger.info("No missing trading days detected in temp file for the specified range. Gap filling complete for this pass.")
                break # No gaps found, exit passes

            bars_added_this_pass = 0
            logger.info("Found %d missing day(s) to attempt filling: %s",
                        len(missing_trading_days),
                        [d.isoformat() for d in missing_trading_days[:5]] + (['...'] if len(missing_trading_days)>5 else []))

            for idx, gap_date_obj in enumerate(missing_trading_days):
                gap_day_start_utc = UTC_TZ.localize(datetime.combine(gap_date_obj, dt_time.min))
                # Ensure gap fill does not exceed the overall target end time for the run
                gap_day_end_utc = min(
                    UTC_TZ.localize(datetime.combine(gap_date_obj, dt_time.max)),
                    self.overall_target_end_dt_utc
                )
                
                contract_details_for_gap_day = self._get_current_contract_ids_for_fetch(gap_date_obj)
                if not contract_details_for_gap_day or not contract_details_for_gap_day[0]:
                    logger.error("Could not determine contract ID for gap day %s (Symbol: %s). Skipping fill for this day.",
                                 gap_date_obj.isoformat(), self.symbol_root)
                    continue
                
                string_id_gap, numeric_id_gap = contract_details_for_gap_day

                logger.debug("Gap fill: Processing Day %s, Resolved Contract ID '%s', Fetch Window (UTC) %s - %s",
                             gap_date_obj.isoformat(), string_id_gap,
                             gap_day_start_utc.isoformat(), gap_day_end_utc.isoformat())
                
                appended_for_this_gap_day = self._fetch_and_append_data_for_window(
                    string_contract_id_for_source_col=string_id_gap,
                    numeric_contract_id_for_api=numeric_id_gap,
                    window_start_dt_utc=gap_day_start_utc,
                    window_end_dt_utc=gap_day_end_utc,
                    target_file_to_append_to=self.temp_parquet_file
                )
                if appended_for_this_gap_day > 0:
                    bars_added_this_pass += appended_for_this_gap_day
                    # self.total_new_bars_appended_this_run is incremented inside _fetch_and_append...

                if self.sleep_between_requests > 0 and idx < len(missing_trading_days) - 1:
                    time.sleep(self.sleep_between_requests) # Sleep between filling different gap days

            total_gaps_filled_bars_all_passes += bars_added_this_pass
            if bars_added_this_pass == 0:
                logger.info("No further bars were added during gap fill pass %d. Assuming all fillable gaps are addressed.", i_pass + 1)
                break # No new data in this pass, probably done
            
            logger.info("Gap fill pass %d appended a total of %d bars.",
                        i_pass + 1, bars_added_this_pass)
            
        return total_gaps_filled_bars_all_passes

    def _merge_temp_into_main(self) -> None:
        """
        Merges the data from the temporary Parquet file into the main Parquet file.

        This involves reading all data from the temporary file and using
        `append_bars_to_parquet` (which handles deduplication and sorting)
        to integrate it into the main file. The temporary file is deleted
        after a successful merge attempt.
        """
        logger.info("--- Step 3: Merging Temporary File '%s' into Main File '%s' ---",
                    self.temp_parquet_file, self.main_parquet_file)
        
        if not (os.path.exists(self.temp_parquet_file) and
                os.path.getsize(self.temp_parquet_file) > 0):
            logger.info("Temporary Parquet file '%s' is non-existent or empty. Nothing to merge.", self.temp_parquet_file)
            return

        try:
            temp_df = pd.read_parquet(self.temp_parquet_file) # Read all data from temp
            if not temp_df.empty:
                logger.info("Attempting to merge %d records from temporary file into main file.",
                            len(temp_df))
                # append_bars_to_parquet handles deduplication and sorting
                merge_appended_count = append_bars_to_parquet(
                    temp_df.to_dict('records'), self.main_parquet_file
                )
                logger.info("Merge process complete. Net %d unique bars were added/updated in the main file '%s'.",
                            merge_appended_count, self.main_parquet_file)
            else:
                logger.info("Temporary Parquet file '%s' read as an empty DataFrame. Nothing to merge.", self.temp_parquet_file)

            # Delete the temporary file after processing
            logger.info("Deleting temporary file: %s", self.temp_parquet_file)
            os.remove(self.temp_parquet_file)
        except Exception as e: # pylint: disable=broad-except
            logger.error("Error during merge operation or temporary file deletion for '%s': %s",
                         self.temp_parquet_file, e, exc_info=True)
            logger.warning("Temporary file '%s' may not have been deleted due to the error.", self.temp_parquet_file)

    def update_data(self) -> None:
        """
        Runs the full historical data update process.

        This involves:
        1. Determining the initial fetch start time based on existing data in the main file
           and any configured overrides.
        2. Performing an initial forward fill of data into a temporary Parquet file,
           resolving contract IDs daily.
        3. Checking for and filling any missing trading days (gaps) within the fetched
           period in the temporary file.
        4. Merging the contents of the temporary file into the main Parquet file.
        """
        # pylint: disable=too-many-locals, too-many-statements, too-many-branches
        logger.info("--- Starting Full Historical Data Update for Symbol: %s ---", self.symbol_root)
        logger.info("Target Main Parquet File: %s", self.main_parquet_file)
        if self.overall_start_date_override_obj:
            logger.info("Using Overall Start Date Override: %s",
                        self.overall_start_date_override_obj.isoformat())
        logger.info("Overall Target End Datetime for this run (UTC): %s",
                    self.overall_target_end_dt_utc.isoformat())

        # Ensure any pre-existing temp file is removed before starting
        if os.path.exists(self.temp_parquet_file):
            logger.info("Removing existing temporary file before update: %s", self.temp_parquet_file)
            try:
                os.remove(self.temp_parquet_file)
            except OSError as e:
                logger.error("Could not remove pre-existing temporary file '%s': %s. This might affect the update.",
                             self.temp_parquet_file, e)
                # Depending on desired robustness, might raise an error or try to proceed

        # --- Step 1: Initial Forward Fill ---
        last_known_ts_main_file = get_last_timestamp_from_parquet(self.main_parquet_file)
        initial_fill_start_utc = self._determine_initial_fill_start_utc(last_known_ts_main_file)

        if initial_fill_start_utc >= self.overall_target_end_dt_utc:
            logger.info("Calculated initial fill start datetime %s is at or after the target end datetime %s. "
                        "No initial forward fill will be performed. Proceeding to gap check and merge if temp file exists.",
                        initial_fill_start_utc.isoformat(), self.overall_target_end_dt_utc.isoformat())
        else:
            self._perform_initial_fill(initial_fill_start_utc)

        # --- Step 2: Gap Filling ---
        # Determine range for gap checking based on initial fill start and overall end
        gap_check_start_date_effective = self.overall_start_date_override_obj or \
                                       initial_fill_start_utc.date()
        gap_check_end_date_effective = self.overall_target_end_dt_utc.date()
        
        gaps_filled_bars_count = self._perform_gap_filling(
            gap_check_start_date=gap_check_start_date_effective,
            gap_check_end_date=gap_check_end_date_effective
        )
        logger.info("--- Step 2: Gap Filling Phase Complete. Approximately %d bars added during gap passes. ---",
                    gaps_filled_bars_count)
        # Note: self.total_new_bars_appended_this_run is cumulative from initial fill + gap fills

        # --- Step 3: Merge Temp into Main ---
        self._merge_temp_into_main()

        logger.info("--- Historical Data Update Process Fully Complete for Symbol: %s. ---", self.symbol_root)
        logger.info("--- Total new bars (initial fill + gaps) potentially processed and written to temp file "
                    "during this run: %d ---", self.total_new_bars_appended_this_run)