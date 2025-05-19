# tsxapipy/historical/updater.py
"""
Module for updating historical market data using Parquet files.
"""
import logging
from datetime import datetime, timedelta, time as dt_time, date
import time
import os
from typing import Optional, Any, Tuple

import pandas as pd  # pylint: disable=import-error

from tsxapipy.common.time_utils import UTC_TZ
from tsxapipy.api.client import APIClient, MAX_BARS_PER_REQUEST
# CORRECTED IMPORT: Use the new function name from contract_utils
from tsxapipy.api.contract_utils import get_futures_contract_details
from tsxapipy.historical.parquet_handler import (
    get_last_timestamp_from_parquet,
    append_bars_to_parquet
)
# Assuming gap_detector.py is in the same historical package
from .gap_detector import find_missing_trading_days
from tsxapipy.api.exceptions import APIError

logger = logging.getLogger(__name__)

# Local constant for _fetch_and_append_data_for_window
_MAX_STALLED_FETCHES_TO_BREAK_WINDOW = 3

def calculate_next_interval_start(last_timestamp_utc: datetime,
                                  api_bar_unit: int,
                                  api_bar_unit_number: int) -> datetime:
    """Calculates the start datetime for the next data fetching interval."""
    if not isinstance(last_timestamp_utc, datetime):
        raise TypeError("last_timestamp_utc must be a datetime object.")

    if last_timestamp_utc.tzinfo is None:
        last_timestamp_utc = UTC_TZ.localize(last_timestamp_utc)
        logger.warning("calculate_next_interval_start received naive timestamp. Localized to UTC: %s",
                       last_timestamp_utc.isoformat())
    elif last_timestamp_utc.tzinfo.utcoffset(last_timestamp_utc) != timedelta(0):
        last_timestamp_utc = last_timestamp_utc.astimezone(UTC_TZ)
        logger.warning("calculate_next_interval_start received non-UTC timestamp. Converted to UTC: %s",
                       last_timestamp_utc.isoformat())

    if api_bar_unit == 2: delta = timedelta(minutes=api_bar_unit_number)
    elif api_bar_unit == 3: delta = timedelta(hours=api_bar_unit_number)
    elif api_bar_unit == 4:
        return (last_timestamp_utc.replace(hour=0, minute=0, second=0, microsecond=0) +
                timedelta(days=api_bar_unit_number))
    else:
        logger.warning("Unsupported api_bar_unit %s for next interval calc. Defaulting to +1 sec.",
                       api_bar_unit)
        return last_timestamp_utc + timedelta(seconds=1)
    return last_timestamp_utc + delta


class HistoricalDataUpdater:
    """
    Manages fetching and updating historical bar data into Parquet files.

    Determines active contract daily, uses integer ID for history API if available,
    and stores source string contract ID. Accepts overall start/end date overrides.
    """
    # pylint: disable=too-many-instance-attributes, too-many-arguments
    def __init__(self, api_client: APIClient,
                 symbol_root: str, main_parquet_file: str,
                 temp_file_suffix: str, api_bar_unit: int, api_bar_unit_number: int,
                 contract_override: Optional[str] = None,
                 fetch_days_if_new: int = 90,
                 sleep_between_requests: float = 0.25, max_gap_fill_passes: int = 3,
                 temp_file_dir: Optional[str] = None,
                 overall_start_date_override: Optional[str] = None,  # YYYY-MM-DD
                 overall_end_date_override: Optional[str] = None):  # YYYY-MM-DD
        """Initializes the HistoricalDataUpdater. See class docstring for details."""
        if not isinstance(api_client, APIClient):
            raise TypeError("api_client must be an instance of APIClient.")
        self.api_client = api_client
        self.symbol_root = symbol_root.upper()
        self.main_parquet_file = main_parquet_file
        self.api_bar_unit, self.api_bar_unit_number = api_bar_unit, api_bar_unit_number
        self.contract_override_str: Optional[str] = contract_override
        self.contract_override_int: Optional[int] = None
        self.fetch_days_if_new = fetch_days_if_new
        self.sleep_between_requests = sleep_between_requests
        self.max_gap_fill_passes = max_gap_fill_passes

        self.temp_parquet_file = self._determine_temp_filepath(
            main_parquet_file, temp_file_suffix, temp_file_dir
        )
        logger.info("HistoricalDataUpdater for '%s'. Main: '%s', Temp: '%s'",
                    self.symbol_root, self.main_parquet_file, self.temp_parquet_file)

        self.overall_target_end_dt_utc = self._parse_overall_end_date(overall_end_date_override)
        self.overall_start_date_override_obj = self._parse_overall_start_date(
            overall_start_date_override
        )

        self.total_new_bars_appended_this_run: int = 0
        self._override_logged_this_instance: bool = False

        if self.contract_override_str:
            self._resolve_contract_override_id()

    def _determine_temp_filepath(self, main_file: str, suffix: str, temp_dir: Optional[str]) -> str:
        main_d = os.path.dirname(main_file) or "."
        main_base, main_ext = os.path.splitext(os.path.basename(main_file))
        effective_ext = main_ext if main_ext and main_ext.lower() == ".parquet" else ".parquet"
        temp_filename = f"{main_base}{suffix}{effective_ext}"

        final_temp_dir = temp_dir or main_d
        if not os.path.exists(final_temp_dir):
            try:
                os.makedirs(final_temp_dir, exist_ok=True)
                logger.info("Created temp dir: %s", final_temp_dir)
            except OSError as e:
                logger.error("Could not create temp_dir '%s': %s. Using main dir: %s",
                             final_temp_dir, e, main_d)
                final_temp_dir = main_d
        return os.path.join(final_temp_dir, temp_filename)

    def _parse_overall_end_date(self, override_str: Optional[str]) -> datetime:
        target_end_dt = datetime.now(UTC_TZ)  # Default to now
        if override_str:
            try:
                dt_obj = datetime.strptime(override_str, "%Y-%m-%d")
                # End of specified day for inclusivity
                target_end_dt = UTC_TZ.localize(datetime.combine(dt_obj.date(), dt_time.max))
                logger.info("  Run end date overridden by CLI to: %s", target_end_dt.isoformat())
            except ValueError:
                logger.error("Invalid overall_end_date_override format '%s'. Using current time.",
                             override_str)
        return target_end_dt

    def _parse_overall_start_date(self, override_str: Optional[str]) -> Optional[date]:
        start_date_obj = None
        if override_str:
            try:
                start_date_obj = datetime.strptime(override_str, "%Y-%m-%d").date()
                logger.info("  Run start date override set to: %s", start_date_obj.isoformat())
            except ValueError:
                logger.error("Invalid overall_start_date_override format '%s'. Ignoring override.",
                             override_str)
        return start_date_obj

    def _resolve_contract_override_id(self) -> None:
        logger.info("Contract override provided: '%s'. Attempting to resolve details.",
                    self.contract_override_str)
        try:
            self.contract_override_int = int(self.contract_override_str) # type: ignore[arg-type]
            logger.info("  Interpreted contract_override '%s' as integer ID: %s.",
                        self.contract_override_str, self.contract_override_int)
        except ValueError:
            logger.info("  Contract_override '%s' is string. Attempting numeric ID lookup.",
                        self.contract_override_str)
            # Use a recent date for lookup; this part is heuristic
            temp_date_for_lookup = self.overall_target_end_dt_utc.date()
            details = get_futures_contract_details(self.api_client,
                                                   temp_date_for_lookup,
                                                   self.symbol_root)
            if details and details[0] == self.contract_override_str:
                self.contract_override_int = details[1]
                if self.contract_override_int:
                    logger.info("  Resolved numeric ID for override '%s' to %s.",
                                self.contract_override_str, self.contract_override_int)
                else:
                    logger.warning("  Could not resolve numeric ID for string override '%s'.",
                                   self.contract_override_str)
            else:
                logger.warning("  String contract_override '%s' doesn't match current dynamic "
                               "contract for '%s'. Numeric ID not reliably determined.",
                               self.contract_override_str, self.symbol_root)

    def _get_current_contract_ids_for_fetch(self,
                                          for_date_obj: date
                                          ) -> Optional[Tuple[str, Optional[int]]]:
        """
        Determines string and integer contract IDs for a given date, using override if set.
        """
        if self.contract_override_str:
            if not self._override_logged_this_instance:
                logger.info("Updater: Using fixed contract_override (String: '%s', Int: %s).",
                            self.contract_override_str, self.contract_override_int)
                self._override_logged_this_instance = True
            return self.contract_override_str, self.contract_override_int

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
        """Fetches and appends data, using numeric_id for API if available."""
        # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        param_for_history_api: Any
        log_id_type = "Numeric"
        if numeric_contract_id_for_api is not None:
            param_for_history_api = numeric_contract_id_for_api
        else:
            param_for_history_api = string_contract_id_for_source_col
            log_id_type = "String - numeric not found/resolved"
            if not self.contract_override_str:
                logger.warning("Numeric contract ID not found for dynamically determined '%s'. "
                               "Using string ID. Verify API requirements.",
                               string_contract_id_for_source_col)

        logger.info("--- Fetching: Contract(source):'%s', API Call ID: '%s' (%s), "
                    "Window: %s to %s -> %s ---",
                    string_contract_id_for_source_col, param_for_history_api, log_id_type,
                    window_start_dt_utc.isoformat(), window_end_dt_utc.isoformat(),
                    os.path.basename(target_file_to_append_to))

        current_fetch_point_utc = window_start_dt_utc
        appended_total = 0
        previous_newest_bar_ts: Optional[datetime] = None
        consecutive_stalled_fetches = 0

        while current_fetch_point_utc < window_end_dt_utc:
            start_iso = current_fetch_point_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_iso = window_end_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            try:
                chunk = self.api_client.get_historical_bars(
                    contract_id=param_for_history_api,
                    start_time_iso=start_iso, end_time_iso=end_iso,
                    unit=self.api_bar_unit, unit_number=self.api_bar_unit_number,
                    limit=MAX_BARS_PER_REQUEST, include_partial_bar=False
                )
            except APIError as e:
                logger.error("API error for %s (API param %s) from %s: %s. Ending window.",
                             string_contract_id_for_source_col, param_for_history_api, start_iso, e)
                break
            if not chunk:
                logger.info("No bars for %s (API param %s) in %s-%s. Window complete.",
                            string_contract_id_for_source_col, param_for_history_api,
                            start_iso, end_iso)
                break

            bars_to_add = [{'contract_id_source': string_contract_id_for_source_col, **bar_data}
                           for bar_data in chunk]
            try:
                appended_now = append_bars_to_parquet(bars_to_add, target_file_to_append_to)
                if appended_now > 0:
                    appended_total += appended_now
                    consecutive_stalled_fetches = 0
                elif bars_to_add: # API returned bars, but none new were appended
                    logger.debug("API returned %d bars, but 0 new were appended (duplicates).",
                                 len(bars_to_add))
                    consecutive_stalled_fetches += 1
            except Exception as e: # pylint: disable=broad-except
                logger.error("Error appending to %s: %s. Ending window.",
                             target_file_to_append_to, e, exc_info=True)
                break

            try:
                newest_dt_py = pd.to_datetime(chunk[-1]['t']).to_pydatetime()
                newest_utc = UTC_TZ.normalize(UTC_TZ.localize(newest_dt_py)
                                            if newest_dt_py.tzinfo is None
                                            else newest_dt_py.astimezone(UTC_TZ))
            except Exception as e_ts: # pylint: disable=broad-except
                logger.error("Bad timestamp '%s' from API for %s: %s. Breaking.",
                             chunk[-1]['t'], string_contract_id_for_source_col, e_ts)
                break

            if previous_newest_bar_ts == newest_utc and \
               len(chunk) >= MAX_BARS_PER_REQUEST and appended_now == 0:
                logger.error("Stalled for %s: Newest bar %s same, no new data. Ending window.",
                             string_contract_id_for_source_col, newest_utc.isoformat())
                break
            if consecutive_stalled_fetches >= _MAX_STALLED_FETCHES_TO_BREAK_WINDOW and \
               len(chunk) >= MAX_BARS_PER_REQUEST:
                logger.error("Halting for %s: %d full API calls yielded no new data.",
                             string_contract_id_for_source_col,
                             _MAX_STALLED_FETCHES_TO_BREAK_WINDOW)
                break

            previous_newest_bar_ts = newest_utc
            current_fetch_point_utc = calculate_next_interval_start(
                newest_utc, self.api_bar_unit, self.api_bar_unit_number
            )
            if len(chunk) < MAX_BARS_PER_REQUEST:
                logger.debug("Got %d bars (< limit). Window for %s likely done.",
                             len(chunk), string_contract_id_for_source_col)
                break
            if self.sleep_between_requests > 0 and current_fetch_point_utc < window_end_dt_utc:
                time.sleep(self.sleep_between_requests)

        logger.info("--- Finished fetch for window (%s). Appended %d bars to %s. ---",
                    string_contract_id_for_source_col, appended_total,
                    os.path.basename(target_file_to_append_to))
        return appended_total

    def _determine_initial_fill_start_utc(self,
                                          last_known_ts_main: Optional[datetime]) -> datetime:
        """Determines the starting UTC timestamp for the initial data fill."""
        if self.overall_start_date_override_obj:
            base_start_dt = UTC_TZ.localize(
                datetime.combine(self.overall_start_date_override_obj, dt_time.min)
            )
            if last_known_ts_main and last_known_ts_main >= base_start_dt:
                initial_fill_start_utc = calculate_next_interval_start(
                    last_known_ts_main, self.api_bar_unit, self.api_bar_unit_number
                )
                logger.info("Start date override '%s' effective, but existing data up to %s. "
                            "Adjusted fill start: %s",
                            self.overall_start_date_override_obj.isoformat(),
                            last_known_ts_main.isoformat(), initial_fill_start_utc.isoformat())
            else:
                initial_fill_start_utc = base_start_dt
                logger.info("Start date override used. Initial fill starts from: %s",
                            initial_fill_start_utc.isoformat())
        elif last_known_ts_main:
            initial_fill_start_utc = calculate_next_interval_start(
                last_known_ts_main, self.api_bar_unit, self.api_bar_unit_number
            )
            logger.info("Last known TS in main: %s. Initial fill starts from: %s",
                        last_known_ts_main.isoformat(), initial_fill_start_utc.isoformat())
        else:  # No override, no existing data
            initial_fill_start_utc = self.overall_target_end_dt_utc - \
                                     timedelta(days=self.fetch_days_if_new)
            if self.api_bar_unit == 4: # Align daily bars to start of day
                initial_fill_start_utc = UTC_TZ.localize(
                    datetime.combine(initial_fill_start_utc.date(), dt_time.min)
                )
            logger.info("Main file new/empty. Initial fill (last %d days) starts from: %s",
                        self.fetch_days_if_new, initial_fill_start_utc.isoformat())
        return initial_fill_start_utc

    def _perform_initial_fill(self, initial_fill_start_utc: datetime) -> None:
        """Performs the initial forward fill of data to the temporary Parquet file."""
        logger.info("--- Step 1: Initial Forward Fill to Temp File: %s ---", self.temp_parquet_file)
        current_day_for_contract_logic = initial_fill_start_utc.date()
        fetch_start_utc = initial_fill_start_utc
        self.total_new_bars_appended_this_run = 0

        while (fetch_start_utc < self.overall_target_end_dt_utc and
               current_day_for_contract_logic <= self.overall_target_end_dt_utc.date()):
            if current_day_for_contract_logic.weekday() >= 5:  # Skip weekends
                current_day_for_contract_logic += timedelta(days=1)
                fetch_start_utc = max(fetch_start_utc, UTC_TZ.localize(
                    datetime.combine(current_day_for_contract_logic, dt_time.min)))
                continue

            contract_details = self._get_current_contract_ids_for_fetch(
                current_day_for_contract_logic
            )
            if not contract_details or not contract_details[0]:
                logger.error("No contract details for %s. Skipping day for initial fill.",
                             current_day_for_contract_logic)
                current_day_for_contract_logic += timedelta(days=1)
                fetch_start_utc = max(fetch_start_utc, UTC_TZ.localize(
                    datetime.combine(current_day_for_contract_logic, dt_time.min)))
                continue

            string_id, numeric_id = contract_details
            day_end_boundary = min(
                UTC_TZ.localize(datetime.combine(current_day_for_contract_logic, dt_time.max)),
                self.overall_target_end_dt_utc
            )

            if fetch_start_utc < day_end_boundary:
                logger.debug("Initial fill: Day %s, Contract %s, Window %s - %s",
                             current_day_for_contract_logic, string_id,
                             fetch_start_utc.isoformat(), day_end_boundary.isoformat())
                appended_for_day = self._fetch_and_append_data_for_window(
                    string_id, numeric_id,
                    fetch_start_utc,
                    day_end_boundary,
                    self.temp_parquet_file
                )
                self.total_new_bars_appended_this_run += appended_for_day
            else:
                logger.debug("Initial fill: Skipping day %s, fetch window invalid (start %s >= end %s).",
                             current_day_for_contract_logic, fetch_start_utc.isoformat(),
                             day_end_boundary.isoformat())

            current_day_for_contract_logic += timedelta(days=1)
            fetch_start_utc = max(fetch_start_utc, UTC_TZ.localize(
                datetime.combine(current_day_for_contract_logic, dt_time.min)))

            if self.sleep_between_requests > 0 and fetch_start_utc < self.overall_target_end_dt_utc:
                time.sleep(self.sleep_between_requests)
        logger.info("--- Step 1: Initial Fill Complete. Appended %d bars to temp file. ---",
                    self.total_new_bars_appended_this_run)

    def _perform_gap_filling(self, gap_check_start_date: date,
                             gap_check_end_date: date) -> int:
        """Checks for and fills gaps in the temporary Parquet file."""
        logger.info("--- Step 2: Checking and Filling Gaps in Temp File: %s ---",
                    self.temp_parquet_file)
        total_gaps_filled_bars = 0

        if not (os.path.exists(self.temp_parquet_file) and
                os.path.getsize(self.temp_parquet_file) > 0):
            logger.info("Temp file empty or non-existent. Skipping gap check.")
            return 0

        for i_pass in range(self.max_gap_fill_passes):
            logger.info("Gap fill pass %d/%d from %s to %s...",
                        i_pass + 1, self.max_gap_fill_passes,
                        gap_check_start_date.isoformat(), gap_check_end_date.isoformat())
            missing_days = find_missing_trading_days(self.temp_parquet_file,
                                                    gap_check_start_date,
                                                    gap_check_end_date)
            if not missing_days:
                logger.info("No missing trading days in temp file. Gap filling complete.")
                break

            bars_added_this_pass = 0
            for idx, gap_date in enumerate(missing_days):
                gap_day_start_utc = UTC_TZ.localize(datetime.combine(gap_date, dt_time.min))
                gap_day_end_utc = min(
                    UTC_TZ.localize(datetime.combine(gap_date, dt_time.max)),
                    self.overall_target_end_dt_utc
                )
                contract_details_gap = self._get_current_contract_ids_for_fetch(gap_date)
                if not contract_details_gap or not contract_details_gap[0]:
                    logger.error("No contract ID for gap day %s. Skipping.", gap_date.isoformat())
                    continue
                str_id_g, num_id_g = contract_details_gap

                logger.debug("Gap fill: Day %s, Contract %s, Window %s - %s",
                             gap_date, str_id_g, gap_day_start_utc.isoformat(),
                             gap_day_end_utc.isoformat())
                appended_for_gap = self._fetch_and_append_data_for_window(
                    str_id_g, num_id_g, gap_day_start_utc, gap_day_end_utc, self.temp_parquet_file
                )
                if appended_for_gap > 0:
                    bars_added_this_pass += appended_for_gap
                    self.total_new_bars_appended_this_run += appended_for_gap

                if self.sleep_between_requests > 0 and idx < len(missing_days) - 1:
                    time.sleep(self.sleep_between_requests)

            total_gaps_filled_bars += bars_added_this_pass
            if bars_added_this_pass == 0:
                logger.info("No further gaps filled in pass %d.", i_pass + 1)
                break
            # No 'else' needed here due to `no-else-break` Pylint check
            logger.info("Gap fill pass %d appended %d bars.",
                        i_pass + 1, bars_added_this_pass)
        return total_gaps_filled_bars

    def _merge_temp_into_main(self) -> None:
        """Merges the temporary Parquet file into the main Parquet file."""
        logger.info("--- Step 3: Merging %s into %s ---",
                    self.temp_parquet_file, self.main_parquet_file)
        if not (os.path.exists(self.temp_parquet_file) and
                os.path.getsize(self.temp_parquet_file) > 0):
            logger.info("Temporary file non-existent or empty. Nothing to merge.")
            return

        try:
            temp_df = pd.read_parquet(self.temp_parquet_file)
            if not temp_df.empty:
                logger.info("Attempting to merge %d records from temp file into main file.",
                            len(temp_df))
                merge_appended_count = append_bars_to_parquet(
                    temp_df.to_dict('records'), self.main_parquet_file
                )
                logger.info("Merge: Net %d unique bars added/updated in main file %s.",
                            merge_appended_count, self.main_parquet_file)
            else:
                logger.info("Temporary file read as empty DataFrame. Nothing to merge.")

            logger.info("Deleting temporary file: %s", self.temp_parquet_file)
            os.remove(self.temp_parquet_file)
        except Exception as e: # pylint: disable=broad-except
            logger.error("Error during merge or temp file deletion (%s): %s",
                         self.temp_parquet_file, e, exc_info=True)
            logger.warning("Temporary file may not have been deleted due to error.")

    def update_data(self) -> None:
        """Runs the full historical data update process."""
        # pylint: disable=too-many-locals, too-many-statements, too-many-branches
        logger.info("--- Starting data update for %s -> %s ---",
                    self.symbol_root, self.main_parquet_file)
        if self.overall_start_date_override_obj:
            logger.info("--- Using CLI start date override: %s ---",
                        self.overall_start_date_override_obj.isoformat())
        logger.info("--- Overall target end datetime for this run: %s ---",
                    self.overall_target_end_dt_utc.isoformat())

        if os.path.exists(self.temp_parquet_file):
            logger.info("Removing existing temp file: %s", self.temp_parquet_file)
            try:
                os.remove(self.temp_parquet_file)
            except OSError as e:
                logger.error("Could not remove temp file %s: %s.", self.temp_parquet_file, e)

        last_known_ts_main = get_last_timestamp_from_parquet(self.main_parquet_file)
        initial_fill_start_utc = self._determine_initial_fill_start_utc(last_known_ts_main)

        if initial_fill_start_utc >= self.overall_target_end_dt_utc:
            logger.info("Calculated initial fill start %s is at or after target end %s. "
                        "No initial fill.", initial_fill_start_utc.isoformat(),
                        self.overall_target_end_dt_utc.isoformat())
        else:
            self._perform_initial_fill(initial_fill_start_utc)

        gap_check_start_date = self.overall_start_date_override_obj or \
                               initial_fill_start_utc.date()
        gap_check_end_date = self.overall_target_end_dt_utc.date()
        gaps_filled_count = self._perform_gap_filling(gap_check_start_date, gap_check_end_date)

        logger.info("--- Step 2: Gap Filling Complete. Total %d bars added during gap passes. ---",
                    gaps_filled_count)

        self._merge_temp_into_main()

        logger.info("--- Update Process Fully Complete for %s. ---", self.symbol_root)
        logger.info("--- Total new bars (initial fill + gaps) potentially added to temp file "
                    "this run: %d ---", self.total_new_bars_appended_this_run)