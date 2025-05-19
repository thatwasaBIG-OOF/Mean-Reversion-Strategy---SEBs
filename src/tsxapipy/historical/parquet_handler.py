# tsxapipy/historical/parquet_handler.py
"""
Module for handling Parquet file operations for historical bar data.
"""
import os
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple

import pandas as pd  # pylint: disable=import-error

from tsxapipy.common.time_utils import UTC_TZ

logger = logging.getLogger(__name__)

class ParquetHandlerError(IOError):
    """Base error for Parquet handling issues within this module."""
    pass

class ParquetReadError(ParquetHandlerError):
    """Error encountered during Parquet file reading operations."""
    pass

class ParquetWriteError(ParquetHandlerError):
    """Error encountered during Parquet file writing or appending operations."""
    pass


def get_last_timestamp_from_parquet(parquet_filepath: str) -> Optional[datetime]:
    """Reads a Parquet file and returns the latest timestamp from the 't' column (UTC)."""
    try:
        if not os.path.exists(parquet_filepath) or os.path.getsize(parquet_filepath) == 0:
            logger.debug("Parquet file '%s' not found or empty for last timestamp retrieval.",
                         parquet_filepath)
            return None

        df = pd.read_parquet(parquet_filepath, engine='pyarrow', columns=['t'])
        if df.empty or 't' not in df.columns:
            logger.debug("Parquet file '%s' is empty after read or no 't' column.",
                         parquet_filepath)
            return None

        df_valid_t = df.dropna(subset=['t'])
        if df_valid_t.empty:
            logger.debug("No valid (non-NaT) timestamps in 't' column of '%s'.",
                         parquet_filepath)
            return None

        last_timestamp_pandas = df_valid_t['t'].max()
        if pd.isna(last_timestamp_pandas):
            logger.debug("Max timestamp is NaT in '%s' after dropna.", parquet_filepath)
            return None

        last_timestamp_datetime = last_timestamp_pandas.to_pydatetime()
        if last_timestamp_datetime.tzinfo is None:
            last_timestamp_utc = UTC_TZ.localize(last_timestamp_datetime)
        else:
            last_timestamp_utc = last_timestamp_datetime.astimezone(UTC_TZ)

        logger.debug("Last timestamp from '%s': %s",
                     parquet_filepath, last_timestamp_utc.isoformat())
        return last_timestamp_utc
    except FileNotFoundError:
        logger.info("Parquet file '%s' not found (race condition?).", parquet_filepath)
        return None
    except Exception as e: # pylint: disable=broad-except
        logger.error("Error reading last timestamp from '%s': %s",
                     parquet_filepath, e, exc_info=True)
        return None

def _prepare_new_bars_df(new_bars_data: List[Dict[str, Any]],
                         expected_cols: List[str],
                         parquet_filepath: str) -> pd.DataFrame:
    """Creates and prepares a DataFrame from new bar data."""
    if not new_bars_data:
        logger.debug("append_bars_to_parquet: No new bars to append (input list is empty).")
        return pd.DataFrame()

    new_df = pd.DataFrame(new_bars_data)
    if new_df.empty:
        logger.debug("append_bars_to_parquet: DataFrame from new_bars_data is empty.")
        return pd.DataFrame()

    if 't' not in new_df.columns:
        raise ValueError("Timestamp column 't' is missing in new_bars_data.")
    if 'contract_id_source' not in new_df.columns:
        logger.warning("Column 'contract_id_source' missing. Appending with 'UNKNOWN'.")
        new_df['contract_id_source'] = 'UNKNOWN'

    try:
        new_df['t'] = pd.to_datetime(new_df['t'], errors='coerce', utc=True)
    except Exception as e_ts_convert:
        logger.error("Critical error converting 't' column for %s: %s",
                     parquet_filepath, e_ts_convert, exc_info=True)
        raise ValueError(f"Timestamp conversion error: {e_ts_convert}") from e_ts_convert

    new_df.dropna(subset=['t'], inplace=True)
    if new_df.empty:
        logger.info("No valid bars with convertible timestamps in new_df for %s.",
                    parquet_filepath)
        return pd.DataFrame()

    for col in expected_cols:
        if col == 't':
            continue
        if col == 'contract_id_source':
            if col not in new_df.columns: new_df[col] = pd.NA # Should be present
            new_df[col] = new_df[col].astype(str)
        elif col in new_df.columns: # o, h, l, c, v
            new_df[col] = pd.to_numeric(new_df[col], errors='coerce')
        else:
            logger.warning("Column '%s' missing in new_bars_data for %s. Added with NA.",
                         col, parquet_filepath)
            new_df[col] = pd.NA
    return new_df.reindex(columns=expected_cols)


def _read_and_combine_dfs(parquet_filepath: str,
                            bars_to_process_df: pd.DataFrame
                           ) -> Tuple[pd.DataFrame, int]:
    """Reads existing Parquet, combines with new data, returns combined_df and original_len."""
    len_original_existing_df = 0
    if os.path.exists(parquet_filepath) and os.path.getsize(parquet_filepath) > 0:
        try:
            logger.debug("Reading existing data from %s for merge.", parquet_filepath)
            existing_df = pd.read_parquet(parquet_filepath, engine='pyarrow')
            len_original_existing_df = len(existing_df)

            if not existing_df.empty and 't' in existing_df.columns:
                # Ensure existing 't' is UTC datetime
                if not pd.api.types.is_datetime64_any_dtype(existing_df['t']):
                    existing_df['t'] = pd.to_datetime(existing_df['t'], errors='coerce', utc=True)
                elif existing_df['t'].dt.tz is None:
                    existing_df['t'] = existing_df['t'].dt.tz_localize(UTC_TZ)
                else:
                    existing_df['t'] = existing_df['t'].dt.tz_convert(UTC_TZ)
                existing_df.dropna(subset=['t'], inplace=True)

                if not existing_df.empty:
                    last_existing_ts = existing_df['t'].max()
                    if pd.notna(last_existing_ts): # Filter new bars
                        bars_to_process_df = \
                            bars_to_process_df[bars_to_process_df['t'] > last_existing_ts]

                if bars_to_process_df.empty:
                    logger.info("All new bars are older/same as existing in %s. No append.",
                                parquet_filepath)
                    # Return existing_df to signify no new data to effectively add
                    return existing_df if not existing_df.empty else pd.DataFrame(), \
                           len_original_existing_df

                combined_df = pd.concat([existing_df, bars_to_process_df], ignore_index=True)
            else: # existing_df is empty or lacks 't'
                combined_df = bars_to_process_df
        except Exception as read_exc: # pylint: disable=broad-except
            logger.error("Could not read/process existing Parquet %s: %s. "
                         "Writing new data as new file.",
                         parquet_filepath, read_exc, exc_info=True)
            combined_df = bars_to_process_df
            len_original_existing_df = 0 # Treat as new file
    else:
        logger.info("Parquet file %s new or empty. Will create/overwrite.", parquet_filepath)
        combined_df = bars_to_process_df
    return combined_df, len_original_existing_df

def _finalize_and_write_df(combined_df: pd.DataFrame, parquet_filepath: str,
                             expected_dtypes: Dict[str, str],
                             len_original_existing_df: int) -> int:
    """Finalizes dtypes, sorts, deduplicates, and writes DataFrame to Parquet."""
    if combined_df.empty:
        logger.info("No data to write to %s after processing and filtering.", parquet_filepath)
        return 0

    # Apply schema / dtypes before writing
    for col, target_dtype_str in expected_dtypes.items():
        if col not in combined_df.columns: continue # Should be handled by reindex earlier

        current_dtype = combined_df[col].dtype
        try:
            if col == 't':
                if not pd.api.types.is_datetime64_any_dtype(current_dtype) or \
                   getattr(current_dtype, 'tz', None) != UTC_TZ:
                    combined_df[col] = pd.to_datetime(combined_df[col], errors='coerce', utc=True)
            elif target_dtype_str == 'object' and pd.api.types.is_string_dtype(current_dtype):
                pass # Already string-like
            elif str(current_dtype) != target_dtype_str:
                if target_dtype_str == 'float64':
                    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
                elif target_dtype_str == 'object':
                    combined_df[col] = combined_df[col].astype(str)
                else:
                    combined_df[col] = combined_df[col].astype(target_dtype_str, errors='ignore')
        except Exception as e_astype: # pylint: disable=broad-except
            logger.warning("Could not coerce column '%s' to '%s', leaving as '%s'. Error: %s",
                           col, target_dtype_str, current_dtype, e_astype)

    combined_df.sort_values(by='t', inplace=True)
    num_rows_before_dedup = len(combined_df)
    combined_df.drop_duplicates(subset=['t'], keep='last', inplace=True)
    if num_rows_before_dedup > len(combined_df):
        logger.info("Dropped %d duplicate(s) by timestamp for %s.",
                    num_rows_before_dedup - len(combined_df), parquet_filepath)

    output_dir = os.path.dirname(parquet_filepath)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logger.info("Created output directory for Parquet file: %s", output_dir)

    # Final check for 't' column timezone awareness
    if 't' in combined_df.columns and pd.api.types.is_datetime64_any_dtype(combined_df['t']):
        if combined_df['t'].dt.tz is None:
            combined_df['t'] = combined_df['t'].dt.tz_localize(UTC_TZ)
        elif str(combined_df['t'].dt.tz) != str(UTC_TZ):
            combined_df['t'] = combined_df['t'].dt.tz_convert(UTC_TZ)
    else:
        logger.error("Column 't' not datetime before writing to %s. Data might be incorrect.",
                     parquet_filepath)

    combined_df.to_parquet(parquet_filepath, engine='pyarrow', index=False)
    final_appended_count = len(combined_df) - len_original_existing_df
    logger.info("Successfully wrote to %s. Net change: %d rows. Total rows: %d",
                parquet_filepath, final_appended_count, len(combined_df))
    return max(0, final_appended_count)


def append_bars_to_parquet(new_bars_data: List[Dict[str, Any]], parquet_filepath: str) -> int:
    """
    Appends new bar data to a Parquet file, avoiding duplicates.

    Ensures 't' (UTC datetime), OHLCV, 'contract_id_source'. Creates file if needed.

    Args:
        new_bars_data: List of bar dictionaries ('t', 'contract_id_source', OHLCV).
        parquet_filepath: Full path to the Parquet file.

    Returns:
        Net number of new, unique bars added.

    Raises:
        ValueError: If 't' or 'contract_id_source' missing or timestamp conversion fails.
        ParquetWriteError: For critical Parquet writing errors.
    """
    # pylint: disable=too-many-locals
    expected_columns = ['t', 'o', 'h', 'l', 'c', 'v', 'contract_id_source']
    expected_dtypes = {
        't': 'datetime64[ns, UTC]',
        'o': 'float64', 'h': 'float64', 'l': 'float64', 'c': 'float64', 'v': 'float64',
        'contract_id_source': 'object'
    }

    try:
        prepared_new_df = _prepare_new_bars_df(new_bars_data, expected_columns, parquet_filepath)
        if prepared_new_df.empty and not new_bars_data: # No input data
            return 0
        if prepared_new_df.empty and new_bars_data: # Input data became empty after prep
             logger.info("No processable bars after preparation for %s.", parquet_filepath)
             return 0


        combined_df, len_original_existing_df = _read_and_combine_dfs(
            parquet_filepath, prepared_new_df.copy() # Pass copy to avoid modification issues
        )

        return _finalize_and_write_df(combined_df, parquet_filepath,
                                      expected_dtypes, len_original_existing_df)

    except ValueError as ve:
        logger.error("ValueError during Parquet operation for %s: %s",
                     parquet_filepath, ve, exc_info=True)
        raise
    except Exception as e: # pylint: disable=broad-except
        logger.error("Critical error updating Parquet file %s: %s",
                     parquet_filepath, e, exc_info=True)
        raise ParquetWriteError(f"Failed to write/append to {parquet_filepath}: {e}") from e