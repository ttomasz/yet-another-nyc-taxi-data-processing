import datetime
import logging
from typing import Union, Any

import numpy as np
import pandas as pd
from pandas._libs.missing import NAType

from helper_objects import column_name_mapping_dict, timer


@timer(logging.DEBUG)
def rename_columns(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names."""

    return data_frame.rename(columns=column_mapping_function)


@timer(logging.DEBUG)
def drop_invalid_coordinates(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where any of the coordinates is 0 or null."""

    data_frame = data_frame.dropna(
        subset=['pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude'])
    data_frame = data_frame[(data_frame['pickup_longitude'] != 0) & (data_frame['pickup_latitude'] != 0) &
                            (data_frame['dropoff_longitude'] != 0) & (data_frame['dropoff_latitude'] != 0)]
    return data_frame


@timer(logging.DEBUG)
def drop_invalid_timestamps(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Remove taxi rides ignoring laws of physics (going back in time).
    Drop where pickup is the same time or later than dropoff or either of the timestamps is null."""

    data_frame = data_frame.dropna(subset=['pickup_datetime', 'pickup_datetime'])
    data_frame = data_frame[data_frame['pickup_datetime'] < data_frame['dropoff_datetime']]
    return data_frame


@timer(logging.DEBUG)
def drop_negative_values(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with trip distance, total fare or passenger count below zero."""

    data_frame = data_frame[data_frame['trip_distance'].fillna(0) >= 0]
    data_frame = data_frame[data_frame['total_amount'].fillna(0) >= 0]
    data_frame = data_frame[data_frame['passenger_count'].fillna(0) >= 0]
    return data_frame


@timer(logging.DEBUG)
def drop_invalid_distances(data_frame: pd.DataFrame) -> pd.DataFrame:
    # we could compare reported distance with pickup and dropoff locations but that's kinda too much work
    raise NotImplementedError()


@timer(logging.DEBUG)
def replace_tip_values_for_cash_payments(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Change tip amount to null for cash transactions (better to have nulls for unknown values than zeroes)."""

    data_frame['tip_amount'].mask(data_frame['payment_type'] == 'cash', np.nan)
    return data_frame


@timer(logging.DEBUG)
def drop_invalid_trip_durations(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where we couldn't calculate trip duration or it was over 90 minutes."""

    data_frame = data_frame.dropna(subset=['trip_duration_minutes'])
    data_frame = data_frame[data_frame['trip_duration_minutes'] <= 90]
    return data_frame


@timer(logging.DEBUG)
def drop_invalid_year_values(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Remove records with dates that were wrong or parsed improperly."""

    data_frame = data_frame.dropna(subset=['year'])
    data_frame = data_frame[(data_frame['year'] >= 2009) & (data_frame['year'] < 2029)]
    return data_frame


@timer(logging.DEBUG)
def drop_missing_location_ids(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that don't have either pickup or dropoff location id."""

    return data_frame.dropna(subset=['pickup_location_id', 'dropoff_location_id'])


@timer(logging.DEBUG)
def standardize_snf_flag_values(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Replace values of store_and_forward with standardized versions."""

    data_frame['store_and_forward'] = data_frame['store_and_forward'].apply(store_and_fwd_flag_mapping_function).astype(
        pd.Int16Dtype())
    return data_frame


@timer(logging.DEBUG)
def standardize_payment_type_values(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Replace values of payment_type with standardized versions."""

    data_frame['payment_type'] = data_frame['payment_type'].apply(payment_type_mapping_function)
    return data_frame


def store_and_fwd_flag_mapping_function(x: Any) -> Union[int, NAType]:
    if x is None:
        return pd.NA
    elif str(x).upper() in {'1', 'Y', 'T'}:
        return 1
    elif str(x).upper() in {'0', 'N', 'F'}:
        return 0
    else:
        return pd.NA


def payment_type_mapping_function(type_id: Any) -> Union[str, NAType]:
    x = str(type_id).lower()
    if x in {'cre', 'crd', '1', 'credit'}:
        return 'credit card'
    elif x in {'cas', 'csh', '2', 'cash'}:
        return 'cash'
    elif x in {'no', 'noc', '3', 'no charge'}:
        return 'no charge'
    elif x in {'dis', '4', 'dispute'}:
        return 'dispute'
    elif x in {'5', 'unk', 'unknown'}:
        return 'unknown'
    elif x in {'6', 'voided trip'}:
        return 'voided trip'
    else:
        return pd.NA


def _lower_strip(s: str) -> str:
    return s.strip().lower()


def column_mapping_function(col: str) -> str:
    col = _lower_strip(col)
    return column_name_mapping_dict.get(col, col)


def trip_type_mapping_function(type_id: int) -> Union[str, NAType]:
    if type_id == 1:
        return 'Street-hail'
    elif type_id == 2:
        return 'Dispatch'
    else:
        return pd.NA


@timer(logging.DEBUG)
def add_trip_duration(data_frame: pd.DataFrame) -> pd.DataFrame:
    data_frame['trip_duration_minutes'] = data_frame['dropoff_datetime'] - data_frame['pickup_datetime']
    data_frame['trip_duration_minutes'] = data_frame['trip_duration_minutes'].dt.seconds / 60
    data_frame['trip_duration_minutes'] = data_frame['trip_duration_minutes'].astype(np.float32)
    return data_frame


@timer(logging.DEBUG)
def add_year(data_frame: pd.DataFrame) -> pd.DataFrame:
    data_frame['year'] = pd.DatetimeIndex(data_frame['pickup_datetime']).year
    data_frame['year'] = data_frame['year'].astype(np.int16)
    return data_frame


@timer(logging.DEBUG)
def add_additional_date_features(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Add columns with values computed from pickup date.

        Adds:
        - year_quarter (eg. 2019Q1),
        - year_month (eg. 2019-01),
        - quarter (1-4),
        - month (1-12),
        - date (eg. 2019-01-01),
        - day_of_week (1-7 where 1 is monday)
        - hour_of_day.
    """

    dti = pd.DatetimeIndex(data_frame['pickup_datetime'])

    data_frame['year_quarter'] = dti.year.astype(str) + 'Q' + dti.quarter.astype(str)
    data_frame['year_month'] = dti.strftime('%Y-%m')
    data_frame['quarter'] = dti.quarter.astype(np.int8)
    data_frame['month'] = dti.month.astype(np.int8)
    data_frame['date'] = dti.date
    data_frame['day_of_week'] = (dti.weekday + 1).astype(np.int8)
    data_frame['hour_of_day'] = dti.hour.astype(np.int8)

    return data_frame


@timer(logging.DEBUG)
def standardize_trip_type_values(data_frame: pd.DataFrame) -> pd.DataFrame:
    # if column doesn't exist add it with null values
    if 'trip_type' not in data_frame.columns:
        data_frame['trip_type'] = pd.NA
    else:
        data_frame['trip_type'] = data_frame['trip_type'].apply(trip_type_mapping_function)
    return data_frame


@timer(logging.DEBUG)
def drop_invalid_passenger_count_values(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where passenger count value is outside acceptable range [0,20]."""

    data_frame = data_frame[(data_frame['passenger_count'] <= 20) & (data_frame['passenger_count'] >= 0)]
    data_frame['passenger_count'] = data_frame['passenger_count'].astype(np.int8)
    return data_frame
