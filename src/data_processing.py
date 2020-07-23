import os
from sys import stdout

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from data_cleaning import rename_columns, drop_invalid_coordinates, drop_invalid_timestamps, \
    drop_negative_values, standardize_snf_flag_values, standardize_payment_type_values, \
    replace_tip_values_for_cash_payments, drop_invalid_trip_durations, drop_invalid_year_values, \
    drop_missing_location_ids, trip_type_mapping_function
from helper_objects import yellow_taxi_params, ParameterType, green_taxi_params, lookup_csv_path, \
    lookup_shp_path, arrow_schema, timer, print_sanity_stats


def _get_yellow_taxi_params(filename: str) -> ParameterType:
    for k in yellow_taxi_params.keys():
        if filename <= k:
            return yellow_taxi_params[k]


def _get_green_taxi_params(filename: str) -> ParameterType:
    for k in green_taxi_params.keys():
        if filename <= k:
            return green_taxi_params[k]


def process_taxi_data(df: pd.DataFrame, params: ParameterType, company: str) -> pd.DataFrame:
    """Applies cleaning rules and feature engineering on the provided DataFrame."""

    initial_number_of_rows = len(df.index)

    df = rename_columns(df)
    
    if params['location'] == 'coordinates':
        df = drop_invalid_coordinates(df)

    df = drop_invalid_timestamps(df)
    df = drop_negative_values(df)
    df = standardize_snf_flag_values(df)
    df = standardize_payment_type_values(df)
    df = replace_tip_values_for_cash_payments(df)

    # add trip duration
    df['trip_duration_minutes'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.seconds / 60
    df['trip_duration_minutes'] = df['trip_duration_minutes'].astype(np.float32)

    df = drop_invalid_trip_durations(df)

    # add date related fields for easier querying
    df['year'] = pd.DatetimeIndex(df['pickup_datetime']).year
    df['year'] = df['year'].astype(np.int16)

    df = drop_invalid_year_values(df)

#     df['year_quarter'] = pd.DatetimeIndex(df['pickup_datetime']).year.astype(str) + 'Q' + pd.DatetimeIndex(df['pickup_datetime']).quarter.astype(str)
#     df['year_month'] = pd.DatetimeIndex(df['pickup_datetime']).strftime('%Y-%m')
#     df['quarter'] = df['year'] = pd.DatetimeIndex(df['pickup_datetime']).quarter
#     df['month'] = df['year'] = pd.DatetimeIndex(df['pickup_datetime']).month
#     df['date'] = df['year'] = pd.DatetimeIndex(df['pickup_datetime']).date
#     df['day_of_week'] = pd.DatetimeIndex(df['pickup_datetime']).weekday + 1

    if 'trip_type' not in df.columns:
        df['trip_type'] = pd.NA
    else:
        df['trip_type'] = df['trip_type'].apply(trip_type_mapping_function)

    df.reset_index(drop=True, inplace=True)
    if params['location'] == 'id':
        df = _join_location_data_by_id(df)
    elif params['location'] == 'coordinates':
        df = _join_location_data_by_coordinates(df)

    df = drop_missing_location_ids(df)

    df['pickup_location_id'] = df['pickup_location_id'].astype(np.int16)
    df['dropoff_location_id'] = df['dropoff_location_id'].astype(np.int16)
    
    # assign company name
    df['company'] = company

    # info about processed DataFrame for sanity check
    final_number_of_rows = len(df.index)
    print_sanity_stats(initial_number_of_rows, final_number_of_rows)

    return df


def process_taxi_data_file(filepath: str) -> pd.DataFrame:
    """Reads file and applies cleaning rules and feature engineering."""

    filename = os.path.basename(filepath)

    if 'yellow' in filename:
        df = _process_yellow_taxi_data(filepath)
    elif 'green' in filename:
        df = _process_green_taxi_data(filepath)
    else:
        raise ValueError(f'Couldn\'t determine how to parse given path: {filepath}')
    return df


def _process_yellow_taxi_data(filepath: str, **kwargs) -> pd.DataFrame:
    filename = os.path.basename(filepath)
    params = _get_yellow_taxi_params(filename)
    
    df = _read_csv(filepath, **params['csv_params'], **kwargs)
    return process_taxi_data(df, params=params, company='yellow')


def _process_green_taxi_data(filepath: str, **kwargs) -> pd.DataFrame:
    filename = os.path.basename(filepath)
    params = _get_green_taxi_params(filename)
    
    df = _read_csv(filepath, **params['csv_params'], **kwargs)
    return process_taxi_data(df, params=params, company='green')


@timer
def _read_csv(filepath: str, **kwargs) -> pd.DataFrame:
    return pd.read_csv(filepath, **kwargs)


@timer
def _join_location_data_by_id(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Merge information about location to DataFrame using locations' ids."""
    
    ldf = pd.read_csv(lookup_csv_path, index_col='LocationID',
                      usecols=['LocationID', 'Borough', 'Zone'])
    pickup_column_names = {'Borough': 'pickup_borough', 'Zone': 'pickup_zone', 'LocationID': 'pickup_location_id'}
    dropoff_column_names = {'Borough': 'dropoff_borough', 'Zone': 'dropoff_zone', 'LocationID': 'dropoff_location_id'}

    data_frame = data_frame.merge(
        ldf.rename(columns=pickup_column_names),
        how='left',
        left_on='pickup_location_id', right_index=True)
    data_frame = data_frame.merge(
        ldf.rename(columns=dropoff_column_names),
        how='left',
        left_on='dropoff_location_id', right_index=True)
    return data_frame


@timer
def _join_location_data_by_coordinates(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Merge information about location to DataFrame using coordinates."""
    
    import geopandas as gpd

    gdf = gpd.read_file(lookup_shp_path)
    gdf.drop(columns=['OBJECTID', 'Shape_Leng', 'Shape_Area'], inplace=True)
    gdf.to_crs('EPSG:4326', inplace=True)  # reproject to common Coordinate Reference System
    pickup_column_names = {'borough': 'pickup_borough', 'zone': 'pickup_zone', 'LocationID': 'pickup_location_id'}
    dropoff_column_names = {'borough': 'dropoff_borough', 'zone': 'dropoff_zone', 'LocationID': 'dropoff_location_id'}

    temp_pickup_gdf = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(
            x=data_frame['pickup_longitude'],
            y=data_frame['pickup_latitude']),
        crs='EPSG:4326')
    temp_dropoff_gdf = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(
            x=data_frame['dropoff_longitude'],
            y=data_frame['dropoff_latitude']),
        crs='EPSG:4326')
    temp_pickup_gdf = gpd.sjoin(
        left_df=temp_pickup_gdf,
        right_df=gdf,
        how='left', op='within')[['borough', 'zone', 'LocationID']]
    temp_dropoff_gdf = gpd.sjoin(
        left_df=temp_dropoff_gdf,
        right_df=gdf,
        how='left', op='within')[['borough', 'zone', 'LocationID']]
    data_frame = data_frame.merge(
        temp_pickup_gdf.rename(columns=pickup_column_names),
        how='left',
        left_index=True, right_index=True)
    data_frame = data_frame.merge(
        temp_dropoff_gdf.rename(columns=dropoff_column_names),
        how='left',
        left_index=True, right_index=True)
    return data_frame.drop(columns=[name for name in data_frame.columns if 'longitude' in name or 'latitude' in name])
