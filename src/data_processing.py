import datetime
import logging
import time
import os
from sys import stdout
from typing import Iterable, Tuple

import pandas as pd

from data_cleaning import rename_columns, drop_invalid_coordinates, drop_invalid_timestamps, \
    drop_negative_values, standardize_snf_flag_values, standardize_payment_type_values, \
    replace_tip_values_for_cash_payments, drop_invalid_trip_durations, drop_invalid_year_values, \
    drop_missing_location_ids, add_trip_duration, add_year, add_additional_date_features, \
    standardize_trip_type_values, drop_invalid_passenger_count_values, sort_df
from helper_objects import yellow_taxi_params, ParameterType, green_taxi_params, lookup_csv_path, \
    lookup_shp_path, timer, print_sanity_stats


def _get_yellow_taxi_params(filename: str) -> ParameterType:
    for k in yellow_taxi_params.keys():
        if filename <= k:
            return yellow_taxi_params[k]


def _get_green_taxi_params(filename: str) -> ParameterType:
    for k in green_taxi_params.keys():
        if filename <= k:
            return green_taxi_params[k]


def get_taxi_params(filename: str) -> Tuple[str, ParameterType]:
    """Returns tuple with company name (yellow, green) and parameters needed for parsing CSV file."""

    if 'yellow' in filename:
        company_name = 'yellow'
        params = _get_yellow_taxi_params(filename)
    elif 'green' in filename:
        company_name = 'green'
        params = _get_green_taxi_params(filename)
    else:
        raise ValueError(f'Couldn\'t determine how to parse given filename: {filename}')

    return company_name, params


def process_taxi_data(df: pd.DataFrame, params: ParameterType, company: str) -> pd.DataFrame:
    """Applies cleaning rules and feature engineering on the provided DataFrame."""

    df = rename_columns(df)
    df = join_location_data(df, params['location'])
    df = drop_invalid_timestamps(df)
    df = drop_negative_values(df)
    df = drop_invalid_passenger_count_values(df)
    df = standardize_snf_flag_values(df)
    df = standardize_payment_type_values(df)
    df = replace_tip_values_for_cash_payments(df)
    df = add_trip_duration(df)
    df = drop_invalid_trip_durations(df)
    df = add_year(df)
    df = drop_invalid_year_values(df)
    df = add_additional_date_features(df)
    df = standardize_trip_type_values(df)

    # assign company name
    df['company'] = company

    # sorting so later on when we save to parquet we get better compression
    df = sort_df(df)

    return df


@timer(logging.INFO)
def process_taxi_data_file(filepath: str, chunksize: int = 1000000, **kwargs) -> pd.DataFrame:
    """Reads file and applies cleaning rules and feature engineering."""

    initial_number_of_rows = 0
    start_time = time.perf_counter()
    filename = os.path.basename(filepath).split('.')[0]
    company_name, params = get_taxi_params(filename)

    data_frames = []
    for idx, chunk in enumerate(_csv_chunks(filepath, chunksize, **params['csv_params'], **kwargs)):
        stdout.write(f'File: {filename!r} - processing chunk: {idx + 1}\n')
        initial_number_of_rows += len(chunk.index)
        data_frames.append(process_taxi_data(chunk, params=params, company=company_name))

    df = pd.concat(data_frames, ignore_index=True)

    end_time = time.perf_counter()
    run_time = datetime.timedelta(seconds=(end_time - start_time))
    stdout.write(f'___\nProcessing DataFrame from file {filename!r} took {run_time}.\n')
    stdout.flush()

    # info about processed DataFrame for sanity check
    final_number_of_rows = len(df.index)
    print_sanity_stats(initial_number_of_rows, final_number_of_rows)

    return df


@timer(logging.DEBUG)
def _read_csv(filepath: str, **kwargs) -> pd.DataFrame:
    return pd.read_csv(filepath, **kwargs)


def _csv_chunks(filepath: str, chunksize: int = 1000000, **kwargs) -> Iterable[pd.DataFrame]:
    return pd.read_csv(filepath, chunksize=chunksize, **kwargs)


def join_location_data(data_frame: pd.DataFrame, join_by: str, drop_missing: bool = True) -> pd.DataFrame:
    data_frame = data_frame.reset_index(drop=True)
    if join_by == 'id':
        data_frame = _join_location_data_by_id(data_frame)
    elif join_by == 'coordinates':
        data_frame = drop_invalid_coordinates(data_frame)
        data_frame = _join_location_data_by_coordinates(data_frame)
    if drop_missing:
        data_frame = drop_missing_location_ids(data_frame)
        new_type = 'int16'
    else:
        new_type = 'Int16'
    data_frame['pickup_location_id'] = data_frame['pickup_location_id'].astype(new_type)
    data_frame['dropoff_location_id'] = data_frame['dropoff_location_id'].astype(new_type)
    return data_frame


@timer(logging.DEBUG)
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


@timer(logging.DEBUG)
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


if __name__ == '__main__':
    # logging.getLogger().setLevel(logging.INFO)

    # for testing
    _df = process_taxi_data_file('F:\\green_tripdata_2013-12.csv.zip')
    # print(_df.head())
    # for col in _df.columns:
    #     print(_df[col].head())
    # print('##################')
    # print(_df.dtypes)
