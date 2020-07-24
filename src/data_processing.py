import datetime
import os
from sys import stdout

import pandas as pd

from data_cleaning import rename_columns, drop_invalid_coordinates, drop_invalid_timestamps, \
    drop_negative_values, standardize_snf_flag_values, standardize_payment_type_values, \
    replace_tip_values_for_cash_payments, drop_invalid_trip_durations, drop_invalid_year_values, \
    drop_missing_location_ids, add_trip_duration, add_year, add_additional_date_features, \
    standardize_trip_type_values, drop_invalid_passenger_count_values
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


def process_taxi_data(df: pd.DataFrame, params: ParameterType, company: str) -> pd.DataFrame:
    """Applies cleaning rules and feature engineering on the provided DataFrame."""

    initial_number_of_rows = len(df.index)
    start_time = time.perf_counter()

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

    # info about processed DataFrame for sanity check
    end_time = time.perf_counter()
    run_time = datetime.timedelta(seconds=(end_time - start_time))
    stdout.write(f'\t___\n\tProcessing DataFrame took {run_time}.\n')
    stdout.flush()
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
    filename = os.path.basename(filepath).split('.')[0]
    params = _get_yellow_taxi_params(filename)
    
    df = _read_csv(filepath, **params['csv_params'], **kwargs)
    return process_taxi_data(df, params=params, company='yellow')


def _process_green_taxi_data(filepath: str, **kwargs) -> pd.DataFrame:
    filename = os.path.basename(filepath).split('.')[0]
    params = _get_green_taxi_params(filename)
    
    df = _read_csv(filepath, **params['csv_params'], **kwargs)
    return process_taxi_data(df, params=params, company='green')


@timer
def _read_csv(filepath: str, **kwargs) -> pd.DataFrame:
    return pd.read_csv(filepath, **kwargs)


def join_location_data(data_frame: pd.DataFrame, join_by: str, drop_missing: bool = True) -> pd.DataFrame:
    data_frame.reset_index(drop=True, inplace=True)
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
        how='left')[['borough', 'zone', 'LocationID']]
    temp_dropoff_gdf = gpd.sjoin(
        left_df=temp_dropoff_gdf,
        right_df=gdf,
        how='left')[['borough', 'zone', 'LocationID']]
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
    # for testing
    import time
    sts = time.perf_counter()
    _df = process_taxi_data_file('F:/green_tripdata_2013-08.csv.zip')
    ets = time.perf_counter()
    print('processing took:', ets-sts)
    print(_df.head())
    for col in _df.columns:
        print(_df[col].head())
    print('##################')
    print(_df.dtypes)

