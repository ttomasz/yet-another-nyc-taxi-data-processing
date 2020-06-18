import pandas as pd
import numpy as np
from glob import glob
import os
from sys import stdout
from datetime import datetime

from helper_objects import yellow_taxi_params, green_taxi_params, store_and_fwd_flag_standardize, payment_type_mapping, lower_strip, column_mapping, trip_type_mapping

taxi_data_basepath = './raw_data/taxi/'
taxi_processed_basepath = './processed_data/taxi/'


def get_yellow_taxi_params(filename: str) -> dict:
    for k in yellow_taxi_params.keys():
        if filename <= k:
            return yellow_taxi_params[k]

def get_green_taxi_params(filename: str) -> dict:
    for k in green_taxi_params.keys():
        if filename <= k:
            return green_taxi_params[k]


def process_taxi_data(df: pd.DataFrame, params, company, **kwargs):
    df.rename(columns=column_mapping, inplace=True)
    
    if params['location'] == 'coordinates':
        # remove rows where any of the coordinates is 0
        df = df[(df['pickup_longitude'] != 0) & (df['pickup_latitude'] != 0) & (df['dropoff_longitude'] != 0) & (df['dropoff_latitude'] != 0)]
    
    # remove taxi rides ignoring laws of physics (going back in time)
    df = df[df['pickup_datetime'] < df['dropoff_datetime']]
    
    # money or distance or passengers shouldn't be less than zero (nulls are ok)
    df = df[df['trip_distance'].fillna(0) >= 0]
    df = df[df['total_amount'].fillna(0) >= 0]
    df = df[df['passenger_count'].fillna(0) >= 0]
    
    # we could compare reported distance with pickup and dropoff locations but that's kinda too much work
    
    df['store_and_forward'] = df['store_and_forward'].apply(store_and_fwd_flag_standardize).astype(np.float16)
    df['payment_type'] = df['payment_type'].apply(payment_type_mapping)
    # change tip amount to null for cash transactions (better to have nulls for unknown values than zeroes)
    df['tip_amount'].mask(df['payment_type'] == 'cash', np.nan, inplace=True)
    # assign company name
    df['company'] = company
    # add trip duration
    df['trip_duration_minutes'] = df['dropoff_datetime'] - df['pickup_datetime']
    df['trip_duration_minutes'] = df['trip_duration_minutes'].dt.seconds / 60
    df['trip_duration_minutes'] = df['trip_duration_minutes'].astype(np.float32)
    # add date related fields for easier querying
    df['year'] = pd.DatetimeIndex(df['pickup_datetime']).year
    df['year'] = df['year'].astype(np.int16)
    # remove records with dates that were wrong or parsed improperly
    df = df[(df['year'] >= 2009) & (df['year'] <= 2029)]
#     df['year_quarter'] = pd.DatetimeIndex(df['pickup_datetime']).year.astype(str) + 'Q' + pd.DatetimeIndex(df['pickup_datetime']).quarter.astype(str)
#     df['year_month'] = pd.DatetimeIndex(df['pickup_datetime']).strftime('%Y-%m')
#     df['quarter'] = df['year'] = pd.DatetimeIndex(df['pickup_datetime']).quarter
#     df['month'] = df['year'] = pd.DatetimeIndex(df['pickup_datetime']).month
#     df['date'] = df['year'] = pd.DatetimeIndex(df['pickup_datetime']).date
#     df['day_of_week'] = pd.DatetimeIndex(df['pickup_datetime']).weekday + 1

    if 'trip_type' not in df.columns:
        df['trip_type'] = np.nan
    else:
        df['trip_type'] = df['trip_type'].apply(trip_type_mapping)


    if params['location'] == 'id':
        ldf = pd.read_csv('./lookup/taxi+_zone_lookup.csv', index_col='LocationID', usecols=['LocationID', 'Borough', 'Zone'])
        pickup_column_names = {'Borough': 'pickup_borough', 'Zone': 'pickup_zone', 'LocationID': 'pickup_location_id'}
        dropoff_column_names = {'Borough': 'dropoff_borough', 'Zone': 'dropoff_zone', 'LocationID': 'dropoff_location_id'}
        
        df = df.merge(
            ldf.rename(columns=pickup_column_names), 
            how='left', 
            left_on='pickup_location_id', right_index=True)
        df = df.merge(
            ldf.rename(columns=dropoff_column_names), 
            how='left', 
            left_on='dropoff_location_id', right_index=True)
        df['pickup_location_id'] = df['pickup_location_id'].astype(np.float32)
        df['dropoff_location_id'] = df['dropoff_location_id'].astype(np.float32)

    elif params['location'] == 'coordinates':
        
        import geopandas as gpd
        
        gdf = gpd.read_file('./lookup/taxi_zones.shp')
        gdf.drop(columns=['OBJECTID', 'Shape_Leng', 'Shape_Area'], inplace=True)
        gdf.to_crs('EPSG:4326', inplace=True)  # reproject to common Coordinate Reference System
        pickup_column_names = {'borough': 'pickup_borough', 'zone': 'pickup_zone', 'LocationID': 'pickup_location_id'}
        dropoff_column_names = {'borough': 'dropoff_borough', 'zone': 'dropoff_zone', 'LocationID': 'dropoff_location_id'}
        
        temp_pickup_gdf = gpd.GeoDataFrame(
            geometry=gpd.points_from_xy(
                x=df['pickup_longitude'], 
                y=df['pickup_latitude']),
            crs='EPSG:4326')
        temp_dropoff_gdf = gpd.GeoDataFrame(
            geometry=gpd.points_from_xy(
                x=df['dropoff_longitude'], 
                y=df['dropoff_latitude']),
            crs='EPSG:4326')
        temp_pickup_gdf = gpd.sjoin(
            left_df=temp_pickup_gdf,
            right_df=gdf, 
            how='left')[['borough', 'zone', 'LocationID']]
        temp_dropoff_gdf = gpd.sjoin(
            left_df=temp_dropoff_gdf,
            right_df=gdf, 
            how='left')[['borough', 'zone', 'LocationID']]
        df = df.merge(
            temp_pickup_gdf.rename(columns=pickup_column_names), 
            how='left', 
            left_index=True, right_index=True)
        df = df.merge(
            temp_dropoff_gdf.rename(columns=dropoff_column_names), 
            how='left', 
            left_index=True, right_index=True)
        df.drop(columns=[name for name in df.columns if 'longitude' in name or 'latitude' in name], inplace=True)
        df['pickup_location_id'] = df['pickup_location_id'].astype(np.float32)
        df['dropoff_location_id'] = df['dropoff_location_id'].astype(np.float32)
    
    # change timestamps to string for better compatibility
    # eg. Amazon Athena wasn't able to read timestamps from parquet files created by pandas and fastparquet
    df['pickup_datetime'] = pd.DatetimeIndex(df['pickup_datetime']).strftime('%Y-%m-%d %H:%M:%S')
    df['dropoff_datetime'] = pd.DatetimeIndex(df['dropoff_datetime']).strftime('%Y-%m-%d %H:%M:%S')
    
    return df


def process_yellow_taxi_data(filepath: str, **kwargs):
    filename = os.path.basename(filepath)
    params = get_yellow_taxi_params(filename)
    
    df = pd.read_csv(filepath, **params['csv_params'], **kwargs)
    return process_taxi_data(df, params=params, company='yellow', **kwargs)


def process_green_taxi_data(filepath: str, **kwargs):
    filename = os.path.basename(filepath)
    params = get_green_taxi_params(filename)
    
    df = pd.read_csv(filepath, **params['csv_params'], **kwargs)
    return process_taxi_data(df, params=params, company='green', **kwargs)


def yellow_taxi_paths(folder: str) -> list:
    return glob(os.path.join(folder, 'yellow_tripdata*'))

def green_taxi_paths(folder: str) -> list:
    return glob(os.path.join(folder, 'green_tripdata*'))


def csv2parquet(paths: list) -> None:
    of = len(paths)

    for i, path in enumerate(paths):
        source_file_name = os.path.basename(path)
        result_file_name = source_file_name.split('.')[0] + '.parquet'
        result_file_path = os.path.join(taxi_processed_basepath, result_file_name)
        stdout.write(f"{str(i+1).zfill(2)}/{of} - {datetime.now().isoformat(timespec='seconds')} - processing: {source_file_name}\n")
        if 'yellow' in source_file_name:
            df = process_yellow_taxi_data(path)
        elif 'green' in source_file_name:
            df = process_green_taxi_data(path)
        else:
            raise ValueError(f'Couldn\'t determine how to parse given path: {path}')
        stdout.write(f"{str(i+1).zfill(2)}/{of} - {datetime.now().isoformat(timespec='seconds')} - writing DataFrame as parquet file: {result_file_name}\n")
        df.to_parquet(result_file_path, compression='snappy', engine='fastparquet')

    stdout.write(f"{datetime.now().isoformat(timespec='seconds')} - finished processing files.\n")

def csv2parquet_green_taxi():    
    csv2parquet(green_taxi_paths(taxi_data_basepath))
    
def csv2parquet_yellow_taxi():
    csv2parquet(yellow_taxi_paths(taxi_data_basepath))
