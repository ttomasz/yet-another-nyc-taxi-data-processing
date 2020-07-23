import os
import time
from glob import glob
from sys import stdout, stderr
from typing import Dict, Union, List
import functools

import pyarrow as pa

this_file_dir = os.path.dirname(os.path.abspath(__file__))
lookup_csv_path = os.path.join(this_file_dir, '../lookup/taxi+_zone_lookup.csv')
lookup_shp_path = os.path.join(this_file_dir, '../lookup/taxi_zones.shp')

column_name_mapping_dict: Dict[str, str] = {
    'congestion_surcharge': 'congestion_surcharge',
    'dolocationid': 'dropoff_location_id',
    'dropoff_datetime': 'dropoff_datetime',
    'dropoff_latitude': 'dropoff_latitude',
    'dropoff_longitude': 'dropoff_longitude',
    'end_lat': 'dropoff_latitude',
    'end_lon': 'dropoff_longitude',
    'extra': 'extra',
    'fare_amount': 'fare_amount',
    'fare_amt': 'fare_amount',
    'improvement_surcharge': 'improvement_surcharge',
    'mta_tax': 'mta_tax',
    'passenger_count': 'passenger_count',
    'payment_type': 'payment_type',
    'pickup_datetime': 'pickup_datetime',
    'pickup_latitude': 'pickup_latitude',
    'pickup_longitude': 'pickup_longitude',
    'pulocationid': 'pickup_location_id',
    'rate_code': 'rate_code',
    'ratecodeid': 'rate_code',
    'start_lat': 'pickup_latitude',
    'start_lon': 'pickup_longitude',
    'store_and_forward': 'store_and_forward',
    'store_and_fwd_flag': 'store_and_forward',
    'surcharge': 'surcharge',
    'tip_amount': 'tip_amount',
    'tip_amt': 'tip_amount',
    'tolls_amount': 'tolls_amount',
    'tolls_amt': 'tolls_amount',
    'total_amount': 'total_amount',
    'total_amt': 'total_amount',
    'tpep_dropoff_datetime': 'dropoff_datetime',
    'tpep_pickup_datetime': 'pickup_datetime',
    'trip_distance': 'trip_distance',
    'trip_dropoff_datetime': 'dropoff_datetime',
    'trip_pickup_datetime': 'pickup_datetime',
    'vendor_id': 'vendor',
    'vendor_name': 'vendor',
    'vendorid': 'vendor',
    'trip_type': 'trip_type',
    'lpep_dropoff_datetime': 'dropoff_datetime',
    'lpep_pickup_datetime': 'pickup_datetime'
}

arrow_schema = pa.schema([
    ('pickup_datetime', pa.timestamp('ns')),
    ('dropoff_datetime', pa.timestamp('ns')),
    ('store_and_forward', pa.int8()),
    ('passenger_count', pa.int8()),
    ('trip_distance', pa.float32()),
    ('fare_amount', pa.float32()),
    ('tip_amount', pa.float32()),
    ('total_amount', pa.float32()),
    ('payment_type', pa.string()),
    ('trip_type', pa.string()),
    ('company', pa.string()),
    ('trip_duration_minutes', pa.float32()),
    ('year', pa.int16()),
    ('pickup_borough', pa.string()),
    ('pickup_zone', pa.string()),
    ('pickup_location_id', pa.int16()),
    ('dropoff_borough', pa.string()),
    ('dropoff_zone', pa.string()),
    ('dropoff_location_id', pa.int16()),
])

ParameterType = Dict[str, Union[str, Dict[str, Union[bool, Dict[str, str], List[str]]]]]
ParametersDictType = Dict[str, ParameterType]
yellow_taxi_params: ParametersDictType = {
    'yellow_tripdata_2009-12.csv': {
        'csv_params': {
            'parse_dates': ['Trip_Pickup_DateTime', 'Trip_Dropoff_DateTime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'Trip_Pickup_DateTime',
                'Trip_Dropoff_DateTime',
                'Passenger_Count',
                'Trip_Distance',
                'Start_Lon',
                'Start_Lat',
                'store_and_forward',
                'End_Lon',
                'End_Lat',
                'Payment_Type',
                'Fare_Amt',
                'Tip_Amt',
                'Total_Amt',
            ],
            'dtype': {
                'store_and_forward': 'object',
                'Passenger_Count': 'Int16',
                'Trip_Distance': 'float32',
                'Fare_Amt': 'float32',
                'Tip_Amt': 'float32',
                'Total_Amt': 'float32'
            }
        },
        'location': 'coordinates'
    },
    'yellow_tripdata_2014-12.csv': {
        'csv_params': {
            'parse_dates': ['pickup_datetime', 'dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'pickup_datetime',
                'dropoff_datetime',
                'passenger_count',
                'trip_distance',
                'pickup_longitude',
                'pickup_latitude',
                'store_and_fwd_flag',
                'dropoff_longitude',
                'dropoff_latitude',
                'payment_type',
                'fare_amount',
                'tip_amount',
                'total_amount',
            ],
            'dtype': {
                'store_and_fwd_flag': 'object',
                'passenger_count': 'Int16',
                'trip_distance': 'float32',
                'fare_amount': 'float32',
                'tip_amount': 'float32',
                'total_amount': 'float32'
            }
        },
        'location': 'coordinates'
    },
    'yellow_tripdata_2016-06.csv': {
        'csv_params': {
            'parse_dates': ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'tpep_pickup_datetime',
                'tpep_dropoff_datetime',
                'passenger_count',
                'trip_distance',
                'pickup_longitude',
                'pickup_latitude',
                'store_and_fwd_flag',
                'dropoff_longitude',
                'dropoff_latitude',
                'payment_type',
                'fare_amount',
                'tip_amount',
                'total_amount',
            ],
            'dtype': {
                'store_and_fwd_flag': 'object',
                'passenger_count': 'Int16',
                'trip_distance': 'float32',
                'fare_amount': 'float32',
                'tip_amount': 'float32',
                'total_amount': 'float32'
            }
        },
        'location': 'coordinates'
    },
    'yellow_tripdata_2016-12.csv': {
        'csv_params': {
            'parse_dates': ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'tpep_pickup_datetime',
                'tpep_dropoff_datetime',
                'passenger_count',
                'trip_distance',
                'store_and_fwd_flag',
                'PULocationID',
                'DOLocationID',
                'payment_type',
                'fare_amount',
                'tip_amount',
                'total_amount',
            ],
            'dtype': {
                'store_and_fwd_flag': 'object',
                'passenger_count': 'Int16',
                'trip_distance': 'float32',
                'fare_amount': 'float32',
                'tip_amount': 'float32',
                'total_amount': 'float32'
            },
            'header': 0,
            'names': 'VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2'.split(
                ',')
        },
        'location': 'id'
    },
    'yellow_tripdata_2018-12.csv': {
        'csv_params': {
            'parse_dates': ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'tpep_pickup_datetime',
                'tpep_dropoff_datetime',
                'passenger_count',
                'trip_distance',
                'store_and_fwd_flag',
                'PULocationID',
                'DOLocationID',
                'payment_type',
                'fare_amount',
                'tip_amount',
                'total_amount',
            ],
            'dtype': {
                'store_and_fwd_flag': 'object',
                'passenger_count': 'Int16',
                'trip_distance': 'float32',
                'fare_amount': 'float32',
                'tip_amount': 'float32',
                'total_amount': 'float32'
            }
        },
        'location': 'id'
    },
    'yellow_tripdata_2019-12.csv': {
        'csv_params': {
            'parse_dates': ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'tpep_pickup_datetime',
                'tpep_dropoff_datetime',
                'passenger_count',
                'trip_distance',
                'store_and_fwd_flag',
                'PULocationID',
                'DOLocationID',
                'payment_type',
                'fare_amount',
                'tip_amount',
                'total_amount',
            ],
            'dtype': {
                'store_and_fwd_flag': 'object',
                'passenger_count': 'Int16',
                'trip_distance': 'float32',
                'fare_amount': 'float32',
                'tip_amount': 'float32',
                'total_amount': 'float32'
            }
        },
        'location': 'id'
    },
    'zzz_generic_schema': {
        'csv_params': {
            'parse_dates': ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'tpep_pickup_datetime',
                'tpep_dropoff_datetime',
                'passenger_count',
                'trip_distance',
                'store_and_fwd_flag',
                'PULocationID',
                'DOLocationID',
                'payment_type',
                'fare_amount',
                'tip_amount',
                'total_amount',
            ],
            'dtype': {
                'store_and_fwd_flag': 'object',
                'passenger_count': 'Int16',
                'trip_distance': 'float32',
                'fare_amount': 'float32',
                'tip_amount': 'float32',
                'total_amount': 'float32'
            }
        },
        'location': 'id'
    }
}

green_taxi_params: ParametersDictType = {
    'green_tripdata_2014-12.csv': {
        'csv_params': {
            'parse_dates': ['lpep_pickup_datetime', 'Lpep_dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'lpep_pickup_datetime',
                'Lpep_dropoff_datetime',
                'Passenger_count',
                'Trip_distance',
                'Store_and_fwd_flag',
                'Pickup_longitude',
                'Pickup_latitude',
                'Dropoff_longitude',
                'Dropoff_latitude',
                'Payment_type',
                'Fare_amount',
                'Tip_amount',
                'Total_amount',
                'Trip_type',
            ],
            'dtype': {
                'Store_and_fwd_flag': 'object',
                'Trip_distance': 'float32',
                'Fare_amount': 'float32',
                'Tip_amount': 'float32',
                'Total_amount': 'float32',
                'Passenger_count': 'Int16'
            },
            'header': 0,
            'names': 'VendorID,lpep_pickup_datetime,Lpep_dropoff_datetime,Store_and_fwd_flag,RateCodeID,Pickup_longitude,Pickup_latitude,Dropoff_longitude,Dropoff_latitude,Passenger_count,Trip_distance,Fare_amount,Extra,MTA_tax,Tip_amount,Tolls_amount,Ehail_fee,Total_amount,Payment_type,Trip_type,junk1,junk2'.split(
                ',')
        },
        'location': 'coordinates'
    },
    'green_tripdata_2015-06.csv': {
        'csv_params': {
            'parse_dates': ['lpep_pickup_datetime', 'Lpep_dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'lpep_pickup_datetime',
                'Lpep_dropoff_datetime',
                'Passenger_count',
                'Trip_distance',
                'Store_and_fwd_flag',
                'Pickup_longitude',
                'Pickup_latitude',
                'Dropoff_longitude',
                'Dropoff_latitude',
                'Payment_type',
                'Fare_amount',
                'Tip_amount',
                'Total_amount',
                'Trip_type',
            ],
            'dtype': {
                'Store_and_fwd_flag': 'object',
                'Trip_distance': 'float32',
                'Fare_amount': 'float32',
                'Tip_amount': 'float32',
                'Total_amount': 'float32',
                'Passenger_count': 'Int16'
            },
            'header': 0,
            'names': 'VendorID,lpep_pickup_datetime,Lpep_dropoff_datetime,Store_and_fwd_flag,RateCodeID,Pickup_longitude,Pickup_latitude,Dropoff_longitude,Dropoff_latitude,Passenger_count,Trip_distance,Fare_amount,Extra,MTA_tax,Tip_amount,Tolls_amount,Ehail_fee,improvement_surcharge,Total_amount,Payment_type,Trip_type,junk1,junk2'.split(
                ',')
        },
        'location': 'coordinates'
    },
    'green_tripdata_2016-06.csv': {
        'csv_params': {
            'parse_dates': ['lpep_pickup_datetime', 'Lpep_dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'lpep_pickup_datetime',
                'Lpep_dropoff_datetime',
                'Passenger_count',
                'Trip_distance',
                'Store_and_fwd_flag',
                'Pickup_longitude',
                'Pickup_latitude',
                'Dropoff_longitude',
                'Dropoff_latitude',
                'Payment_type',
                'Fare_amount',
                'Tip_amount',
                'Total_amount',
                'Trip_type ',
            ],
            'dtype': {
                'Store_and_fwd_flag': 'object',
                'Trip_distance': 'float32',
                'Fare_amount': 'float32',
                'Tip_amount': 'float32',
                'Total_amount': 'float32',
                'Passenger_count': 'Int16'
            }
        },
        'location': 'coordinates'
    },
    'green_tripdata_2016-12.csv': {
        'csv_params': {
            'parse_dates': ['lpep_pickup_datetime', 'lpep_dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'lpep_pickup_datetime',
                'lpep_dropoff_datetime',
                'passenger_count',
                'trip_distance',
                'store_and_fwd_flag',
                'PULocationID',
                'DOLocationID',
                'payment_type',
                'fare_amount',
                'tip_amount',
                'total_amount',
                'trip_type',
            ],
            'dtype': {
                'store_and_fwd_flag': 'object',
                'trip_distance': 'float32',
                'fare_amount': 'float32',
                'tip_amount': 'float32',
                'total_amount': 'float32',
                'passenger_count': 'Int16'
            },
            'header': 0,
            'names': 'VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,junk1,junk2'.split(
                ',')
        },
        'location': 'id'
    },
    'green_tripdata_2018-12.csv': {
        'csv_params': {
            'parse_dates': ['lpep_pickup_datetime', 'lpep_dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'lpep_pickup_datetime',
                'lpep_dropoff_datetime',
                'passenger_count',
                'trip_distance',
                'store_and_fwd_flag',
                'PULocationID',
                'DOLocationID',
                'payment_type',
                'fare_amount',
                'tip_amount',
                'total_amount',
                'trip_type',
            ],
            'dtype': {
                'store_and_fwd_flag': 'object',
                'trip_distance': 'float32',
                'fare_amount': 'float32',
                'tip_amount': 'float32',
                'total_amount': 'float32',
                'passenger_count': 'Int16'
            }
        },
        'location': 'id'
    },
    'green_tripdata_2019-12.csv': {
        'csv_params': {
            'parse_dates': ['lpep_pickup_datetime', 'lpep_dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'lpep_pickup_datetime',
                'lpep_dropoff_datetime',
                'passenger_count',
                'trip_distance',
                'store_and_fwd_flag',
                'PULocationID',
                'DOLocationID',
                'payment_type',
                'fare_amount',
                'tip_amount',
                'total_amount',
                'trip_type',
            ],
            'dtype': {
                'store_and_fwd_flag': 'object',
                'trip_distance': 'float32',
                'fare_amount': 'float32',
                'tip_amount': 'float32',
                'total_amount': 'float32',
                'passenger_count': 'Int16'
            }
        },
        'location': 'id'
    },
    'zzz_generic_schema': {
        'csv_params': {
            'parse_dates': ['lpep_pickup_datetime', 'lpep_dropoff_datetime'],
            'infer_datetime_format': True,
            'skipinitialspace': True,
            'usecols': [
                'lpep_pickup_datetime',
                'lpep_dropoff_datetime',
                'passenger_count',
                'trip_distance',
                'store_and_fwd_flag',
                'PULocationID',
                'DOLocationID',
                'payment_type',
                'fare_amount',
                'tip_amount',
                'total_amount',
                'trip_type',
            ],
            'dtype': {
                'store_and_fwd_flag': 'object',
                'trip_distance': 'float32',
                'fare_amount': 'float32',
                'tip_amount': 'float32',
                'total_amount': 'float32',
                'passenger_count': 'Int16'
            }
        },
        'location': 'id'
    }
}


def yellow_taxi_paths(folder: str) -> List[str]:
    return glob(os.path.join(folder, 'yellow_tripdata*'))


def green_taxi_paths(folder: str) -> List[str]:
    return glob(os.path.join(folder, 'green_tripdata*'))


def print_sanity_stats(initial_number_of_rows: int, final_number_of_rows: int) -> None:
    stdout.write(f'\tInitial number of rows in DataFrame: {initial_number_of_rows:_d}.\n')
    stdout.write(f'\tFinal number of rows in DataFrame: {final_number_of_rows:_d}.\n')
    dropped_rows = initial_number_of_rows - final_number_of_rows
    percent_dropped = (100.0 * dropped_rows) / initial_number_of_rows
    stdout.write(f'\tDropped rows from DataFrame: {dropped_rows:_d}. Percent: {percent_dropped:.1f}%.\n')

    warning_threshold = 5.0
    if percent_dropped > warning_threshold:
        stderr.write(f'##############\n')
        stderr.write(f'WARNING!\n')
        stderr.write(f'Percentage of dropped rows above {warning_threshold}% threshold!\n')
        stderr.write(f'##############\n')


def timer(func):
    """Print the runtime of the decorated function"""
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        stdout.write(f'\tFinished {func.__name__!r} in {run_time:.4f} secs\n')
        return value
    return wrapper_timer
