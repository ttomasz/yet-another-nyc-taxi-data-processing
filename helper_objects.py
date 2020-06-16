import numpy as np

column_name_mapping = {
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

yellow_taxi_params = {
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
                'Passenger_Count': 'uint8', 
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
                'passenger_count': 'uint8', 
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
                'passenger_count': 'uint8', 
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
                'passenger_count': 'uint8', 
                'trip_distance': 'float32', 
                'fare_amount': 'float32', 
                'tip_amount': 'float32', 
                'total_amount': 'float32'
            },
            'header': 0, 
            'names': 'VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,junk1,junk2'.split(',')
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
                'passenger_count': 'uint8', 
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
                'passenger_count': 'uint8', 
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
                'passenger_count': 'uint8', 
                'trip_distance': 'float32', 
                'fare_amount': 'float32', 
                'tip_amount': 'float32', 
                'total_amount': 'float32'
            }
        },
        'location': 'id'
    }
}

green_taxi_params = {
    'green_tripdata_2014-12.csv':{
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
                'Passenger_count': 'float16'
            },
            'header': 0,
            'names': 'VendorID,lpep_pickup_datetime,Lpep_dropoff_datetime,Store_and_fwd_flag,RateCodeID,Pickup_longitude,Pickup_latitude,Dropoff_longitude,Dropoff_latitude,Passenger_count,Trip_distance,Fare_amount,Extra,MTA_tax,Tip_amount,Tolls_amount,Ehail_fee,Total_amount,Payment_type,Trip_type,junk1,junk2'.split(',')
        },
        'location': 'coordinates'
    },
    'green_tripdata_2015-06.csv':{
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
                'Passenger_count': 'float16'
            },
            'header': 0,
            'names': 'VendorID,lpep_pickup_datetime,Lpep_dropoff_datetime,Store_and_fwd_flag,RateCodeID,Pickup_longitude,Pickup_latitude,Dropoff_longitude,Dropoff_latitude,Passenger_count,Trip_distance,Fare_amount,Extra,MTA_tax,Tip_amount,Tolls_amount,Ehail_fee,improvement_surcharge,Total_amount,Payment_type,Trip_type,junk1,junk2'.split(',')
        },
        'location': 'coordinates'
    },
    'green_tripdata_2016-06.csv':{
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
                'Passenger_count': 'float16'
            }
        },
        'location': 'coordinates'
    },
    'green_tripdata_2016-12.csv':{
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
                'passenger_count': 'float16'
            },
            'header': 0,
            'names': 'VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,junk1,junk2'.split(',')
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
                'passenger_count': 'float16'
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
                'passenger_count': 'float16'
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
                'passenger_count': 'float16'
            }
        },
        'location': 'id'
    }
}

def store_and_fwd_flag_standardize(x) -> int:
    if x is None:
        return None
    elif str(x).upper() in {'1', 'Y', 'T'}:
        return 1
    elif str(x).upper() in {'0', 'N', 'F'}:
        return 0
    else:
        return np.nan


def payment_type_mapping(id) -> str:
    x = str(id).lower()
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
        return np.nan
    
def lower_strip(s: str) -> str:
    return s.strip().lower()

def column_mapping(col: str) -> str:
    from helper_objects import column_name_mapping
    col = lower_strip(col)
    return column_name_mapping.get(col, col)

def trip_type_mapping(id: int) -> str:
    if id == 1:
        return 'Street-hail'
    elif id == 2:
        return 'Dispatch'
    else:
        return np.nan