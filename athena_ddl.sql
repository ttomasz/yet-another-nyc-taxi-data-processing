CREATE EXTERNAL TABLE nyc_taxi (
    pickup_datetime             TIMESTAMP,
    dropoff_datetime            TIMESTAMP,
    store_and_forward             TINYINT,
    passenger_count               TINYINT,
    trip_distance                   FLOAT,
    fare_amount                     FLOAT,
    tip_amount                      FLOAT,
    total_amount                    FLOAT,
    payment_type                  VARCHAR,
    trip_type                     VARCHAR,
    company                       VARCHAR,
    trip_duration_minutes           FLOAT,
    year                         SMALLINT,
    pickup_borough                VARCHAR,
    pickup_zone                   VARCHAR,
    pickup_location_id           SMALLINT,
    dropoff_borough               VARCHAR,
    dropoff_zone                  VARCHAR,
    dropoff_location_id          SMALLINT,
    year_quarter                  VARCHAR,
    year_month                    VARCHAR,
    quarter                       TINYINT,
    month                         TINYINT,
    date                             DATE,
    day_of_week                   TINYINT
)
STORED AS PARQUET
LOCATION 's3://your-location/'
tblproperties ("parquet.compression"="SNAPPY");

MSCK REPAIR TABLE nyc_taxi;
