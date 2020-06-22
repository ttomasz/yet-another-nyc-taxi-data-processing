CREATE EXTERNAL TABLE nyc_taxi (
	pickup_datetime               string,
	dropoff_datetime              string,
	store_and_forward              float,
	pickup_location_id             float,
	dropoff_location_id            float,
	passenger_count                float,
	trip_distance                  float,
	fare_amount                    float,
	tip_amount                     float,
	total_amount                   float,
	payment_type                  string,
	trip_type                     string,
	company                       string,
	trip_duration_minutes          float,
	year                             int,
	pickup_borough                string,
	pickup_zone                   string,
	dropoff_borough               string,
	dropoff_zone                  string
)
STORED AS PARQUET
LOCATION 's3://your-location/'
tblproperties ("parquet.compression"="SNAPPY");

MSCK REPAIR TABLE nyc_taxi;
