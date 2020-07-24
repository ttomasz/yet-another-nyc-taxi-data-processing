# New York Taxi data processing scripts
## About
This repo contains some scripts that I used to process New York City's data about taxi trips.  

These are not production ready scripts but they may be useful to somebody looking to process the data themself.  
My main goal was to convert CSV files to Parquet format to save space and for better performance so I could use them with eg. Amazon Athena for analytics but as I found along the way data needed to be cleaned and transformed to better suit that purpose.

Scripts are free to use for any purpose but as mentioned they are not production ready and may contains bugs I take no responsibility for.

Currently I only processed green and yellow taxis' data, no FHV data.

## Sources
I based my work on two repos that were very helpful to get started:
- https://github.com/r-shekhar/NYC-transport
- https://github.com/toddwschneider/nyc-taxi-data

Lookup data is from official NYC site: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page  

Raw data downloaded using "wget" with -i and -P flags using list of files from aforementioned repo: https://github.com/toddwschneider/nyc-taxi-data/blob/master/setup_files/raw_data_urls.txt  

## Structure
Folder _src_ contains Python scripts. Watch out as I was pretty aggressive with removing rows due to bad or missing data (eg. I seriously doubt that someone would ride a taxi for 4 hours to travel 0,5 mile and pay 5$ for that or that taxi could hold 208 passengers.)

Description of files:
- src/helper_objects.py - some helper functions as well as definitions of parameters for various files since the schema changed over time
- src/data_processing.py - main functions for processing data
- src/data_cleaning.py - contains functions that clean/transform data
- src/data_export.py - functions that save DataFrame as Parquet file, end-to-end functions that will take path of the file, process data, and save results as parquet file
- lookup/ - folder with lookup data for taxi zones in New York City, there is the shapefile with geometries and the csv file with mappings (id:name), attaching here for easier setup
- taxi-eda.ipynb - jupyter notebook with leftover pieces of code I used to analyze the data in no particular order, uploaded it to repo should I want to modify something in the process as notebooks make it easier to iterate
- zones.geojson - I converted shapefile from the lookup data to geojson using geopandas to be able to render the data in jupyter lab for testing
- athena_ddl.sql - example of athena script to create table out of the files stored in s3, for compatibility some fields have unoptimal data types (dates as string, ids as float), once the table is created another table can be created with values cast to the right types

## Requirements
Using virtual environment is highly recommended.

For windows you'll probably need to install GDAL, Fiona, rtree, and python-snappy packages manually using wheel files that can be found here: https://www.lfd.uci.edu/~gohlke/pythonlibs/

GDAL needs to be installed before Fiona. Fiona and rtree need to be installed before GeoPandas. After installing these you can continue with installing the rest by executing:
```
pip install requirements.txt
```
On linux/mac python-snappy package should install with regular:
```
pip install python-snappy
```

---

Alternatively you use conda environment and install all packages with this command:
```
conda install -c conda-forge geopandas pyarrow python-snappy
```

## Usage
Files: data_processing.py and data_export.py are the two most important ones use them depending if you want to convert files straight to parquet or want a DataFrame.

Examples:
```python
from data_processing import process_taxi_data_file
from data_export import csv2parquet

# returns DataFrame
df = process_taxi_data_file(r'path/to/file')

# saves directly to specified folder
csv2parquet([r'path1', r'path2', r'etc'], r'output_folder')
```

## Data structure
DataFrame structure:
```
pickup_datetime          datetime64[ns]
dropoff_datetime         datetime64[ns]
store_and_forward                 Int16
passenger_count                   Int16
trip_distance                   float32
fare_amount                     float32
tip_amount                      float32
total_amount                    float32
payment_type                     object
trip_type                        object
pickup_borough                   object
pickup_zone                      object
pickup_location_id                int16
dropoff_borough                  object
dropoff_zone                     object
dropoff_location_id               int16
trip_duration_minutes           float32
year                              int16
year_quarter                     object
year_month                       object
quarter                           int64
month                             int64
date                             object
day_of_week                       int64
hour_of_day                       int64
company                          object
```

Parquet structure:
```
pickup_datetime         timestamp('ns')
dropoff_datetime        timestamp('ns')
store_and_forward                  int8
passenger_count                    int8
trip_distance                   float32
fare_amount                     float32
tip_amount                      float32
total_amount                    float32
payment_type                     string
trip_type                        string
company                          string
trip_duration_minutes           float32
year                              int16
pickup_borough                   string
pickup_zone                      string
pickup_location_id                int16
dropoff_borough                  string
dropoff_zone                     string
dropoff_location_id               int16
year_quarter                     string
year_month                       string
quarter                            int8
month                              int8
date                             date32
day_of_week                        int8
```
