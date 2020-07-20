# New York Taxi data processing scripts
## About
This repo contains some scripts that I used to process New York City's data about taxi trips.  

These are not production ready scripts but they may be useful to somebody looking to process the data themself.  
My main goal was to convert CSV files to Parquet format to save space and for better performance so I could use them with eg. Amazon Athena for analytics but as I found along the way data needed to be cleaned and transformed to better suit that purpose.

Scripts are free to use for any purpose but as mentioned they are not production ready and may contains bugs I take no reponsibility for.

Currently I only processed green and yellow taxis' data, no FHV data.

## Sources
I based my work on two repos that were very helpful to get started:
- https://github.com/r-shekhar/NYC-transport
- https://github.com/toddwschneider/nyc-taxi-data

Lookup data is from official NYC site: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page  

Raw data downloaded using "wget" with -i and -P flags using list of files from aforementioned repo: https://github.com/toddwschneider/nyc-taxi-data/blob/master/setup_files/raw_data_urls.txt  

## Structure
Description of files:
- helper_objects.py - some helper functions as well as definitions of parameters for various files since the schema changed over time
- data_processing.py - main functions for processing data (uses helper_objects.py as dependency)
- lookup - folder with lookup data for taxi zones in New York City, there is shapefile with geometries and csv file with mappings (id:name), attaching here for easier setup
- taxi-eda.ipynb - jupyter notebook with leftover pieces of code I used to analyze the data in no particular order, uploaded it to repo should I want to modify something in the process as notebooks make it easier to iterate
- zones.geojson - I converted shapefile from the lookup data to geojson using geopandas to be able to render the data in jupyter lab for testing
- athena_ddl.sql - example of athena script to create table out of the files stored in s3, for compatilibty some fields have unoptimal data types (dates as string, ids as float), once the table is created another table can be created with values cast to the right types

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
In data_processing.py the main methods to process and save as parquet all downloaded files for green taxi or yellow taxi companies are:
- csv2parquet_green_taxi
- csv2parquet_yellow_taxi

If you just want to get a cleaned DataFrame of a single file import and use method:
- process_yellow_taxi_data

or

- process_green_taxi_data

Example:
```python
from data_processing import *

csv2parquet(['path1', 'path2', 'etc'])
```
