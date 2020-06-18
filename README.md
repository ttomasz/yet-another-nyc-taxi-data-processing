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

## Requirements
Since geopandas requires GDAL libraries it is the easiest to use conda environment and install packages using:
```
conda install -c conda-forge beautifulsoup4 bokeh distributed fastparquet geopandas jupyter numba palettable pyarrow python-snappy scikit-learn seaborn
```
This is from Shekhar's repo, I did not use all of the packages (mostly geopandas, fastparquet, and python-snappy) but installing them doesn't hurt.

## Usage
In data_processing.py the main methods to process and save as parquet all downloaded files for green taxi or yellow taxi companies are:
- csv2parquet_green_taxi
- csv2parquet_yellow_taxi

If you just want to get a cleaned DataFrame of a single file import and use method:
- process_yellow_taxi_data

or

- process_green_taxi_data

## Known issues
- some dates are not parsed well or are wrong and scripts do not discard records like that, when using the data make sure to only select records where year is between 2009 and 2029 or something
