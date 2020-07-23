import os
from datetime import datetime
from sys import stdout
from typing import List

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_processing import process_taxi_data_file
from helper_objects import arrow_schema, yellow_taxi_paths, green_taxi_paths, timer


def csv2parquet(paths: List[str], output_folder: str) -> None:
    of = len(paths)

    for i, path in enumerate(paths):
        source_file_name = os.path.basename(path)
        result_file_name = source_file_name.split('.')[0] + '.parquet'
        result_file_path = os.path.join(output_folder, result_file_name)

        stdout.write(f"{str(i+1).zfill(2)}/{str(of).zfill(2)} - {datetime.now().isoformat(timespec='seconds')} - processing: {source_file_name}\n")
        df = process_taxi_data_file(path)
        stdout.write(f"{str(i+1).zfill(2)}/{str(of).zfill(2)} - {datetime.now().isoformat(timespec='seconds')} - writing DataFrame as parquet file: {result_file_name}\n")
        _write_to_parquet(df, result_file_path)
        stdout.write(f"{str(i+1).zfill(2)}/{str(of).zfill(2)} - {datetime.now().isoformat(timespec='seconds')} - finished writing DataFrame as parquet file: {result_file_name}\n")

    stdout.write(f"{datetime.now().isoformat(timespec='seconds')} - finished processing files.\n")


def csv2parquet_green_taxi(taxi_data_basepath: str, output_folder: str) -> None:
    csv2parquet(green_taxi_paths(taxi_data_basepath), output_folder)


def csv2parquet_yellow_taxi(taxi_data_basepath: str, output_folder: str) -> None:
    csv2parquet(yellow_taxi_paths(taxi_data_basepath), output_folder)


@timer
def _write_to_parquet(data_frame: pd.DataFrame, filepath: str) -> None:
    # prepare arrow table
    #   sorting for better compression
    #   replacing NA with NaN due to current incompatibility of pyarrow with that type
    table = pa.Table.from_pandas(
        df=data_frame.fillna(np.nan).sort_values(by=['pickup_location_id', 'dropoff_location_id']),
        Schema_schema=arrow_schema,
        preserve_index=False)
    # write table to parquet file
    pq.write_table(table=table, where=filepath, flavor='spark')
