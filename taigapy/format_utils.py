import h5py
import numpy as np
import os
import pandas as pd
from pyarrow import parquet as pq


# Define reading and writing functions
def write_hdf5(df: pd.DataFrame, filename: str):
    if os.path.exists(filename):
        os.remove(filename)

    dest = h5py.File(filename, mode="w")

    try:
        dim_0 = [x.encode("utf8") for x in df.index]
        dim_1 = [x.encode("utf8") for x in df.columns]

        dest.create_dataset("dim_0", track_times=False, data=dim_0)
        dest.create_dataset("dim_1", track_times=False, data=dim_1)
        dest.create_dataset(
            "data", track_times=False, data=df.values, compression="gzip"
        )
    finally:
        dest.close()


def read_hdf5(filename: str) -> pd.DataFrame:
    src = h5py.File(filename, mode="r")
    try:
        dim_0 = [x.decode("utf8") for x in src["dim_0"]]
        dim_1 = [x.decode("utf8") for x in src["dim_1"]]
        data = np.array(src["data"])
        return pd.DataFrame(index=dim_0, columns=dim_1, data=data)
    finally:
        src.close()


def write_parquet(df: pd.DataFrame, dest: str):
    df.to_parquet(dest)

def read_parquet(filename: str) -> pd.DataFrame:
    df = pd.read_parquet(filename)
    # tables (via read_parquet) should never have an index according to Taiga. So, call
    # reset_index() on df before returning the result so any index columns are treated
    # as normal columns. _However_ pandas will always create
    # a default index because all dataframes need one. So, let's check for that specific case
    # and skip the reset_index() if we have a default index
    if isinstance( df.index, pd.RangeIndex ) and df.index.name is None:
        return df
    return df.reset_index()


def convert_csv_to_hdf5(csv_path: str, hdf5_path: str):
    df = pd.read_csv(csv_path, index_col=0, low_memory=False)
    write_hdf5(df, hdf5_path)


def convert_csv_to_parquet(csv_path: str, parquet_path: str):
    df = pd.read_csv(csv_path, low_memory=False)
    write_parquet(df, parquet_path)
