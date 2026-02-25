import pandas as pd
import datetime as dt

import pytest

from taigapy.client_v3 import (
    LocalFormat,
    TaigaStorageFormat,
    Client,
    UploadedFile,
)
from taigapy.format_utils import write_hdf5, write_parquet

sample_matrix = pd.DataFrame(data={"a": [1.2, 2.0], "b": [2.1, 3.0]}, index=["x", "y"])
sample_table = pd.DataFrame(data={"a": [1.2, 2.0], "b": [2.1, 3.0]})


write_csv_matrix = lambda df, dest: df.to_csv(dest, index=True)
write_csv_table = lambda df, dest: df.to_csv(dest, index=False)
writers_by_format = {
    LocalFormat.HDF5_MATRIX.value: write_hdf5,
    LocalFormat.PARQUET_TABLE.value: write_parquet,
    LocalFormat.CSV_TABLE.value: write_csv_table,
    LocalFormat.CSV_MATRIX.value: write_csv_matrix,
}


def test_upload_with_fault_injection(mock_client: Client, tmpdir, s3_mock_client):
    sample_file = tmpdir.join("file")
    df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
    df.to_csv(str(sample_file), index=False)

    version = mock_client.create_dataset(
        "test",
        "desc",
        [
            UploadedFile(
                name="matrix",
                local_path=str(sample_file),
                format=LocalFormat.CSV_TABLE,
                custom_metadata={},
            )
        ],
    )
    assert len(version.files) == 1
    file = version.files[0]

    fetched_df = mock_client.get(file.datafile_id)

    assert df.equals(fetched_df)


@pytest.mark.parametrize(
    "df,write_initial_file,upload_format,expected_taiga_format",
    [
        (
            sample_matrix,
            write_hdf5,
            LocalFormat.HDF5_MATRIX,
            TaigaStorageFormat.RAW_HDF5_MATRIX.value,
        ),
        (
            sample_matrix,
            write_csv_matrix,
            LocalFormat.CSV_MATRIX,
            TaigaStorageFormat.HDF5_MATRIX.value,
        ),
        (
            sample_table,
            write_parquet,
            LocalFormat.PARQUET_TABLE,
            TaigaStorageFormat.RAW_PARQUET_TABLE,
        ),
        (
            sample_table,
            write_csv_table,
            LocalFormat.CSV_TABLE,
            TaigaStorageFormat.CSV_TABLE,
        ),
    ],
)

def test_upload_hdf5(
    mock_client: Client,
    tmpdir,
    df: pd.DataFrame,
    write_initial_file,
    upload_format: LocalFormat,
    expected_taiga_format: TaigaStorageFormat,
    s3_mock_client,
):
    sample_file = tmpdir.join("file")
    write_initial_file(df, str(sample_file))

    version = mock_client.create_dataset(
        "test",
        "desc",
        [
            UploadedFile(
                name="matrix",
                local_path=str(sample_file),
                format=upload_format,
                custom_metadata={},
            )
        ],
    )
    assert len(version.files) == 1
    file = version.files[0]

    fetched_df = mock_client.get(file.datafile_id)

    assert df.equals(fetched_df)


@pytest.mark.parametrize(
    "df,write_initial_file,upload_format,expected_taiga_format",
    [
        (
            sample_matrix,
            write_hdf5,
            LocalFormat.HDF5_MATRIX,
            TaigaStorageFormat.RAW_HDF5_MATRIX.value,
        ),
        (
            sample_matrix,
            write_csv_matrix,
            LocalFormat.CSV_MATRIX,
            TaigaStorageFormat.HDF5_MATRIX.value,
        ),
        (
            sample_table,
            write_parquet,
            LocalFormat.PARQUET_TABLE,
            TaigaStorageFormat.RAW_PARQUET_TABLE,
        ),
        (
            sample_table,
            write_csv_table,
            LocalFormat.CSV_TABLE,
            TaigaStorageFormat.CSV_TABLE,
        ),
    ],
)
def test_get_dataframe_offline(
    mock_client: Client,
    tmpdir,
    df: pd.DataFrame,
    write_initial_file,
    upload_format: LocalFormat,
    expected_taiga_format: TaigaStorageFormat,
    s3_mock_client,
    monkeypatch,
):
    sample_file = tmpdir.join("file")
    write_initial_file(df, str(sample_file))

    version = mock_client.create_dataset(
        "test",
        "desc",
        [
            UploadedFile(
                name="matrix",
                local_path=str(sample_file),
                format=upload_format,
                custom_metadata={},
            )
        ],
    )
    assert len(version.files) == 1
    file = version.files[0]

    def mock_is_not_connected() -> bool:
        return False

    def mock_is_connected() -> bool:
        return True

    # Offline mode
    monkeypatch.setattr(mock_client.api, "is_connected", mock_is_not_connected)

    assert mock_client.api.is_connected() == False

    # We can't get a file that isn't in the cache if we're offline
    with pytest.raises(Exception):
        fetched_df = mock_client.get(file.datafile_id)

    # If we try to get the file with a good connection, the file should be added to cache
    monkeypatch.setattr(mock_client.api, "is_connected", mock_is_connected)
    assert mock_client.api.is_connected() == True
    fetched_df = mock_client.get(file.datafile_id)
    assert df.equals(fetched_df)

    # If we disconnect from the api again, and try to retrieve the file while disconnected,
    # this time, we should be successful because the file can be retrieved from the cache
    monkeypatch.setattr(mock_client.api, "is_connected", mock_is_not_connected)
    assert mock_client.api.is_connected() == False
    fetched_df = mock_client.get(file.datafile_id)

    assert df.equals(fetched_df)

def test_clear_cache(mock_client: Client, tmpdir, s3_mock_client):
    sample_file = tmpdir.join("file")
    df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
    df.to_csv(str(sample_file), index=False)

    version = mock_client.create_dataset(
        "test",
        "desc",
        [
            UploadedFile(
                name="matrix",
                local_path=str(sample_file),
                format=LocalFormat.CSV_TABLE,
                custom_metadata={},
            )
        ],
    )
    assert len(version.files) == 1
    file = version.files[0]

    fetched_df = mock_client.get(file.datafile_id)

    assert df.equals(fetched_df)

    bytes_freed = mock_client.remove_old_cached_files()
    assert bytes_freed == 0

    bytes_freed = mock_client.remove_old_cached_files(dt.timedelta(days=0))
    assert bytes_freed > 0
