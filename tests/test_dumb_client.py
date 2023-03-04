import pandas as pd
from typing import Optional, Dict, List
from taigapy.taiga_api import TaigaApi
import tempfile
from dataclasses import dataclass
import boto3
import colorful as cf
import uuid
from taigapy.types import DatasetVersionMetadataDict
from taigapy.types import DataFileUploadFormat
from taigapy.dumb_client import LocalFormat, DatasetVersion, write_hdf5, write_parquet, TaigaStorageFormat, Client, UploadedFile, DatasetVersionFile, convert_csv_to_hdf5
from taigapy.types import (
    S3Credentials
)

import pytest

sample_matrix = pd.DataFrame(data={"a": [1.2, 2.0], "b": [2.1, 3.0]}, index=["x", "y"])
sample_table = pd.DataFrame(data={"a": [1.2, 2.0], "b": [2.1, 3.0]})



from unittest.mock import create_autospec
from typing import Union
from taigapy.types import DatasetMetadataDict, DatasetVersionMetadataDict


@pytest.fixture
def mock_client(tmpdir, s3_mock_client):
    api = create_autospec(TaigaApi)
    api.url = "https://mock/"
    api.get_s3_credentials.return_value = S3Credentials(
        {
            "accessKeyId": "a",
            "bucket": "bucket",
            "expiration": "expiration",
            "prefix": "prefix",
            "secretAccessKey": "secretAccessKey",
            "sessionToken": "sessionToken",
        }
    )

    # map of simulated taiga session ID -> list of files added to the session
    sessions = {}
    dataset_versions: Dict[str, DatasetVersion] = {}
    # map of {bucket}/{key} to bytes stored in s3
    s3_objects = {}
    datafiles_by_id = {}

    def _upload_file(local_path, bucket, key):
        with open(local_path, "rb") as fd:
            bytes = fd.read()
        s3_objects[f"{bucket}/{key}"] = bytes
    s3_mock_client.upload_file.side_effect = _upload_file

    def _get_dataset_version_metadata(
        dataset_permaname: str, dataset_version: Optional[str]
    ) -> Union[DatasetMetadataDict, DatasetVersionMetadataDict]:
        version = dataset_version
        dataset_version = dataset_versions.get(f"{dataset_permaname}.{version}")
        if dataset_version is None:
            return None

        assert version is not None
        # not bothering to return all fields. Only those that this code relies on.
        return {
            "dataset": {"name": "name", "permanames": [dataset_permaname]},
            "datasetVersion": {
                # "can_edit": True,
                # "can_view": True,
                #        "creation_date": "",
                #        "creator": User,
                "datafiles": [
                    {
                        "allowed_conversion_type": ["raw"],
                        "datafile_type": "s3",
                        "id": f"{dataset_permaname}.{version}/{file.name}",
                        "name": file.name,
                        "short_summary": "",
                        "type": file.format,
                        "underlying_file_id": None,
                        # "original_file_md5": None,
                        # "original_file_sha256": None,
                        "metadata": file.metadata
                    }
                    for file in dataset_version.files
                ],
                # "dataset_id": str,
                # "description": str,
                #        "folders": List[Folder],  # empty list (TODO: remove)
                #        "id": str,
                #        "name": str,
                #        "reason_state": str,
                "state": "Approved",
                "version": version,
            },
        }

    api.get_dataset_version_metadata.side_effect = _get_dataset_version_metadata

    def _create_dataset(
        upload_session_id: str,
        folder_id: str,
        dataset_name: str,
        dataset_description: Optional[str],
    ):
        files = sessions[upload_session_id]

        permaname = uuid.uuid4().hex
        version = 1

        version_files = []
        for f in files:
            if f["s3Upload"]["format"] == DataFileUploadFormat.NumericMatrixCSV.value:
                format = "HDF5"
                # simulate conversion
                # fetch the original bytes
                csv_bytes = s3_objects[f["s3Upload"]["bucket"]+"/"+f["s3Upload"]["key"]]

                # convert csv to HDF5
                with tempfile.NamedTemporaryFile(mode="wb") as csv_fd:
                    csv_fd.write(csv_bytes)
                    csv_fd.flush()
                    with tempfile.NamedTemporaryFile(mode="wb") as hdf5_fd:
                        s3_objects
                        convert_csv_to_hdf5(csv_fd.name, hdf5_fd.name)
                        # read out the resulting file into memory
                        with open(hdf5_fd.name, "rb") as fd:
                            hdf5_bytes = fd.read()

                # create a new key, and update the data for that key in s3
                f = dict(f)
                f["s3Upload"]["key"] = uuid.uuid4().hex
                s3_objects[f["s3Upload"]["bucket"]+"/"+f["s3Upload"]["key"]] = hdf5_bytes
            elif f["s3Upload"]["format"] == DataFileUploadFormat.TableCSV.value:
                format = "Columnar"
            else:
                assert f["s3Upload"]["format"] == DataFileUploadFormat.Raw.value
                format = "Raw"

            datafile_id = f"{permaname}.{version}/{f['filename']}"
            version_files.append(
                DatasetVersionFile(
                    name=f["filename"],
                    metadata=f["metadata"],
                    format=format,
                    gs_path=None,
                    datafile_id=datafile_id,
                )
                )
            datafiles_by_id[datafile_id] = f

        dataset_version = DatasetVersion(
            permanames=[permaname],
            version_number=version,
            version_id=uuid.uuid4().hex,
            description=dataset_description,
            files=version_files,
        )

        dataset_versions[f"{permaname}.1"] = dataset_version

        return dataset_version

    api.create_dataset.side_effect = _create_dataset

    def _upload_file_to_taiga(session_id: str, session_file):
        if isinstance(session_file, dict):
            api_params = session_file
        else:
            api_params = session_file.to_api_param()

        for k, v in api_params.get('metadata', {}).items():
            assert isinstance(k, str)
            assert isinstance(v, str)

        sessions[session_id].append(api_params)

    api.upload_file_to_taiga.side_effect = _upload_file_to_taiga

    def _create_session():
        session_id = uuid.uuid4().hex
        sessions[session_id] = []
        return session_id

    api.create_upload_session.side_effect = _create_session

    def _download_datafile(dataset_permaname: str,
        dataset_version: str,
        datafile_name: str,
        dest: str):
        key = f"{dataset_permaname}.{dataset_version}/{datafile_name}"
        f = datafiles_by_id[key]
        s3_key = f["s3Upload"]["bucket"]+"/"+f["s3Upload"]["key"]
        bytes = s3_objects[s3_key]
        with open(dest, "wb") as fd:
            fd.write(bytes)
        print(f"Fetched {s3_key} from s3 and got {len(bytes)} bytes and writing to {dest}")
    api.download_datafile.side_effect = _download_datafile

    client = Client(str(tmpdir.join("cache")), api)
    return client

@pytest.fixture
def s3_mock_client(monkeypatch):
    import unittest.mock

    mock_s3_client_fn = unittest.mock.MagicMock()
    mock_s3_client = unittest.mock.MagicMock()
    mock_s3_client_fn.return_value = mock_s3_client
    monkeypatch.setattr(boto3, "client", mock_s3_client_fn)
    return mock_s3_client


write_csv_matrix = lambda df, dest: df.to_csv(dest, index=True)
write_csv_table = lambda df, dest: df.to_csv(dest, index=False)
writers_by_format = {
    LocalFormat.HDF5_MATRIX.value: write_hdf5,
    LocalFormat.PARQUET_TABLE.value: write_parquet,
    LocalFormat.CSV_TABLE.value: write_csv_table,
    LocalFormat.CSV_MATRIX.value: write_csv_matrix
}

@pytest.mark.parametrize(
    "df,write_initial_file,upload_format,expected_taiga_format",
    [
        (sample_matrix, write_hdf5, LocalFormat.HDF5_MATRIX, TaigaStorageFormat.RAW_HDF5_MATRIX.value),
       (sample_matrix, write_csv_matrix, LocalFormat.CSV_MATRIX, TaigaStorageFormat.HDF5_MATRIX.value),
       (sample_table, write_parquet, LocalFormat.PARQUET_TABLE, TaigaStorageFormat.RAW_PARQUET_TABLE),
        (sample_table, write_csv_table, LocalFormat.CSV_TABLE, TaigaStorageFormat.CSV_TABLE),
    ],
)
def test_upload_hdf5(
    mock_client: Client, tmpdir, df: pd.DataFrame, write_initial_file, upload_format: LocalFormat, expected_taiga_format: TaigaStorageFormat, s3_mock_client
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
                metadata = {}
            )
        ],
    )
    assert len(version.files) == 1
    file = version.files[0]

    fetched_df = mock_client.get(file.datafile_id)

    assert df.equals(fetched_df)
