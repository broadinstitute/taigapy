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
from taigapy.client_v3 import (
    LocalFormat,
    DatasetVersion,
    TaigaStorageFormat,
    Client,
    UploadedFile,
    DatasetVersionFile,
)
from taigapy.format_utils import write_hdf5, write_parquet, convert_csv_to_hdf5
from taigapy.types import S3Credentials

import pytest

sample_matrix = pd.DataFrame(data={"a": [1.2, 2.0], "b": [2.1, 3.0]}, index=["x", "y"])
sample_table = pd.DataFrame(data={"a": [1.2, 2.0], "b": [2.1, 3.0]})


from unittest.mock import create_autospec
from typing import Union
from taigapy.types import DatasetMetadataDict, DatasetVersionMetadataDict


@dataclass
class MockDBDataset:
    id: str
    name: str
    permanames: List[str]


@dataclass
class MockDBDatasetVersion(DatasetVersion):
    id: str
    dataset_id: str


@dataclass
class MockDB:
    # map of simulated taiga session ID -> list of files added to the session
    sessions: Dict[str, List[str]]

    # map of simulated dataset id -> permaname
    dataset_by_permaname: Dict[str, MockDBDataset]
    dataset_by_id: Dict[str, MockDBDataset]
    dataset_versions_by_id: Dict[str, MockDBDatasetVersion]
    dataset_versions_by_permaname_version: Dict[str, MockDBDatasetVersion]

    # map of {bucket}/{key} to bytes stored in s3
    s3_objects: Dict[str, bytes]
    datafiles_by_id: Dict[str, str]

    def __init__(self):
        self.dataset_by_id = {}
        self.dataset_by_permaname = {}
        self.sessions = {}
        self.dataset_versions_by_id = {}
        self.dataset_versions_by_permaname_version = {}
        self.s3_objects = {}
        self.datafiles_by_id = {}

    def add_dataset(self, dataset: MockDBDataset):
        assert dataset.id not in self.dataset_by_id
        self.dataset_by_id[dataset.id] = dataset
        for permaname in dataset.permanames:
            assert permaname not in self.dataset_by_permaname
            self.dataset_by_permaname[permaname] = dataset

    def add_version(self, dataset_version: MockDBDatasetVersion):
        assert dataset_version.permanames[0] in self.dataset_by_permaname

        assert dataset_version.id not in self.dataset_versions_by_id
        self.dataset_versions_by_id[dataset_version.id] = dataset_version
        permaname_version = (
            f"{dataset_version.permanames[0]}.{dataset_version.version_number}"
        )
        assert permaname_version not in self.dataset_versions_by_permaname_version
        self.dataset_versions_by_permaname_version[permaname_version] = dataset_version

    def get_version_by_id(self, dataset_version_id):
        return self.dataset_versions_by_id.get(dataset_version_id)

    def get_versions_by_dataset_id(self, dataset_id):
        versions = [
            v
            for v in self.dataset_versions_by_id.values()
            if dataset_id == v.dataset_id
        ]
        return versions

    def get_dataset(self, permaname_or_id):
        dataset = self.dataset_by_id.get(permaname_or_id)
        if dataset is not None:
            return dataset
        return self.dataset_by_permaname.get(permaname_or_id)

    def get_by_permaname_version(self, permaname, version_number):
        permaname_version = f"{permaname}.{version_number}"
        return self.dataset_versions_by_permaname_version[permaname_version]


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

    db = MockDB()

    def _upload_file(local_path, bucket, key):
        with open(local_path, "rb") as fd:
            bytes = fd.read()
        db.s3_objects[f"{bucket}/{key}"] = bytes

    s3_mock_client.upload_file.side_effect = _upload_file

    from typing import cast

    def _get_dataset_version_metadata(
        dataset_permaname: str, dataset_version: Optional[str]
    ) -> Union[DatasetMetadataDict, DatasetVersionMetadataDict]:
        # this function can be called 3 ways:  (This is a ridiculous function)
        # 1. if no version is provided, then it fetches the dataset information.
        # 2. if a permaname and a _version_number_ is specified it returns dataset version data
        # 3. if permaname is None then dataset_version is interpreted as a version ID and version metadata returned

        assert not (dataset_permaname is None and dataset_version is None)

        if dataset_permaname is not None:
            assert isinstance(dataset_permaname, str)
            if dataset_version is None:
                dataset = db.get_dataset(dataset_permaname)
                assert dataset is not None
                versions = db.get_versions_by_dataset_id(dataset.id)
                return cast(
                    DatasetMetadataDict,
                    {
                        "name": "name",
                        "permanames": dataset.permanames,
                        "versions": [
                            {"id": v.id, "name": v.version_number, "state": "approved"}
                            for v in versions
                        ],
                        "can_edit": True,
                        "can_view": True,
                        "description": "description",
                        "folders": [],
                        "id": dataset.id,
                    },
                )
            else:
                dataset_version_ = db.get_by_permaname_version(
                    dataset_permaname, dataset_version
                )
        else:
            dataset_version_id = dataset_version
            dataset_version_ = db.get_version_by_id(dataset_version_id)

        if dataset_version_ is None:
            print("_get_dataset_version_metadata: dataset_version is None")
            return None

        assert dataset_version_ is not None
        # not bothering to return all fields. Only those that this code relies on.
        return cast(
            DatasetVersionMetadataDict,
            {
                "dataset": {"name": "name", "permanames": dataset_version_.permanames},
                "datasetVersion": {
                    # "can_edit": True,
                    # "can_view": True,
                    #        "creation_date": "",
                    #        "creator": User,
                    "datafiles": [
                        {
                            "allowed_conversion_type": ["raw"],
                            "datafile_type": "s3",
                            "id": f"{dataset_permaname}.{dataset_version_.version_number}/{file.name}",
                            "name": file.name,
                            "short_summary": "",
                            "type": file.format,
                            # "original_file_md5": None,
                            # "original_file_sha256": None,
                            "custom_metadata": file.custom_metadata,
                        }
                        for file in dataset_version_.files
                    ],
                    # "dataset_id": str,
                    "description": "",
                    #        "folders": List[Folder],  # empty list (TODO: remove)
                    #        "id": str,
                    "name": "1",
                    #        "reason_state": str,
                    "state": "Approved",
                    "version": str(dataset_version_.version_number),
                },
            },
        )

    api.get_dataset_version_metadata.side_effect = _get_dataset_version_metadata

    def _create_dataset(
        upload_session_id: str,
        folder_id: str,
        dataset_name: str,
        dataset_description: Optional[str],
    ) -> str:
        files = db.sessions[upload_session_id]

        dataset_id = "ds-" + uuid.uuid4().hex
        permaname = "p-" + uuid.uuid4().hex
        version = 1

        version_files = []
        for f in files:
            if f["s3Upload"]["format"] == DataFileUploadFormat.NumericMatrixCSV.value:
                format = "HDF5"
                # simulate conversion
                # fetch the original bytes
                csv_bytes = db.s3_objects[
                    f["s3Upload"]["bucket"] + "/" + f["s3Upload"]["key"]
                ]

                # convert csv to HDF5
                with tempfile.NamedTemporaryFile(mode="wb") as csv_fd:
                    csv_fd.write(csv_bytes)
                    csv_fd.flush()
                    with tempfile.NamedTemporaryFile(mode="wb") as hdf5_fd:
                        db.s3_objects
                        convert_csv_to_hdf5(csv_fd.name, hdf5_fd.name)
                        # read out the resulting file into memory
                        with open(hdf5_fd.name, "rb") as fd:
                            hdf5_bytes = fd.read()

                # create a new key, and update the data for that key in s3
                f = dict(f)
                f["s3Upload"]["key"] = "s3-" + uuid.uuid4().hex
                db.s3_objects[
                    f["s3Upload"]["bucket"] + "/" + f["s3Upload"]["key"]
                ] = hdf5_bytes
            elif f["s3Upload"]["format"] == DataFileUploadFormat.TableCSV.value:
                format = "Columnar"
            else:
                assert f["s3Upload"]["format"] == DataFileUploadFormat.Raw.value
                format = "Raw"

            datafile_id = f"{permaname}.{version}/{f['filename']}"
            version_files.append(
                DatasetVersionFile(
                    name=f["filename"],
                    custom_metadata=f["custom_metadata"],
                    format=format,
                    gs_path=None,
                    datafile_id=datafile_id,
                )
            )
            db.datafiles_by_id[datafile_id] = f

        assert permaname is not None
        dataset_version = MockDBDatasetVersion(
            dataset_id=dataset_id,
            permanames=[permaname],
            version_number=version,
            description=dataset_description,
            files=version_files,
            id="v-" + uuid.uuid4().hex,
        )
        dataset = MockDBDataset(
            id=dataset_id, name=dataset_name, permanames=[permaname]
        )

        # dataset_version_id = "mock-version-id"
        # assert dataset_version_id not in dataset_versions_by_id
        # dataset_versions_by_id[dataset_version_id] = dataset_version
        # dataset_versions_by_permaname_version[f"{permaname}.1"] = dataset_version
        # dataset_permaname_by_id[dataset_id] = permaname
        db.add_dataset(dataset)
        db.add_version(dataset_version)

        return dataset_id

    api.create_dataset.side_effect = _create_dataset

    def _upload_file_to_taiga(session_id: str, session_file):
        if isinstance(session_file, dict):
            api_params = session_file
        else:
            api_params = session_file.to_api_param()

        for k, v in api_params.get("metadata", {}).items():
            assert isinstance(k, str)
            assert isinstance(v, str)

        db.sessions[session_id].append(api_params)

    api.upload_file_to_taiga.side_effect = _upload_file_to_taiga

    def _create_session():
        session_id = "s-" + uuid.uuid4().hex
        db.sessions[session_id] = []
        return session_id

    api.create_upload_session.side_effect = _create_session

    def _download_datafile(
        dataset_permaname: str,
        dataset_version: str,
        datafile_name: str,
        dest: str,
        *,
        format="test_raw",
    ):
        key = f"{dataset_permaname}.{dataset_version}/{datafile_name}"
        f = db.datafiles_by_id[key]
        s3_key = f["s3Upload"]["bucket"] + "/" + f["s3Upload"]["key"]
        bytes = db.s3_objects[s3_key]
        with open(dest, "wb") as fd:
            fd.write(bytes)
        print(
            f"Fetched {s3_key} from s3 and got {len(bytes)} bytes and writing to {dest}"
        )

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

def test_remove_old_cached_files(mock_client: Client, tmpdir):
    # TODO: Add a test that verifies that remove_old_cached_files actually removes files from the cache
    # 1. Create some files in the cache
    # 2. Call remove_old_cached_files
    # 3. Verify that the files are removed from the cache
    mock_client.remove_old_cached_files()
    assert True
