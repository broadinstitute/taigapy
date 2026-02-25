"""Shared pytest fixtures for taigapy tests."""

import tempfile
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from unittest.mock import create_autospec

import boto3
import pytest

from taigapy.client_v3 import Client, DatasetVersion, DatasetVersionFile
from taigapy.format_utils import convert_csv_to_hdf5
from taigapy.taiga_api import TaigaApi
from taigapy.types import (
    DataFileUploadFormat,
    DatasetMetadataDict,
    DatasetVersionMetadataDict,
    S3Credentials,
)


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
    """In-memory database for mocking Taiga server state."""

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
        return self.dataset_versions_by_permaname_version.get(permaname_version)


@pytest.fixture
def s3_mock_client(monkeypatch):
    """Mock S3 client for testing uploads."""
    import unittest.mock

    mock_s3_client_fn = unittest.mock.MagicMock()
    mock_s3_client = unittest.mock.MagicMock()
    mock_s3_client_fn.return_value = mock_s3_client
    monkeypatch.setattr(boto3, "client", mock_s3_client_fn)
    return mock_s3_client


@pytest.fixture
def mock_db():
    """Create a fresh MockDB for each test."""
    return MockDB()


@pytest.fixture
def mock_client(tmpdir, s3_mock_client, mock_db):
    """
    Create a real V3 Client with mocked API layer.

    This fixture creates an actual Client instance but replaces the TaigaApi
    with mocks that use an in-memory MockDB. This allows testing client behavior
    without making real API calls.

    The mock_db fixture is injected so tests can pre-populate it with datasets.
    """
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

    db = mock_db

    def _upload_file(local_path, bucket, key):
        with open(local_path, "rb") as fd:
            bytes = fd.read()
        db.s3_objects[f"{bucket}/{key}"] = bytes

    s3_mock_client.upload_file.side_effect = _upload_file

    from typing import cast

    def _get_dataset_version_metadata(
        dataset_permaname: str, dataset_version: Optional[str]
    ) -> Union[DatasetMetadataDict, DatasetVersionMetadataDict]:
        # this function can be called 3 ways:
        # 1. if no version is provided, then it fetches the dataset information.
        # 2. if a permaname and a _version_number_ is specified it returns dataset version data
        # 3. if permaname is None then dataset_version is interpreted as a version ID and version metadata returned

        assert not (dataset_permaname is None and dataset_version is None)

        if dataset_permaname is not None:
            assert isinstance(dataset_permaname, str)
            if dataset_version is None:
                dataset = db.get_dataset(dataset_permaname)
                if dataset is None:
                    from taigapy.custom_exceptions import Taiga404Exception
                    raise Taiga404Exception(f"Dataset {dataset_permaname} not found")
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

        def _build_datafile_dict(file):
            """Build datafile dict, omitting 'type' for GCS files."""
            d = {
                "allowed_conversion_type": ["raw"],
                "datafile_type": "s3",
                "id": f"{dataset_permaname}.{dataset_version_.version_number}/{file.name}",
                "name": file.name,
                "short_summary": "",
                "custom_metadata": file.custom_metadata,
            }
            # Only include 'type' if format is set (GCS files don't have type)
            if file.format is not None:
                d["type"] = file.format
            return d

        return cast(
            DatasetVersionMetadataDict,
            {
                "dataset": {"name": "name", "permanames": dataset_version_.permanames},
                "datasetVersion": {
                    "datafiles": [
                        _build_datafile_dict(file)
                        for file in dataset_version_.files
                    ],
                    "description": "",
                    "name": "1",
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
            # Handle virtual files (TaigaReference)
            if f.get("filetype") == "virtual":
                format = "format-not-implemented-in-mock-for-virtual"
                datafile_id = f"{permaname}.{version}/{f['filename']}"
                version_files.append(
                    DatasetVersionFile(
                        name=f["filename"],
                        custom_metadata=f.get("custom_metadata", f.get("metadata", {})),
                        format=format,
                        gs_path=None,
                        datafile_id=datafile_id,
                    )
                )
                continue

            # Handle S3 uploads
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


def add_mock_dataset_version(
    mock_db: MockDB,
    permaname: str,
    version: int,
    files: List[Dict],
    dataset_id: Optional[str] = None,
    description: str = "",
):
    """
    Helper to add a dataset version to the mock database.

    Args:
        mock_db: The MockDB instance to populate
        permaname: Dataset permaname (e.g., "my-dataset-1234")
        version: Version number
        files: List of file dicts with keys: name, format (e.g., "HDF5", "Columnar"), custom_metadata
        dataset_id: Optional dataset ID (auto-generated if not provided)
        description: Version description
    """
    dataset_id = dataset_id or f"ds-{permaname}"

    # Create dataset if it doesn't exist
    if permaname not in mock_db.dataset_by_permaname:
        dataset = MockDBDataset(
            id=dataset_id,
            name=permaname,
            permanames=[permaname],
        )
        mock_db.add_dataset(dataset)

    # Create version files
    version_files = [
        DatasetVersionFile(
            name=f["name"],
            format=f.get("format", "HDF5"),
            gs_path=f.get("gs_path"),
            datafile_id=f"{permaname}.{version}/{f['name']}",
            custom_metadata=f.get("custom_metadata", {}),
        )
        for f in files
    ]

    dataset_version = MockDBDatasetVersion(
        id=f"v-{permaname}-{version}",
        dataset_id=dataset_id,
        permanames=[permaname],
        version_number=version,
        description=description,
        files=version_files,
    )
    mock_db.add_version(dataset_version)
