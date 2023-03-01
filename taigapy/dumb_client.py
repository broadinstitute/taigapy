import pandas as pd
import shelve
from typing import TypeVar, Generic, Optional, Dict, List
import os
from .taiga_api import TaigaApi
import re
from enum import Enum, auto
import tempfile
from dataclasses import dataclass
import boto3
import colorful as cf

from taigapy.types import (
    S3Credentials,
    #     DataFileFormat,
    #     DataFileMetadata,
    #     DatasetMetadataDict,
    #     DatasetVersion,
    #     DatasetVersionMetadataDict,
    #     DatasetVersionState,
    #     S3Credentials,
    #     UploadS3DataFile,
    #     UploadS3DataFileDict,
    #     UploadVirtualDataFile,
    #     UploadVirtualDataFileDict,
    #     UploadGCSDataFileDict,
    #     UploadGCSDataFile,
    #     UploadDataFile,
)


# class Structure(Enum):
#     TABLE = auto()
#     MATRIX = auto()
#     UNSTRUCTURED = auto()

class LocalFormat(Enum):
    HDF5_MATRIX = "hdf5_matrix"
    PARQUET_TABLE = "parquet_table"
    CSV_TABLE = "csv_table"
    CSV_MATRIX = "csv_matrix"

class TaigaStorageFormat(Enum):
    CSV_TABLE = "csv_table"
    HDF5_MATRIX = "hdf5_matrix"
    RAW_HDF5_MATRIX = "raw_hdf5_matrix"
    RAW_PARQUET_TABLE = "raw_parquet_table"
    RAW_BYTES = "raw_bytes"


V = TypeVar("V")

# todo:
# 1. write end-to-end integration test
#   still need to write tests for updating datasets
# 2. make mock taiga client which simulates submissions and responses


@dataclass
class DataFileID:
    permaname: str
    version: int
    name: str


DATAFILE_ID_PATTERN = "([a-z0-9]+)\\.([0-9]+)/(.*)"


def _parse_datafile_id(datafile_id):
    m = re.match(DATAFILE_ID_PATTERN, datafile_id)
    if m:
        permname, version, name = m.groups()
        return DataFileID(permname, int(version), name)
    else:
        return None


import shelve


class Cache(Generic[V]):
    def __init__(self, filename: str) -> None:
        super().__init__()
        self.in_memory_cache = {}
        self.filename = filename

    def _ensure_parent_dir_exists(self):
        parent = os.path.dirname(self.filename)
        if not os.path.exists(parent):
            os.makedirs(parent)

    def get(self, key: str, default: Optional[V]) -> V:
        if key in self.in_memory_cache:
            return self.in_memory_cache[key]

        self._ensure_parent_dir_exists()
        with shelve.open(self.filename) as s:
            if key in s:
                value = s[key]
                self.in_memory_cache[key] = value
                return value
            else:
                return default

    def put(self, key: str, value: V):
        self._ensure_parent_dir_exists()
        with shelve.open(self.filename) as s:
            if key in s:
                assert s[key] == value
            else:
                s[key] = value
                self.in_memory_cache[key] = value


@dataclass
class DatasetVersionFile:
    name: str
    metadata: Dict[str, str]
    gs_path: Optional[str]
    datafile_id: str
    format: str


@dataclass
class DatasetVersion:
    permanames: List[str]
    version_number: int
    version_id: str
    description: str
    files: List[DatasetVersionFile]

    @property
    def permaname(self):
        return self.permanames[0]


@dataclass
class File:
    name: str
    metadata: Dict[str, str]


@dataclass
class UploadedFile(File):
    local_path: str
    format: str
    encoding: str = "utf8"


@dataclass
class TaigaReference(File):
    taiga_id: str


@dataclass
class GCSReference(File):
    gs_path: str


import uuid
from typing import Callable, Tuple
from .types import DatasetVersionMetadataDict

Uploader = Callable[[str], Tuple[str, str]]


def create_s3_uploader(api: TaigaApi) -> Tuple[str, Uploader]:
    upload_session_id = api.create_upload_session()
    s3_credentials = api.get_s3_credentials()

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=s3_credentials.access_key_id,
        aws_secret_access_key=s3_credentials.secret_access_key,
        aws_session_token=s3_credentials.session_token,
    )

    def upload(local_path):
        bucket = s3_credentials.bucket
        partial_prefix = s3_credentials.prefix
        key = f"{partial_prefix}{upload_session_id}/{uuid.uuid4().hex}"
        s3_client.upload_file(local_path, bucket, key)
        print("Finished uploading {} to S3".format(local_path))

        return bucket, key

    return upload_session_id, upload


# type_to_structure = {
#     "HDF5": Structure.MATRIX,
#     "Columnar": Structure.TABLE,
#     "Raw": Structure.UNSTRUCTURED
# }

@dataclass
class MinDataFileMetadata:
    type: str
    metadata : Dict[str, str]

    @property
    def structure(self):
        return type_to_structure[self.type]

class Client:
    def __init__(self, cache_dir: str, api: TaigaApi):
        self.canonical_id_cache: Cache[str, str] = Cache(
            os.path.join(cache_dir, "canonical_id.cache")
        )
        self.internal_format_cache: Cache[str, str] = Cache(
            os.path.join(cache_dir, "internal_format.cache")
        )
        # todo: define MinDataFileMetadata (Do not use DataFileMetadata because it's too much work)
        self.datafile_metadata_cache: Cache[str, MinDataFileMetadata] = Cache(
            os.path.join(cache_dir, "datafile_metadata.cache")
        )
        self.download_cache_dir = os.path.join(cache_dir, "downloaded")
        self.api = api

    def _add_file_to_cache(
        self, full_metadata: DatasetVersionMetadataDict, data_file_metadata_dict
    ):
        canonical_id = data_file_metadata_dict["underlying_file_id"]
        datafile_id = data_file_metadata_dict["id"]
        if canonical_id is None:
            canonical_id = datafile_id
        assert re.match(DATAFILE_ID_PATTERN, datafile_id)
        print(f"adding {datafile_id} -> {canonical_id}")
        print(f"adding {canonical_id} -> {canonical_id}")
        self.canonical_id_cache.put(datafile_id, canonical_id)
        self.canonical_id_cache.put(canonical_id, canonical_id)

        # data_file_metadata_dict = dict(data_file_metadata_dict)
        # data_file_metadata_dict["dataset_name"] = full_metadata["dataset"]["name"]
        # data_file_metadata_dict["dataset_permaname"] = full_metadata["dataset"][
        #     "permanames"
        # ][0]
        # data_file_metadata_dict["dataset_version"] = full_metadata["datasetVersion"][
        #     "version"
        # ]
        # data_file_metadata_dict["dataset_id"] = full_metadata["dataset"]["permanames"][
        #     0
        # ]
        # data_file_metadata_dict["dataset_version_id"] = "...."
        # data_file_metadata_dict["datafile_name"] = data_file_metadata_dict["name"]
        # data_file_metadata_dict["status"] = "ok"
        # data_file_metadata_dict["state"] = full_metadata["datasetVersion"]["state"]

        self.datafile_metadata_cache.put(
            datafile_id, MinDataFileMetadata(
                type=data_file_metadata_dict["type"],
                metadata=data_file_metadata_dict["metadata"])
        )

    def _ensure_dataset_version_cached(self, permaname, version) -> bool:
        "returns False if this dataset version could not be found"
        full_metadata = self.api.get_dataset_version_metadata(
            dataset_permaname=permaname, dataset_version=version
        )
        if full_metadata is None:
            return False
        else:
            for file in full_metadata["datasetVersion"]["datafiles"]:
                self._add_file_to_cache(full_metadata, file)
            return True

    def get_canonical_id(self, datafile_id: str) -> str:
        """
        Given a taiga ID for a data file, resolves the ID to the "canonical ID". (That is to say, the ID of the data file that was
        originally uploaded to Taiga. Useful for comparing two Taiga IDs and determining whether they point to the same file or not.)
        """
        parsed_datafile_id = _parse_datafile_id(datafile_id)
        assert (
            parsed_datafile_id
        ), f"{datafile_id} doesn't look like a well qualified taiga datafile ID"

        canonical_id = self.canonical_id_cache.get(datafile_id, None)
        if canonical_id is None:
            self._ensure_dataset_version_cached(
                parsed_datafile_id.permaname, parsed_datafile_id.version
            )
            canonical_id = self.canonical_id_cache.get(datafile_id, None)
            assert canonical_id is not None

        return canonical_id

    def get(self, datafile_id: str) -> pd.DataFrame:
        """
        Retrieve the specified file as a pandas.Dataframe
        """
        canonical_id = self.get_canonical_id(datafile_id)
        if canonical_id is None:
            return None

        taiga_format = self._get_taiga_storage_format(canonical_id)
        if taiga_format in (TaigaStorageFormat.HDF5_MATRIX, TaigaStorageFormat.RAW_HDF5_MATRIX):
            path = self.download_to_cache(datafile_id, LocalFormat.HDF5_MATRIX)
            result = read_hdf5(path)
        elif taiga_format in (TaigaStorageFormat.CSV_TABLE, TaigaStorageFormat.RAW_PARQUET_TABLE):
            path = self.download_to_cache(datafile_id, LocalFormat.PARQUET_TABLE)
            result = read_parquet(path)
        else:
            raise ValueError(
                f"Datafile is neither a table nor matrix, but was: {taiga_format}"
            )

        return result

    def download_to_cache(self, datafile_id: str, requested_format: LocalFormat) -> str:
        """
        Download the specified file to the cache directory (if not already there and converting if necessary) and return the path to that file.
        """
        canonical_id = self.get_canonical_id(datafile_id)
        key = repr((canonical_id, format))
        path = self.internal_format_cache.get(key, None)
        if path:
            return path

        taiga_format = self._get_taiga_storage_format(canonical_id)
        if requested_format == LocalFormat.HDF5_MATRIX:
            if taiga_format == TaigaStorageFormat.HDF5_MATRIX or taiga_format == TaigaStorageFormat.RAW_HDF5_MATRIX:
                local_path = self._download_to_cache(canonical_id)
            else:
                raise Exception(f"Requested {requested_format} but taiga_format={taiga_format}")
        elif requested_format == LocalFormat.PARQUET_TABLE:
            if taiga_format == TaigaStorageFormat.CSV_TABLE:
                csv_path = self._download_to_cache(canonical_id)
                local_path = self._get_unique_name(canonical_id, ".parquet")
                convert_csv_to_parquet(csv_path, local_path)
            elif taiga_format == TaigaStorageFormat.RAW_PARQUET_TABLE:
                local_path = self._download_to_cache(canonical_id)
            else:
                raise Exception(f"Requested {requested_format} but taiga_format={taiga_format}")
        else:
            raise Exception(f"Requested {requested_format} but taiga_format={taiga_format}")

        self.internal_format_cache.put(key, local_path)
        assert local_path is not None
        return local_path

    def _upload_files(self, all_uploads: List[File]) -> str:
        upload_session_id, uploader = create_s3_uploader(self.api)

        for upload in all_uploads:
            self._upload_file(upload_session_id, uploader, upload)

        return upload_session_id

    def _upload_file(self, upload_session_id, uploader: Uploader, upload: File):
        def _upload_uploaded_file(upload_file: UploadedFile):
            bucket, key = uploader(upload_file.local_path)

            print("Uploading {} to Taiga".format(upload_file.local_path))
            self.api.upload_file_to_taiga(
                upload_session_id,
                {
                    "filename": upload_file.name,
                    "filetype": "s3",
                    "s3Upload": {
                        "format": upload_file.format,
                        "bucket": bucket,
                        "key": key,
                        "encoding": upload_file.encoding,
                    },
                    "metadata": upload_file.metadata,
                },
            )

            print("Finished uploading {} to Taiga".format(upload_file.local_path))

        def _upload_taiga_reference(file: TaigaReference):
            print(f"Linking virtual file {file.taiga_id} -> {file.name}")
            self.api.upload_file_to_taiga(
                upload_session_id,
                {
                    "filename": upload.name,
                    "filetype": "virtual",
                    "existingTaigaId": upload.taiga_id,
                },
            )

        if isinstance(upload, UploadedFile):
            _upload_uploaded_file(upload)
        elif isinstance(upload, TaigaReference):
            _upload_taiga_reference(upload)
        else:
            raise Exception(f"Unknown upload type: {type(upload)}")

    def create_dataset(
        self,
        name: str,
        description: str,
        files: List[File],
        folder_id: Optional[str] = None,
    ) -> DatasetVersion:
        if folder_id is None:
            folder_id = self.api.get_user()["home_folder_id"]

        upload_session_id = self._upload_files(files)

        dataset_id = self.api.create_dataset(
            upload_session_id, folder_id, name, description
        )

        print(
            cf.green(
                f"Dataset created. Access it directly with this url: {self.api.url}/dataset/{dataset_id}\n"
            )
        )

        return dataset_id

    def replace_dataset(
        self, permaname: str, description: str, files: List[File], reason: str
    ) -> DatasetVersion:
        raise NotImplementedError()

    def update_dataset(
        self,
        permaname: str,
        reason: str,
        description: Optional[str] = None,
        additions: List[File] = [],
        removals: List[str] = [],
    ) -> DatasetVersion:
        raise NotImplementedError()

    def _download_to_cache(self, datafile_id: str) -> str:
        canonical_id = self.get_canonical_id(datafile_id)
        dest = self._get_unique_name(canonical_id, ".raw")
        parsed = _parse_datafile_id(datafile_id)
        self.api.download_datafile(
            parsed.permaname, parsed.version, parsed.name, dest,
        )
        return dest

    def _get_unique_name(self, prefix, suffix):
        prefix = re.sub("[^a-z0-9]+", "-", prefix.lower())
        if not os.path.exists(self.download_cache_dir):
            os.makedirs(self.download_cache_dir)
        fd = tempfile.NamedTemporaryFile(
            mode="w",
            delete=False,
            prefix=prefix,
            suffix=suffix,
            dir=self.download_cache_dir,
        )
        fd.close()
        return fd.name

    def _get_taiga_storage_format(self, datafile_id: str) -> TaigaStorageFormat:
        metadata = self.get_datafile_metadata(datafile_id)
        assert metadata
        if metadata.type == "HDF5":
            return TaigaStorageFormat.HDF5_MATRIX
        if metadata.type == "Columnar":
            return TaigaStorageFormat.CSV_TABLE
        if metadata.type == "Raw":
            value = metadata.metadata.get("client_storage_format", TaigaStorageFormat.RAW_BYTES)
            return TaigaStorageFormat(value)
        else:
            raise Exception(f"unknown type: {metadata.type}")

    def get_datafile_metadata(self, datafile_id: str) -> Dict[str, str]:
        file = self._get_full_taiga_datafile_metadata(datafile_id)
        return file

    def _get_full_taiga_datafile_metadata(
        self, datafile_id
    ) -> Optional[MinDataFileMetadata]:
        parsed_datafile_id = _parse_datafile_id(datafile_id)
        found = self._ensure_dataset_version_cached(
            parsed_datafile_id.permaname, parsed_datafile_id.version
        )
        if not found:
            return None

        return self.datafile_metadata_cache.get(datafile_id, None)


# New caches:
# Dataset version id → DatasetVersion
# (data file id, format) → filepath

# UploadedFile:
#   local_path
#   name
#   format
#   metadata

# TaigaReference:
#    taiga_id
#    name
#    metadata

# GCSReference
#    gs_path
#    name
#    metadata

# File = Union[UploadedFile | TaigaReference | GCSReference]

# get(datafile_id) → pandas data frame
#   If the file is "raw" and the file has a "format" field download to cache and parse file
# otherwise delete to legacy taiga client get

# download_to_cache(datafile_id, format) → filename

# create_dataset(name, folder, description, files)
# replace_dataset(dataset_id, description, files, reason) → DatasetVersion
# update_dataset(dataset_id, description, additions, removals, reason) → DatasetVersion

# # note when adding a TaigaReference to dataset, the metadata should be a copy of the targeted metadata + the metadata in TaigaReference. The client should take care of retrieving the metadata from the source and doing the merge

# get_canonical_taiga_id(datafile_id) → str
# get_dataset_metadata( dataset_id ) → Dataset
# get_version_metadata( version_id ) → DatasetVersion
# get_datafile_metadata( id ) → dict


################
import h5py
import numpy as np

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
        dest.create_dataset("data", track_times=False, data=df.values)
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
    return pd.read_parquet(filename)


def convert_csv_to_hdf5(csv_path: str, hdf5_path: str):
    df = pd.read_csv(csv_path, index_col=0)
    write_hdf5(df, hdf5_path)


def convert_csv_to_parquet(csv_path: str, parquet_path: str):
    df = df.read_csv(csv_path)
    write_parquet(df, parquet_path)


################

import pytest

sample_matrix = pd.DataFrame(data={"a": [1.2, 2.0], "b": [2.1, 3.0]}, index=["x", "y"])
sample_table = pd.DataFrame(data={"a": [1.2, 2.0], "b": [2.1, 3.0]})


writers_by_format = {
    LocalFormat.HDF5_MATRIX.value: write_hdf5,
    LocalFormat.PARQUET_TABLE.value: write_parquet,
    LocalFormat.CSV_TABLE.value: lambda df, dest: df.to_csv(dest, index=False),
    LocalFormat.CSV_MATRIX.value: lambda df, dest: df.to_csv(dest, index=True),
}

from unittest.mock import create_autospec


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

    sessions = {}
    dataset_versions: Dict[str, DatasetVersion] = {}
    s3_objects = {}
    datafiles_by_id = {}

    def _upload_file(local_path, bucket, key):
        with open(local_path, "rb") as fd:
            bytes = fd.read()
        s3_objects[f"{bucket}/{key}"] = bytes
    s3_mock_client.upload_file.side_effect = _upload_file

    from typing import Union
    from .types import DatasetMetadataDict, DatasetVersionMetadataDict

    def _get_dataset_version_metadata(
        dataset_permaname: str, dataset_version: Optional[str]
    ) -> Union[DatasetMetadataDict, DatasetVersionMetadataDict]:
        version = dataset_version
        dataset_version = dataset_versions.get(f"{dataset_permaname}.{version}")
        if dataset_version is None:
            return None

        assert version is not None
        return {
            "dataset": {"name": "name", "permanames": [dataset_permaname]},
            "datasetVersion": {
                "can_edit": True,
                "can_view": True,
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
                        "original_file_md5": None,
                        "original_file_sha256": None,
                        "metadata": file.metadata
                    }
                    for file in dataset_version.files
                ],
                "dataset_id": str,
                "description": str,
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
            if f["s3Upload"]["format"] == "csv_matrix":
                format = "HDF5"
                # simulate conversion
                f = dict(f)
                f["s3Upload"]["key"] = uuid.uuid4().hex
                with tempfile.NamedTemporaryFile(mode="wb") as fd:
                    s3_objects
                    convert_csv_to_hdf5(csv_bytes, fd.name)
                    fd.seek(0)
                    hdf5_bytes = fd.read()
                s3_objects[f["s3Upload"]["key"]] = hdf5_bytes
            elif f["s3Upload"]["format"] == "csv_table":
                format = "Columnar"
            else:
                assert f["s3Upload"]["format"] == "raw"
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
        bytes = s3_objects[f["s3Upload"]["bucket"]+"/"+f["s3Upload"]["key"]]
        with open(dest, "wb") as fd:
            fd.write(bytes)
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


def test_upload_hdf5(
    mock_client: Client, tmpdir,  s3_mock_client
):
    _test_upload_hdf5(mock_client, tmpdir, sample_matrix, LocalFormat.CSV_MATRIX.value, s3_mock_client)

@pytest.mark.parametrize(
    "df,format",
    [
        (sample_matrix, LocalFormat.HDF5_MATRIX.value),
        (sample_table, LocalFormat.PARQUET_TABLE.value),
        (sample_matrix, LocalFormat.CSV_MATRIX.value),
        (sample_table, LocalFormat.CSV_TABLE.value),
    ],
)
def _test_upload_hdf5(
    mock_client: Client, tmpdir, df: pd.DataFrame, format: str, s3_mock_client
):
    sample_file = tmpdir.join("file")
    writers_by_format[format](df, str(sample_file))

    metadata = {}
    if format == LocalFormat.HDF5_MATRIX.value:
        metadata["client_storage_format"] = TaigaStorageFormat.RAW_HDF5_MATRIX.value
    elif format == LocalFormat.PARQUET_TABLE.value:
        metadata["client_storage_format"] = TaigaStorageFormat.RAW_PARQUET_TABLE.value

    version = mock_client.create_dataset(
        "test",
        "desc",
        [
            UploadedFile(
                name="matrix", metadata=metadata, local_path=str(sample_file), format=format
            )
        ],
    )
    assert len(version.files) == 1
    file = version.files[0]

    fetched_df = mock_client.get(file.datafile_id)

    assert df.equals(fetched_df)
