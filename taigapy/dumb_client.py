import pandas as pd
from typing import Optional, Dict, List
import os
from .taiga_api import TaigaApi
import re
from enum import Enum, auto
import tempfile
from dataclasses import dataclass
import boto3
import colorful as cf
import uuid
from typing import Callable, Tuple
from .types import DatasetVersionMetadataDict
from .simple_cache import Cache
from .types import DataFileUploadFormat
from .format_utils import read_hdf5, read_parquet, convert_csv_to_parquet

# from taigapy.types import (
#    S3Credentials
# )

# the different formats that we might store files as locally
class LocalFormat(Enum):
    HDF5_MATRIX = "hdf5_matrix"
    PARQUET_TABLE = "parquet_table"
    CSV_TABLE = "csv_table"
    CSV_MATRIX = "csv_matrix"


# the different formats files might be stored in on Taiga.
class TaigaStorageFormat(Enum):
    CSV_TABLE = "csv_table"
    HDF5_MATRIX = "hdf5_matrix"
    RAW_HDF5_MATRIX = "raw_hdf5_matrix"
    RAW_PARQUET_TABLE = "raw_parquet_table"
    RAW_BYTES = "raw_bytes"


@dataclass
class DataFileID:
    permaname: str
    version: int
    name: str


DATAFILE_ID_PATTERN = "([a-z0-9-]+)\\.([0-9]+)/(.*)"


def _parse_datafile_id(datafile_id):
    "Split a datafile id into its components"
    m = re.match(DATAFILE_ID_PATTERN, datafile_id)
    if m:
        permname, version, name = m.groups()
        return DataFileID(permname, int(version), name)
    else:
        return None


@dataclass
class DatasetVersionFile:
    name: str
    custom_metadata: Dict[str, str]
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
    """
    Base class for different types of files you can upload. Never use 
    this class directly, but instead create an instance of UploadedFile, TaigaReference or 
    GCSReference depending on the type of file.
    """

    name: str
    custom_metadata: Dict[str, str]


@dataclass
class UploadedFile(File):
    local_path: str
    format: LocalFormat
    encoding: str = "utf8"


@dataclass
class TaigaReference(File):
    taiga_id: str


@dataclass
class GCSReference(File):
    gs_path: str


Uploader = Callable[[str], Tuple[str, str]]


def _create_s3_uploader(api: TaigaApi) -> Tuple[str, Uploader]:
    "Used to encapsulate the logic for uploading to s3"
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


@dataclass
class MinDataFileMetadata:
    type: str
    custom_metadata: Dict[str, str]


class Client:
    def __init__(self, cache_dir: str, api: TaigaApi):
        assert api is not None
        assert cache_dir is not None

        # caches canonical ID for each Taiga ID
        self.canonical_id_cache: Cache[str, str] = Cache(
            os.path.join(cache_dir, "canonical_id.cache")
        )

        # caches local path to file for each (canonical_id, format)
        self.internal_format_cache: Cache[str, str] = Cache(
            os.path.join(cache_dir, "internal_format.cache")
        )

        # caches min datafile metadata given a (non canonical) datafile ID
        self.datafile_metadata_cache: Cache[str, MinDataFileMetadata] = Cache(
            os.path.join(cache_dir, "dataset_version.cache")
        )

        # caches dataset version metadata given a dataset version ID
        self.dataset_version_cache: Cache[str, DatasetVersionMetadataDict] = Cache(
            os.path.join(cache_dir, "datafile_metadata.cache")
        )

        # path to where to store downloaded files
        self.download_cache_dir = os.path.join(cache_dir, "downloaded")
        self.api = api

    def _add_file_to_cache(self, permaname, version, data_file_metadata_dict):
        canonical_id = data_file_metadata_dict.get("underlying_file_id")
        datafile_id = f"{permaname}.{version}/{ data_file_metadata_dict['name'] }"
        if canonical_id is None:
            canonical_id = datafile_id
        assert re.match(
            DATAFILE_ID_PATTERN, datafile_id
        ), f"{repr(datafile_id)} does not look like a datafile ID"
        self.canonical_id_cache.put(datafile_id, canonical_id)
        self.canonical_id_cache.put(canonical_id, canonical_id)

        self.datafile_metadata_cache.put(
            datafile_id,
            MinDataFileMetadata(
                type=data_file_metadata_dict["type"],
                custom_metadata=data_file_metadata_dict["custom_metadata"],
            ),
        )

    def _ensure_dataset_version_cached(self, permaname: str, version: int) -> bool:
        "returns False if this dataset version could not be found"
        key = f"{permaname}.{version}"
        dataset_version = self.dataset_version_cache.get(key, None)
        if dataset_version is None:
            full_metadata = self.api.get_dataset_version_metadata(
                dataset_permaname=permaname, dataset_version=version
            )
            if full_metadata is None:
                return False
            else:
                for file in full_metadata["datasetVersion"]["datafiles"]:
                    self._add_file_to_cache(
                        full_metadata["dataset"]["permanames"][0],
                        full_metadata["datasetVersion"]["version"],
                        file,
                    )
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
        if taiga_format in (
            TaigaStorageFormat.HDF5_MATRIX,
            TaigaStorageFormat.RAW_HDF5_MATRIX,
        ):
            path = self.download_to_cache(datafile_id, LocalFormat.HDF5_MATRIX)
            result = read_hdf5(path)
        elif taiga_format in (
            TaigaStorageFormat.CSV_TABLE,
            TaigaStorageFormat.RAW_PARQUET_TABLE,
        ):
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
            if (
                taiga_format == TaigaStorageFormat.HDF5_MATRIX
                or taiga_format == TaigaStorageFormat.RAW_HDF5_MATRIX
            ):
                local_path = self._download_to_cache(canonical_id)
            else:
                raise Exception(
                    f"Requested {requested_format} but taiga_format={taiga_format}"
                )
        elif requested_format == LocalFormat.PARQUET_TABLE:
            if taiga_format == TaigaStorageFormat.CSV_TABLE:
                csv_path = self._download_to_cache(canonical_id)
                local_path = self._get_unique_name(canonical_id, ".parquet")
                convert_csv_to_parquet(csv_path, local_path)
            elif taiga_format == TaigaStorageFormat.RAW_PARQUET_TABLE:
                local_path = self._download_to_cache(canonical_id)
            else:
                raise Exception(
                    f"Requested {requested_format} but taiga_format={taiga_format}"
                )
        else:
            raise Exception(
                f"Requested {requested_format} but taiga_format={taiga_format}"
            )

        self.internal_format_cache.put(key, local_path)
        assert local_path is not None
        return local_path

    def _upload_files(self, all_uploads: List[File]) -> str:
        upload_session_id, uploader = _create_s3_uploader(self.api)

        for upload in all_uploads:
            self._upload_file(upload_session_id, uploader, upload)

        return upload_session_id

    def _upload_file(self, upload_session_id, uploader: Uploader, upload: File):
        # one method for each type of file we can upload
        def _upload_uploaded_file(upload_file: UploadedFile):
            bucket, key = uploader(upload_file.local_path)

            print("Uploading {} to Taiga".format(upload_file.local_path))
            custom_metadata = dict(upload_file.custom_metadata)
            if upload_file.format == LocalFormat.CSV_MATRIX:
                taiga_format = DataFileUploadFormat.NumericMatrixCSV
            elif upload_file.format == LocalFormat.CSV_TABLE:
                taiga_format = DataFileUploadFormat.TableCSV
            elif upload_file.format == LocalFormat.HDF5_MATRIX:
                taiga_format = DataFileUploadFormat.Raw
                custom_metadata[
                    "client_storage_format"
                ] = TaigaStorageFormat.RAW_HDF5_MATRIX.value
            elif upload_file.format == LocalFormat.PARQUET_TABLE:
                taiga_format = DataFileUploadFormat.Raw
                custom_metadata[
                    "client_storage_format"
                ] = TaigaStorageFormat.RAW_PARQUET_TABLE.value
            elif upload_file.format == LocalFormat.RAW:
                taiga_format = DataFileUploadFormat.Raw
            else:
                raise Exception(f"Unknown format: {upload_file.format}")

            self.api.upload_file_to_taiga(
                upload_session_id,
                {
                    "filename": upload_file.name,
                    "filetype": "s3",
                    "s3Upload": {
                        "format": taiga_format.value,
                        "bucket": bucket,
                        "key": key,
                        "encoding": upload_file.encoding,
                    },
                    "custom_metadata": custom_metadata,
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
        """
        Create a new dataset given a list of files.
        """
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

        # TODO: This is the wrong return type
        return dataset_id

    def replace_dataset(
        self, permaname: str, description: str, files: List[File], reason: str
    ) -> DatasetVersion:
        """
        Update an existing dataset by replacing all datafiles with the ones provided (Results in a new dataset version)
        """
        raise NotImplementedError()

    def update_dataset(
        self,
        permaname: str,
        reason: str,
        description: Optional[str] = None,
        additions: List[File] = [],
        removals: List[str] = [],
    ) -> DatasetVersion:
        """
        Update an existing dataset by adding and removing the specified files. (Results in a new dataset version)
        """
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
            value = metadata.custom_metadata.get(
                "client_storage_format", TaigaStorageFormat.RAW_BYTES
            )
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
