import pandas as pd
from typing import Optional, Dict, List, Any
import os

from taigapy.custom_exceptions import Taiga404Exception, TaigaHttpException
from .taiga_api import TaigaApi
import re
from enum import Enum, auto
import tempfile
from dataclasses import dataclass
import boto3
import colorful as cf
import uuid
from typing import Callable, Tuple
from .types import DatasetMetadataDict, DatasetVersionMetadataDict
from .simple_cache import Cache
from .types import DataFileUploadFormat
from .format_utils import read_hdf5, read_parquet, convert_csv_to_parquet
from taigapy.utils import get_latest_valid_version_from_metadata
from typing import Union
from google.cloud import storage, exceptions as gcs_exceptions
from . import utils

class LocalFormat(Enum):
    """
    The various different formats that we can use with taigapy. These are formats that
    can be uploaded to Taiga, or when we download from taiga we can request the file
    be stored in this format.
    """
    HDF5_MATRIX = "hdf5_matrix"
    PARQUET_TABLE = "parquet_table"
    CSV_TABLE = "csv_table"
    CSV_MATRIX = "csv_matrix"
    RAW = "raw"
    FEATHER_TABLE = "feather_table"
    FEATHER_MATRIX = "feather_matrix"


class TaigaStorageFormat(Enum):
    """
    The different ways files are encoded when they are uploaded to Taiga. Users of the client
    should not need to be aware of these.
    """
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

@dataclass
class DatasetVersionID:
    permaname: str
    version: int


DATAFILE_ID_PATTERN = "^([a-z0-9-]+)\\.([0-9]+)/(.*)$"
DATASET_VERSION_ID_PATTERN = "^([a-z0-9-]+)\\.([0-9]+)$"

def _parse_dataset_version_id(dataset_id):
    "Split a dataset id into its components"
    m = re.match(DATASET_VERSION_ID_PATTERN, dataset_id)
    if m:
        permname, version = m.groups()
        return DatasetVersionID(permname, int(version))
    else:
        return None

def _parse_datafile_id(datafile_id):
    "Split a datafile id into its components"
    m = re.match(DATAFILE_ID_PATTERN, datafile_id)
    if m:
        permname, version, name = m.groups()
        return DataFileID(permname, int(version), name)
    else:
        return None

def _filter_out_matching_files(tc, permaname, additions):
    # fetch the metadata about the latest version
    latest_id = tc.get_latest_version_id(permaname)
    permaname, version = latest_id.split(".")
    dvm = tc.get_dataset_metadata(permaname, version)
    by_name = {f['name'] : f for f in dvm['datasetVersion']['datafiles']}

    filtered = []
    for addition in additions:
        skip = False
        if isinstance(addition, UploadedFile):
            name = addition.name
            existing_file = by_name.get(name)
            if existing_file:
                existing_sha256 = existing_file.get("original_file_sha256")
                if existing_sha256:
                    sha256, md5 = utils.get_file_hashes(addition.local_path)
                    if sha256 == existing_sha256:
                        skip = True

        if not skip:
            filtered.append(addition)

    return latest_id, filtered

@dataclass
class DatasetVersionFile:
    name: str
    custom_metadata: Dict[str, Any]
    gs_path: Optional[str]
    datafile_id: str
    format: str


@dataclass
class DatasetVersion:
    permanames: List[str]
    version_number: int
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

    def __init__(self, name: str, custom_metadata={}):
        self.name = name
        self.custom_metadata = custom_metadata


@dataclass
class UploadedFile(File):
    local_path: str
    format: LocalFormat
    encoding: str = "utf8"

    def __init__(
        self,
        name: str,
        local_path: str,
        format: LocalFormat,
        encoding="utf8",
        custom_metadata={},
    ):
        super(UploadedFile, self).__init__(name, custom_metadata)
        self.local_path = local_path
        self.format = format
        self.encoding = encoding


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
        print(f"Finished uploading {local_path} to S3")

        return bucket, key

    return upload_session_id, upload


@dataclass
class MinDataFileMetadata:
    type: str
    custom_metadata: Dict[str, str]
    original_file_sha256: str


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
            os.path.join(cache_dir, "internal_format.cache"),
            is_value_valid=lambda filename: os.path.exists(filename),
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

    def _add_file_to_cache(self, permaname, version, data_file_metadata_dict, was_single_file):
        original_file_sha256 = data_file_metadata_dict.get('original_file_sha256')
        canonical_id = data_file_metadata_dict.get("underlying_file_id")
        datafile_id = f"{permaname}.{version}/{ data_file_metadata_dict['name'] }"
        if canonical_id is None:
            canonical_id = datafile_id
        assert re.match(
            DATAFILE_ID_PATTERN, datafile_id
        ), f"{repr(datafile_id)} does not look like a datafile ID"
        self.canonical_id_cache.put(datafile_id, canonical_id)
        self.canonical_id_cache.put(canonical_id, canonical_id)
        if was_single_file:
            # special case: if this is the only file in the dataset version, we can also use an ID without
            # the filename suffix
            self.canonical_id_cache.put(f"{permaname}.{version}", canonical_id)

        self.datafile_metadata_cache.put(
            datafile_id,
            MinDataFileMetadata(
                type=data_file_metadata_dict["type"],
                custom_metadata=data_file_metadata_dict["custom_metadata"],
                original_file_sha256=original_file_sha256
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
                        was_single_file=len(full_metadata["datasetVersion"]["datafiles"]) == 1
                    )
        return True

    def _get_file_storage_type(
        self, metadata: MinDataFileMetadata
    ) -> TaigaStorageFormat:
        if metadata.type == "HDF5":
            return TaigaStorageFormat.HDF5_MATRIX
        if metadata.type == "Columnar":
            return TaigaStorageFormat.CSV_TABLE
        if metadata.type == "Raw":
            # Refactored into separate function due to needing this in 2 places. The following
            # 2 comments were originally added by Jessica.
            # NOTE: Upload files with HDF5_MATRIX format is stored in taiga as Raw format but its custom_metadata dict 'client_storage_format' key holds info about its format as RAW_HDF5_MATRIX (see _upload_uploaded_file()). This is confusing and it would be nice if client and api data types match. Also, it's possible for custom_metadata to somehow be None so need to account for that too...
            # TODO: Figure out how custom_metadata can be None. My guess is uploads that used old client?
            value = (
                metadata.custom_metadata.get(
                    "client_storage_format", TaigaStorageFormat.RAW_BYTES
                )
                if metadata.custom_metadata
                else TaigaStorageFormat.RAW_BYTES
            )
            return TaigaStorageFormat(value)
        else:
            raise Exception(f"unknown type: {metadata.type}")

    def get_latest_version_id(self, permaname):
        """
        Given a permaname, get the ID of the latest version of that dataset
        """
        metadata = self.get_dataset_metadata(permaname)
        if metadata is None:
            return None
        last_version = max(int(x["name"]) for x in metadata['versions'])
        return f"{permaname}.{last_version}"
    
    def get_canonical_id(self, datafile_id: str, only_use_cache=False) -> str:
        """
        Given a taiga ID for a data file, resolves the ID to the "canonical ID". (That is to say, the ID of the data file that was
        originally uploaded to Taiga. Useful for comparing two Taiga IDs and determining whether they point to the same file or not.)
        """
        canonical_id = self.canonical_id_cache.get(datafile_id, None)
        if canonical_id is None:
            parsed = _parse_datafile_id(datafile_id)

            if parsed is None:
                # old Taiga IDs had the filename as optional. We have a special case where 
                # if a filename is missing _and_ the dataset has only one file then the ID refers
                # to that file. Try seeing if this case applies

                parsed = _parse_dataset_version_id(datafile_id)
                assert (
                    parsed
                ), f"{datafile_id} doesn't look like a well qualified taiga datafile ID"

            if not only_use_cache:
                self._ensure_dataset_version_cached(
                    parsed.permaname, parsed.version
                )
            # the process of caching the dataversion also populates the canonical_id cache
            canonical_id = self.canonical_id_cache.get(datafile_id, None)
            assert canonical_id is not None

        return canonical_id

    def get(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
        version: Optional[DatasetVersion] = None,
        file: Optional[str] = None,
    ) -> pd.DataFrame:
        only_use_cache = not self.api.is_connected()
        if only_use_cache:
            print(
                cf.orange(
                    "You are in offline mode, please be aware that you might be out of sync with the state of the dataset version (deprecation)."
                )
            )
            if id is None and version is None:
                print(cf.red("Dataset version must be specified"))
                return None

        if id is None:
            assert name is not None
            assert file is not None

            # Handle case where people want the latest version. This only works
            # in online mode.
            if version is None and not only_use_cache:
                dataset_metadata = self.api.get_dataset_version_metadata(name, None)
                version = get_latest_valid_version_from_metadata(dataset_metadata)
                print(
                    cf.orange(
                        "No dataset version provided. Using version {}.".format(version)
                    )
                )

            id = f"{name}.{version}/{file}"

        assert (
            re.match("[a-z0-9-]+\\.\\d+/.*", id) is not None
        ), f"expected {id} to be of the form permaname.version/filename"

        try:
            return self._get(id, only_use_cache=only_use_cache)
        except Exception as ex:
            raise Exception(
                f"Got an internal error when trying to get({repr(id)})"
            ) from ex

    def _get(self, datafile_id: str, only_use_cache=False) -> pd.DataFrame:
        """
        Retrieve the specified file as a pandas.Dataframe
        """
        canonical_id = self.get_canonical_id(datafile_id, only_use_cache=only_use_cache)

        if canonical_id is None:
            return None

        if only_use_cache:
            metadata = self.datafile_metadata_cache.get(datafile_id, None)

        taiga_format = (
            self._get_file_storage_type(metadata)
            if only_use_cache
            else self._get_taiga_storage_format(canonical_id)
        )

        if taiga_format in (
            TaigaStorageFormat.HDF5_MATRIX,
            TaigaStorageFormat.RAW_HDF5_MATRIX,
        ):
            path = self.download_to_cache(
                datafile_id, LocalFormat.HDF5_MATRIX, only_use_cache=only_use_cache
            )
            result = read_hdf5(path)
        elif taiga_format in (
            TaigaStorageFormat.CSV_TABLE,
            TaigaStorageFormat.RAW_PARQUET_TABLE,
        ):
            path = self.download_to_cache(
                datafile_id, LocalFormat.PARQUET_TABLE, only_use_cache=only_use_cache
            )
            result = read_parquet(path)
        else:
            raise ValueError(
                f"Datafile is neither a table nor matrix, but was: {taiga_format}"
            )

        return result

    def upload_to_gcs(
        self,
        data_file_taiga_id: str,
        requested_format: LocalFormat,
        dest_gcs_path_for_file: str,
    ) -> bool:
        """Upload a Taiga datafile to a specified location in Google Cloud Storage.

        The service account taiga-892@cds-logging.iam.gserviceaccount.com must have
        storage.buckets.create access for this request.

        Arguments:
            `data_file_taiga_id` -- Taiga ID in the form dataset_permaname.dataset_version/datafile_name
            `requested_format` -- The format of the file you want to upload
            `dest_gcs_path_for_file` -- Google Storage path to upload to, in the form 'gs://bucket:file_obj_name'

        Returns:
            bool -- Whether the file was successfully uploaded
        """

        def parse_gcs_path(gcs_path: str) -> Tuple[str, str]:
            m = re.match("gs://([^/]+)/(.*)$", gcs_path)
            if not m:
                raise ValueError(
                    "Invalid GCS path. '{}' is not in the form 'gs://bucket_name/object_name'".format(
                        gcs_path
                    )
                )
            bucket_name, file_object_name = m.groups()
            return (
                bucket_name,
                file_object_name,
            )

        def get_bucket(bucket_name: str) -> storage.Bucket:
            client = storage.Client()
            try:
                bucket = client.get_bucket(bucket_name)
            except gcs_exceptions.Forbidden as e:
                raise e
            except gcs_exceptions.NotFound as e:
                raise ValueError("No GCS bucket found: {}".format(bucket_name))
            return bucket

        full_taiga_id = self.get_canonical_id(data_file_taiga_id)
        if full_taiga_id is None:
            return False
        print("Downloading file to cache...")
        datafile_path = self.download_to_cache(data_file_taiga_id, requested_format)
        print(f"Data file path from cache: {datafile_path}")

        dest_bucket_name, dest_file_object_name = parse_gcs_path(dest_gcs_path_for_file)
        print(f"Get GCS bucket: {dest_bucket_name} ...")
        bucket = get_bucket(dest_bucket_name)
        blob = bucket.blob(dest_file_object_name, chunk_size=1024 * 1024)

        try:
            print("Upload file to GCS ...")
            blob.upload_from_filename(datafile_path)
            return True
        except gcs_exceptions.Forbidden as e:
            raise e

    def get_allowed_local_formats(self, datafile_id):
        "Given a datafile_id, return which values of LocalFormat can be used with `download_to_cache` to retreive the data"
        canonical_id = self.get_canonical_id(datafile_id)
        taiga_format = self._get_taiga_storage_format(canonical_id)
        matrix_formats = [LocalFormat.HDF5_MATRIX, LocalFormat.CSV_MATRIX, LocalFormat.FEATHER_MATRIX]
        table_formats = [LocalFormat.PARQUET_TABLE, LocalFormat.CSV_TABLE, LocalFormat.FEATHER_TABLE]
        return {TaigaStorageFormat.HDF5_MATRIX: matrix_formats,
                TaigaStorageFormat.CSV_TABLE: table_formats,
                TaigaStorageFormat.RAW_BYTES: [LocalFormat.RAW],
                TaigaStorageFormat.RAW_PARQUET_TABLE: table_formats,
                TaigaStorageFormat.RAW_HDF5_MATRIX: matrix_formats}[taiga_format]

    def _dl_and_convert_hdf5_matrix(self, canonical_id, taiga_format, requested_format):
        if taiga_format in [
            TaigaStorageFormat.HDF5_MATRIX,
            TaigaStorageFormat.RAW_HDF5_MATRIX,
        ]:
            hdf5_path = self.download_to_cache(
                canonical_id, requested_format=LocalFormat.HDF5_MATRIX
            )
            # taiga client will convert from HDF5 to CSV
            df = read_hdf5(hdf5_path)
            if requested_format == LocalFormat.CSV_MATRIX:
                local_path = self._get_unique_name(canonical_id, ".csv")
                df.to_csv(local_path)
            else:
                assert requested_format == LocalFormat.FEATHER_MATRIX
                local_path = self._get_unique_name(canonical_id, ".ftr")
                df.reset_index(inplace=True)
                df.to_feather(local_path)
        else:
            raise Exception(
                f"Requested {requested_format} but taiga_format={taiga_format}"
            )
        return local_path

    def _dl_and_convert_csv_table(self, canonical_id, taiga_format, requested_format):
        if taiga_format == TaigaStorageFormat.CSV_TABLE:
            if requested_format == LocalFormat.CSV_TABLE:
                local_path = self._download_to_cache(canonical_id)
            else:
                assert requested_format == LocalFormat.FEATHER_TABLE
                local_csv = self.download_to_cache(canonical_id, requested_format=LocalFormat.PARQUET_TABLE)
                df = pd.read_parquet(local_csv)
                local_path = self._get_unique_name(canonical_id, ".ftr")
                df.to_feather(local_path)
        elif taiga_format == TaigaStorageFormat.RAW_PARQUET_TABLE:
            local_parqet_file = self._download_to_cache(canonical_id)
            df = pd.read_parquet(local_parqet_file)
            if requested_format == LocalFormat.CSV_TABLE:
                local_path = self._get_unique_name(canonical_id, ".csv")
                df.to_csv(local_path)
            else:
                assert requested_format == LocalFormat.FEATHER_TABLE
                local_path = self._get_unique_name(canonical_id, ".ftr")
                df.to_feather(local_path)        
        else:
            raise Exception(
                f"Requested {requested_format} but taiga_format={taiga_format}"
            )
        return local_path

    def download_to_cache(
        self,
        datafile_id: str,
        requested_format: Union[LocalFormat, str],
        only_use_cache=False,
    ) -> str:
        """
        Download the specified file to the cache directory (if not already there
        and converts if necessary) and return the path to that file.
        """
        # coerce to enum
        if isinstance(requested_format, str):
            requested_format = LocalFormat(requested_format)

        canonical_id = self.get_canonical_id(datafile_id, only_use_cache=only_use_cache)
        key = repr((canonical_id, requested_format))
        path = self.internal_format_cache.get(key, None)
        if path:
            return path

        assert not only_use_cache, f"Expected {key} to be cached, but it was not!"

        taiga_format = self._get_taiga_storage_format(canonical_id)
        if requested_format == LocalFormat.HDF5_MATRIX:
            if taiga_format == TaigaStorageFormat.HDF5_MATRIX:
                local_path = self._download_to_cache(canonical_id, format="hdf5")
            elif taiga_format == TaigaStorageFormat.RAW_HDF5_MATRIX:
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
        elif requested_format in [LocalFormat.CSV_MATRIX, LocalFormat.FEATHER_MATRIX]:
            local_path = self._dl_and_convert_hdf5_matrix(canonical_id, taiga_format, requested_format)
        elif requested_format in [LocalFormat.CSV_TABLE, LocalFormat.FEATHER_TABLE]:
            local_path = self._dl_and_convert_csv_table(canonical_id, taiga_format, requested_format)
        elif requested_format == LocalFormat.RAW:
            if taiga_format == TaigaStorageFormat.RAW_BYTES:
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
        print(f"Created temporary upload session {upload_session_id}")

        for upload in all_uploads:
            self._upload_file(upload_session_id, uploader, upload)

        return upload_session_id

    def _upload_file(self, upload_session_id, uploader: Uploader, upload: File):
        # one method for each type of file we can upload
        def _upload_uploaded_file(upload_file: UploadedFile):
            print(f"Uploading {upload_file.local_path} to S3")
            bucket, key = uploader(upload_file.local_path)
            print(f"Completed uploading {upload_file.local_path} to S3")

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
            print(
                f"Added {upload_file.local_path} to upload session {upload_session_id}"
            )

        def _upload_taiga_reference(file: TaigaReference):
            print(f"Linking virtual file {file.taiga_id} -> {file.name}")
            self.api.upload_file_to_taiga(
                upload_session_id,
                {
                    "filename": file.name,
                    "filetype": "virtual",
                    "existingTaigaId": file.taiga_id,
                    "custom_metadata": file.custom_metadata,
                },
            )

        if isinstance(upload, UploadedFile):
            _upload_uploaded_file(upload)
        elif isinstance(upload, TaigaReference):
            _upload_taiga_reference(upload)
        else:
            raise Exception(f"Unknown upload type: {type(upload)}")

    def _dataset_version_summary(self, dataset_version_id):
        # look up dataset version metadata and unpack values from the resulting dicts
        # this is a fairly round about way to get all the info we need, but trying to work within
        # what the current APIs expose.
        m = re.match("([a-z0-9.-]+)\\.(\\d+)", dataset_version_id)
        if m is not None:
            # dataset_version_id might be the opaque (old) uuid-like IDs for versions or
            # it might be the newer <permaname>.<version> format. Allow this function to handle
            # both. This code path is for the newer format
            version = self.api.get_dataset_version_metadata(
                m.group(1), dataset_version=m.group(2)
            )
        else:
            version = self.api.get_dataset_version_metadata(
                None, dataset_version=dataset_version_id
            )
        permaname = version["dataset"]["permanames"][0]

        version_number = version["datasetVersion"]["name"]
        assert permaname is not None
        assert version_number is not None

        print(
            cf.green(
                f"Dataset created. Access it directly with this url: {self.api.url}/dataset/{permaname}/{version_number}\n"
            )
        )

        version_metadata = self.api.get_dataset_version_metadata(
            permaname, version_number
        )
        assert version_metadata is not None

        # repackage the information callers might want in this client type
        return DatasetVersion(
            permanames=version_metadata["dataset"]["permanames"],
            version_number=int(version_metadata["datasetVersion"]["name"]),
            description=version_metadata["datasetVersion"]["description"],
            files=[
                DatasetVersionFile(
                    name=f["name"],
                    custom_metadata=f["custom_metadata"],
                    gs_path=f.get(
                        "gs_path"
                    ),  # todo: confirm this is the name of the field
                    datafile_id=f.get(
                        "underlying_file_id",
                        f"{permaname}.{version_number}/{f['name']}",
                    ),
                    format=f["type"],
                )
                for f in version_metadata["datasetVersion"]["datafiles"]
            ],
        )

    def get_dataset_metadata(
        self, permaname: str, version: Optional[str] = None
    ) -> Optional[Union[DatasetMetadataDict, DatasetVersionMetadataDict]]:
        """Get metadata about a dataset

        Keyword Arguments:
            - `permaname` -- Datafile ID of the datafile to get, in the form dataset_permaname
            - `dataset_version`: Either the numerical version (if `dataset_permaname` is provided)
            or the unique Taiga dataset version id

        Returns:
            Union[DatasetMetadataDict, DatasetVersionMetadataDict]
            `DatasetMetadataDict` if only permaname provided.  `DatasetVersionMetadataDict` if both permaname and version provided.
        """
        try:
            return self.api.get_dataset_version_metadata(
                dataset_permaname=permaname, dataset_version=version
            )
        except (ValueError, Taiga404Exception) as e:
            print(cf.red(str(e)))
            return None

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

        metadata = self.api.get_dataset_version_metadata(dataset_id, None)
        assert len(metadata["versions"]) == 1

        dataset_version_id = metadata["versions"][0]["id"]

        return self._dataset_version_summary(dataset_version_id)

    def replace_dataset(
        self,
        permaname: str,
        reason: str,
        files: List[File],
        description: Optional[str] = None,
    ) -> DatasetVersion:
        """
        Update an existing dataset creating a new version by replacing all datafiles with the ones
        provided (Results in a new dataset version)

        args:
            permaname: A taiga ID (without version) w

        """
        assert "." not in permaname, "When specifying a permaname, don't include the version suffix"
        metadata = self.api.get_dataset_version_metadata(permaname, None)

        upload_session_id = self._upload_files(files)

        prev_description = metadata["description"]
        if description is None:
            description = prev_description

        dataset_version_id = self.api.update_dataset(
            metadata["id"],
            upload_session_id,
            description,
            reason,
            None,
            add_existing_files=False,
        )

        return self._dataset_version_summary(dataset_version_id)

    def update_dataset(
        self,
        permaname: str,
        reason: str,
        description: Optional[str] = None,
        additions: List[File] = [],
        removals: List[str] = [],
        skip_uploads_if_sha_matches=True
    ) -> DatasetVersion:
        """
        Update an existing dataset by adding and removing the specified files. (Results in a new dataset version)
        """
        assert "." not in permaname, "When specifying a permaname, don't include the version suffix"

        if len(removals) > 0:
            raise NotImplementedError(
                "This option doesn't work at this time because changes are required to the Taiga service. Instead you can call replace_dataset with only the files you want to keep."
            )

        assert len(additions) > 0, f"No additions specified. This update would have no effect"

        latest_version_id = None
        if skip_uploads_if_sha_matches:
            latest_version_id, additions = _filter_out_matching_files(self, permaname, additions)

        if len(additions) == 0:
            print(
                cf.green(
                    f"no files needed uploading because all additions matched what was already there"
                )
            )
            dataset_version_id = latest_version_id
        else:
            metadata = self.api.get_dataset_version_metadata(permaname, None)

            upload_session_id = self._upload_files(additions)

            prev_description = metadata["description"]
            if description is None:
                description = prev_description

            dataset_version_id = self.api.update_dataset(
                metadata["id"],
                upload_session_id,
                description,
                reason,
                None,
                add_existing_files=True,
            )

        return self._dataset_version_summary(dataset_version_id)

    def _download_to_cache(self, datafile_id: str, *, format: str = "raw_test") -> str:
        try:
            canonical_id = self.get_canonical_id(datafile_id)
            dest = self._get_unique_name(canonical_id, ".raw")
            parsed = _parse_datafile_id(datafile_id)
            self.api.download_datafile(
                parsed.permaname, parsed.version, parsed.name, dest, format=format
            )
            return dest
        except Exception as ex:
            raise Exception(
                f"Got an internal error when trying to download {datafile_id} (format={format}) to cache"
            ) from ex

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
        return self._get_file_storage_type(metadata)

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
