import codecs
import os

from abc import ABC
from enum import Enum
from typing import List, Optional, Union
from typing_extensions import Literal, TypedDict


class DataFileType(Enum):
    S3 = "s3"
    Virtual = "virtual"
    GCS = "gcs"


class DataFileFormat(Enum):
    HDF5 = "HDF5"
    Columnar = "Columnar"
    Raw = "Raw"


class DataFileUploadFormat(Enum):
    NumericMatrixCSV = "NumericMatrixCSV"
    TableCSV = "TableCSV"
    Raw = "Raw"


class DatasetVersionState(Enum):
    approved = "Approved"
    deprecated = "Deprecated"
    deleted = "Deleted"


class TaskState(Enum):
    PENDING = "PENDING"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    REVOKED = "REVOKED"
    PROGRESS = "PROGRESS"


User = TypedDict("User", {"id": str, "name": str})
Folder = TypedDict("Folder", {"id": str, "name": str})
DatasetVersion = Union[str, int]
DatasetVersionShortDict = TypedDict(
    "DatasetVersionShortDict",
    {"id": str, "name": str, "state": Literal["approved", "deprecated", "deleted"]},
)
DatasetVersionFiles = TypedDict(
    "DatasetVersionFiles",
    {
        "allowed_conversion_type": List[str],  # TODO: remove
        "datafile_type": DataFileType,
        "gcs_path": Optional[str],
        "id": str,
        "name": str,
        "short_summary": str,
        "type": str,  # DataFileFormat
        "underlying_file_id": Optional[str],
        "original_file_md5": Optional[str],
        "original_file_sha256": Optional[str],
    },
)
DatasetVersionLongDict = TypedDict(
    "DatasetVersionLongDict",
    {
        "can_edit": bool,
        "can_view": bool,
        "changes_description": Optional[str],
        "creation_date": str,
        "creator": User,
        "datafiles": List[DatasetVersionFiles],
        "dataset_id": str,
        "description": str,
        "folders": List[Folder],  # empty list (TODO: remove)
        "id": str,
        "name": str,
        "reason_state": str,
        "state": Literal["approved", "deprecated", "deleted"],
        "version": str,
    },
)

DatasetMetadataDict = TypedDict(
    "DatasetMetadataDict",
    {
        "can_edit": bool,
        "can_view": bool,
        "description": str,
        "folders": List[Folder],
        "id": str,
        "name": str,
        "permanames": List[str],
        "versions": List[DatasetVersionShortDict],
    },
)

DatasetVersionMetadataDict = TypedDict(
    "DatasetVersionMetadataDict",
    {"dataset": DatasetMetadataDict, "datasetVersion": DatasetVersionLongDict},
)

DataFileMetadataDict = TypedDict(
    "DataFileMetadataDict",
    {
        "dataset_name": str,
        "dataset_permaname": str,
        "dataset_version": str,
        "dataset_id": str,
        "dataset_version_id": str,
        "datafile_name": str,
        "status": str,
        "state": str,
        "reason_state": str,
        "datafile_type": str,
        "datafile_format": str,
        "datafile_encoding": str,
        "urls": Optional[List[str]],
        "underlying_file_id": Optional[str],
    },
)


class DataFileMetadata:
    def __init__(self, datafile_metadata_dict: DataFileMetadataDict):
        self.dataset_name: str = datafile_metadata_dict["dataset_name"]
        self.dataset_permaname: str = datafile_metadata_dict["dataset_permaname"]
        self.dataset_version: str = datafile_metadata_dict["dataset_version"]
        self.dataset_id: str = datafile_metadata_dict["dataset_id"]
        self.dataset_version_id: str = datafile_metadata_dict["dataset_version_id"]
        self.datafile_name: str = datafile_metadata_dict["datafile_name"]
        self.status: str = datafile_metadata_dict["status"]
        self.state: DatasetVersionState = DatasetVersionState(
            datafile_metadata_dict["state"]
        )
        self.reason_state: Optional[str] = datafile_metadata_dict.get("reason_state")
        self.datafile_type: DataFileType = DataFileType(
            datafile_metadata_dict["datafile_type"]
        )
        # datafile_format does not exist if type is gcs
        self.datafile_format: Optional[DataFileFormat] = DataFileFormat(
            datafile_metadata_dict.get("datafile_format")
        )
        self.datafile_encoding: Optional[str] = datafile_metadata_dict.get(
            "datafile_encoding"
        )
        self.urls: Optional[List[str]] = datafile_metadata_dict.get("urls")
        self.underlying_file_id: Optional[str] = datafile_metadata_dict.get(
            "underlying_file_id"
        )


TaskStatusDict = TypedDict(
    "TaskStatusDict",
    {
        "id": str,
        "state": str,
        "message": str,
        "current": float,
        "total": float,
        "s3Key": str,
    },
)


class TaskStatus:
    def __init__(self, task_status_dict: TaskStatusDict):
        self.id: str = task_status_dict["id"]
        self.state: TaskState = TaskState(task_status_dict["state"])
        self.message: str = task_status_dict["message"]
        self.current: float = task_status_dict["current"]
        self.total: float = task_status_dict["total"]
        self.s3Key: str = task_status_dict["s3Key"]


class S3Credentials:
    def __init__(self, s3_credentials_dict):
        self.access_key_id: str = s3_credentials_dict["accessKeyId"]
        self.bucket: str = s3_credentials_dict["bucket"]
        self.expiration: str = s3_credentials_dict["expiration"]
        self.prefix: str = s3_credentials_dict["prefix"]
        self.secret_access_key: str = s3_credentials_dict["secretAccessKey"]
        self.session_token: str = s3_credentials_dict["sessionToken"]


UploadS3DataFileDict = TypedDict(
    "UploadS3DataFileDict",
    {"path": str, "name": str, "format": str, "encoding": str},
    total=False,
)


class UploadDataFile(ABC):
    file_name: str

    def to_api_param(self):
        pass


class UploadS3DataFile(UploadDataFile):
    def __init__(self, upload_s3_file_dict: UploadS3DataFileDict):
        from taigapy.utils import standardize_file_name

        self.file_path = os.path.abspath(upload_s3_file_dict["path"])
        if not os.path.exists(self.file_path):
            raise Exception(
                "File '{}' does not exist.".format(upload_s3_file_dict["path"])
            )
        self.file_name = upload_s3_file_dict.get(
            "name", standardize_file_name(self.file_path)
        )
        self.datafile_format = DataFileUploadFormat(upload_s3_file_dict["format"])
        self.encoding = codecs.lookup(upload_s3_file_dict.get("encoding", "utf-8")).name
        self.bucket: Optional[str] = None
        self.key: Optional[str] = None

    def add_s3_upload_information(self, bucket: str, key: str):
        self.bucket = bucket
        self.key = key

    def to_api_param(self):
        return {
            "filename": self.file_name,
            "filetype": "s3",
            "s3Upload": {
                "format": self.datafile_format.value,
                "bucket": self.bucket,
                "key": self.key,
                "encoding": self.encoding,
            },
        }


UploadVirtualDataFileDict = TypedDict(
    "UploadVirtualDataFileDict", {"taiga_id": str, "name": str}, total=False
)


class UploadVirtualDataFile(UploadDataFile):
    def __init__(self, upload_virtual_file_dict: UploadVirtualDataFileDict):
        self.taiga_id = upload_virtual_file_dict["taiga_id"]
        self.file_name = upload_virtual_file_dict.get(
            "name", self.taiga_id.split("/", 1)[1]
        )

    def to_api_param(self):
        return {
            "filename": self.file_name,
            "filetype": "virtual",
            "existingTaigaId": self.taiga_id,
        }


UploadGCSDataFileDict = TypedDict(
    "UploadGCSDataFileDict", {"gcs_path": str, "name": str}, total=False
)


class UploadGCSDataFile(UploadDataFile):
    def __init__(self, upload_gsc_file_dict: UploadGCSDataFileDict):
        self.file_name = upload_gsc_file_dict["name"]
        self.gcs_path = upload_gsc_file_dict["gcs_path"]

    def to_api_param(self):
        return {"filename": self.file_name, "filetype": "gcs", "gcsPath": self.gcs_path}
