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
        "type": DataFileFormat,
        "underlying_file_id": str,
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
        self.dataset_name: str = datafile_metadata_dict.get("dataset_name")
        self.dataset_permaname: str = datafile_metadata_dict.get("dataset_permaname")
        self.dataset_version: str = datafile_metadata_dict.get("dataset_version")
        self.dataset_id: str = datafile_metadata_dict.get("dataset_id")
        self.dataset_version_id: str = datafile_metadata_dict.get("dataset_version_id")
        self.datafile_name: str = datafile_metadata_dict.get("datafile_name")
        self.status: str = datafile_metadata_dict.get("status")
        self.state: DatasetVersionState = DatasetVersionState(
            datafile_metadata_dict.get("state")
        )
        self.reason_state: Optional[str] = datafile_metadata_dict.get("reason_state")
        self.datafile_type: DataFileType = DataFileType(
            datafile_metadata_dict.get("datafile_type")
        )
        self.datafile_format: DataFileFormat = DataFileFormat(
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
        self.id: str = task_status_dict.get("id")
        self.state: TaskState = TaskState(task_status_dict.get("state"))
        self.message: str = task_status_dict.get("message")
        self.current: float = task_status_dict.get("current")
        self.total: float = task_status_dict.get("total")
        self.s3Key: str = task_status_dict.get("s3Key")
