from enum import Enum
from typing import List, Optional, Union
from typing_extensions import TypedDict


class DataFileType(Enum):
    S3 = "s3"
    Virtual = "virtual"


class DataFileFormat(Enum):
    HDF5 = "HDF5"
    Columnar = "Columnar"
    Raw = "Raw"


class DatasetVersionState(Enum):
    approved = "Approved"
    deprecated = "Deprecated"
    deleted = "Deleted"


DatasetVersion = Union[str, int]

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
