from collections import defaultdict
import hashlib
import os
import re
from typing import (
    Collection,
    DefaultDict,
    Iterable,
    List,
    MutableSequence,
    Optional,
    Tuple,
)


from taigapy.custom_exceptions import TaigaTokenFileNotFound
from taigapy.types import (
    DatasetVersion,
    DatasetMetadataDict,
    DatasetVersionMetadataDict,
    DatasetVersionFiles,
    DataFileMetadata,
    UploadDataFile,
    UploadS3DataFileDict,
    UploadVirtualDataFile,
    UploadVirtualDataFileDict,
    S3Credentials,
    UploadS3DataFile,
)

DATAFILE_ID_FORMAT = "{dataset_permaname}.{dataset_version}/{datafile_name}"
DATAFILE_ID_FORMAT_MISSING_DATAFILE = "{dataset_permaname}.{dataset_version}"
DATAFILE_ID_REGEX_FULL = r"^(.*)\.(\d*)\/(.*)$"
DATAFILE_ID_REGEX_MISSING_DATAFILE = r"^(.*)\.(\d*)$"
DATAFILE_CACHE_FORMAT = "{dataset_permaname}_v{dataset_version}_{datafile_name}"
DATAFILE_UPLOAD_FORMAT_TO_STORAGE_FORMAT = {
    "NumericMatrixCSV": "HDF5",
    "TableCSV": "Columnar",
    "Raw": "Raw",
}


def find_first_existing(paths: Iterable[str]):
    for path in paths:
        path = os.path.expanduser(path)
        if os.path.exists(path):
            return path

    raise TaigaTokenFileNotFound(paths)


def untangle_dataset_id_with_version(taiga_id: str,) -> Tuple[str, str, Optional[str]]:
    """Returns dataset_permaname, dataset_version, and datafile_name from
    `taiga_id` in the form dataset_permaname.version/datafile_name or
    dataset_permaname.version.

    Arguments:
        taiga_id {str} -- Taiga datafile ID in the form
            dataset_permaname.version/datafile_name or
            dataset_permaname.version

    Raises:
        Exception: `taiga_id` not in the form
            dataset_permaname.version/datafile_name or
            dataset_permaname.version

    Returns:
        Tuple[str, str, Optional[str]] -- dataset_permaname, dataset_version, and
            datafile_name or None
    """
    taiga_id_search = re.search(DATAFILE_ID_REGEX_FULL, taiga_id)
    if taiga_id_search:
        dataset_permaname, dataset_version, datafile_name = (
            taiga_id_search.group(1),
            taiga_id_search.group(2),
            taiga_id_search.group(3),
        )
    else:
        taiga_id_search = re.search(DATAFILE_ID_REGEX_MISSING_DATAFILE, taiga_id)
        if taiga_id_search is None:
            raise ValueError(
                "{} not in the form dataset_permaname.version/datafile_name or dataset_permaname.version".format(
                    taiga_id
                )
            )

        dataset_permaname, dataset_version, datafile_name = (
            taiga_id_search.group(1),
            taiga_id_search.group(2),
            None,
        )

    return dataset_permaname, dataset_version, datafile_name


def format_datafile_id(
    dataset_permaname: str,
    dataset_version: DatasetVersion,
    datafile_name: Optional[str],
):
    name_parts = {
        "dataset_permaname": dataset_permaname,
        "dataset_version": dataset_version,
        "datafile_name": datafile_name,
    }

    id_format = (
        DATAFILE_ID_FORMAT
        if datafile_name is not None
        else DATAFILE_ID_FORMAT_MISSING_DATAFILE
    )

    return id_format.format(**name_parts)


def format_datafile_id_from_datafile_metadata(
    datafile_metadata: DataFileMetadata,
) -> str:
    return DATAFILE_ID_FORMAT.format(
        dataset_permaname=datafile_metadata.dataset_permaname,
        dataset_version=datafile_metadata.dataset_version,
        datafile_name=datafile_metadata.datafile_name,
    )


def get_latest_valid_version_from_metadata(
    dataset_metadata: DatasetMetadataDict,
) -> str:
    versions = dataset_metadata["versions"]
    latest_valid_version = 1
    for version in versions:
        version_num = int(version["name"])
        if version_num > latest_valid_version and version["state"] != "deleted":
            latest_valid_version = version_num

    return str(latest_valid_version)


def modify_upload_files(
    upload_files: MutableSequence[UploadS3DataFileDict],
    add_taiga_ids: MutableSequence[UploadVirtualDataFileDict],
    dataset_version_metadata: Optional[DatasetVersionMetadataDict] = None,
    add_all_existing_files: bool = False,
) -> Tuple[List[UploadS3DataFile], List[UploadVirtualDataFile]]:
    if dataset_version_metadata is not None:
        dataset_permaname = dataset_version_metadata["dataset"]["permanames"][-1]
        dataset_version = dataset_version_metadata["datasetVersion"]["version"]
        datafiles = dataset_version_metadata["datasetVersion"]["datafiles"]

        # For upload files that have the same content as file in the base dataset version,
        # add the file as a virtual datafile instead of uploading it
        add_as_virtual = {}
        for upload_file_dict in upload_files:
            sha256, md5 = get_file_hashes(upload_file_dict["path"])
            matching_file: DatasetVersionFiles = next(
                (
                    f
                    for f in datafiles
                    if (
                        f.get("original_file_sha256") == sha256
                        and f.get("original_file_md5") == md5
                        and (
                            DATAFILE_UPLOAD_FORMAT_TO_STORAGE_FORMAT[
                                upload_file_dict["format"]
                            ]
                            == f.get("type")
                        )
                    )
                ),
                None,
            )

            if matching_file is not None:
                name = upload_file_dict.get(
                    "name", standardize_file_name(upload_file_dict["path"])
                )
                taiga_id = (
                    f"{dataset_permaname}.{dataset_version}/{matching_file['name']}"
                )
                add_as_virtual[upload_file_dict["path"]] = (name, taiga_id)

        add_taiga_ids.extend(
            {"taiga_id": taiga_id, "name": name}
            for _, (name, taiga_id) in add_as_virtual.items()
        )

        upload_files = [
            upload_file_dict
            for upload_file_dict in upload_files
            if upload_file_dict["path"] not in add_as_virtual
        ]

    if add_all_existing_files:
        previous_version_taiga_ids: List[UploadVirtualDataFileDict] = [
            {
                "taiga_id": format_datafile_id(
                    dataset_permaname, dataset_version, datafile["name"]
                )
            }
            for datafile in dataset_version_metadata["datasetVersion"]["datafiles"]
        ]
    else:
        previous_version_taiga_ids = None

    upload_s3_datafiles = [UploadS3DataFile(f) for f in upload_files]
    upload_virtual_datafiles = [UploadVirtualDataFile(f) for f in add_taiga_ids]
    previous_version_datafiles = (
        [UploadVirtualDataFile(f) for f in previous_version_taiga_ids]
        if previous_version_taiga_ids is not None
        else None
    )

    # https://github.com/python/typeshed/issues/2383
    all_upload_datafiles: Collection[UploadDataFile] = (
        upload_s3_datafiles + upload_virtual_datafiles
    )  # type: ignore

    datafile_names: DefaultDict[str, int] = defaultdict(int)
    for upload_datafile in all_upload_datafiles:
        datafile_names[upload_datafile.file_name] += 1

    duplicate_file_names = [
        file_name for file_name, count in datafile_names.items() if count > 1
    ]
    if len(duplicate_file_names) > 0:
        raise ValueError(
            "Multiple files named {}.".format(", ".join(duplicate_file_names))
        )

    if previous_version_taiga_ids is not None:
        for upload_datafile in previous_version_datafiles:
            if upload_datafile.file_name not in datafile_names:
                upload_virtual_datafiles.append(upload_datafile)

    return upload_s3_datafiles, upload_virtual_datafiles


def standardize_file_name(file_name: str) -> str:
    return os.path.basename(os.path.splitext(file_name)[0])


def get_file_hashes(file_name: str) -> Tuple[str, str]:
    """Returns the sha256 and md5 hashes for a file."""
    sha256 = hashlib.sha256()
    md5 = hashlib.md5()
    with open(file_name, "rb") as fd:
        while True:
            buffer = fd.read(1024 * 1024)
            if len(buffer) == 0:
                break
            sha256.update(buffer)
            md5.update(buffer)
    return sha256.hexdigest(), md5.hexdigest()
