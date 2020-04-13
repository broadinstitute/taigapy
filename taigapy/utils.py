import asyncio
import os
import re
from typing import Iterable, Optional, Tuple

import aiobotocore

from taigapy.custom_exceptions import TaigaTokenFileNotFound
from taigapy.types import (
    DatasetVersion,
    DatasetMetadataDict,
    DatasetVersionMetadataDict,
    DataFileMetadata,
    S3Credentials,
    UploadS3DataFile,
)

DATAFILE_ID_FORMAT = "{dataset_permaname}.{dataset_version}/{datafile_name}"
DATAFILE_ID_FORMAT_MISSING_DATAFILE = "{dataset_permaname}.{dataset_version}"
DATAFILE_ID_REGEX_FULL = r"^(.*)\.(\d*)\/(.*)$"
DATAFILE_ID_REGEX_MISSING_DATAFILE = r"^(.*)\.(\d*)$"
DATAFILE_CACHE_FORMAT = "{dataset_permaname}_v{dataset_version}_{datafile_name}"


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


async def upload_to_s3(
    s3_credentials: S3Credentials, new_session_id: str, upload_file: UploadS3DataFile
):
    print("Uploading {} to S3".format(upload_file.file_name))
    bucket = s3_credentials.bucket
    partial_prefix = s3_credentials.prefix
    key = "{}{}/{}".format(partial_prefix, new_session_id, upload_file.file_name)

    session = aiobotocore.get_session()
    async with session.create_client(
        "s3",
        aws_access_key_id=s3_credentials.access_key_id,
        aws_secret_access_key=s3_credentials.secret_access_key,
        aws_session_token=s3_credentials.session_token,
    ) as s3_client:
        with open(upload_file.file_path, "rb") as f:
            await s3_client.put_object(Bucket=bucket, Key=key, Body=f.read())
    upload_file.add_s3_upload_information(bucket, key)
    print("Finished uploading {} to S3".format(upload_file.file_name))
