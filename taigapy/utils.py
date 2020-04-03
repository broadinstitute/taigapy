import os
import re
from typing import Iterable, Optional, Tuple

from taigapy.types import DatasetVersion

DATAFILE_ID_FORMAT = "{dataset_permaname}.{dataset_version}/{datafile_name}"
DATAFILE_ID_FORMAT_MISSING_DATAFILE = "{dataset_permaname}.{dataset_version}"
DATAFILE_ID_REGEX_FULL = r"^(.*)\.(\d*)\/(.*)$"
DATAFILE_ID_REGEX_MISSING_DATAFILE = r"^(.*)\.(\d*)$"
DATAFILE_CACHE_FORMAT = "{dataset_permaname}_v{dataset_version}_{datafile_name}"


def find_first_existing(paths: Iterable[str]):
    for path in paths:
        if os.path.exists(path):
            return path
    raise Exception(
        "No token file found. Checked the following locations: {}".format(paths)
    )


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
