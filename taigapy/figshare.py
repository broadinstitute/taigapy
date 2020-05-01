import json
import os
from typing import Dict, Optional
from typing_extensions import TypedDict

import requests

from taigapy.types import DataFileFormat

BASE_URL = "https://api.figshare.com/v2"

FigshareFileMetadata = TypedDict(
    "FigshareFileMetadata",
    {
        "download_url": str,
        "format": DataFileFormat,
        "column_types": Optional[Dict[str, str]],
        "encoding": str,
    },
    total=False,
)


def parse_figshare_map_file(figshare_map_file: str) -> Dict[str, FigshareFileMetadata]:
    if not os.path.exists(figshare_map_file):
        raise ValueError(
            "Could not find figshare_map_file at {}.".format(parse_figshare_map_file)
        )

    with open(figshare_map_file) as f:
        figshare_map = json.load(f)

    for taiga_id, file_info in figshare_map.items():
        if not all(
            k in file_info
            for k in ["download_url", "format", "column_types", "encoding"]
        ):
            raise ValueError("The files in the figshare_map_file are ill-formed.")

        datafile_format = DataFileFormat(figshare_map[taiga_id]["format"])

        figshare_map[taiga_id]["format"] = datafile_format

        if (
            datafile_format == DataFileFormat.Columnar
            and file_info.get("column_types") is None
        ):
            raise ValueError("{} is missing column types".format(taiga_id))

    return figshare_map


def download_file_from_figshare(download_url: str, dest: str):
    r = requests.get(download_url)
    with open(dest, "wb") as f:
        f.write(r.content)
