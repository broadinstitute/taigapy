import json
import os
from typing import Dict
from typing_extensions import TypedDict

import requests

from taigapy.types import DataFileFormat

BASE_URL = "https://api.figshare.com/v2"

FigshareFileMetadata = TypedDict(
    "FigshareFileMetadata",
    {
        "article_id": str,
        "file_id": str,
        "format": DataFileFormat,
        "column_types": Dict[str, str],
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
        if not all(k in file_info for k in ["article_id", "file_id", "format"]):
            raise ValueError("The files in the figshare_map_file are ill-formed.")

        datafile_format = DataFileFormat(figshare_map[taiga_id]["format"])

        figshare_map[taiga_id]["format"] = datafile_format

        if (
            datafile_format == DataFileFormat.Columnar
            and file_info.get("column_types") is None
        ):
            raise ValueError("{} is missing column types".format(taiga_id))

    return figshare_map


def download_file_from_figshare(article_id: str, file_id: str, dest: str):
    api_endpoint = "{}/articles/{}/files/{}".format(BASE_URL, article_id, file_id)
    r = requests.get(api_endpoint)

    if r.status_code != 200:
        raise Exception("TODO")

    file_data = r.json()

    download_url = file_data["download_url"]

    r = requests.get(download_url)
    with open(dest, "wb") as f:
        f.write(r.content)
