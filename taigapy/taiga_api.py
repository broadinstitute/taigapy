import re
import time
from typing import Dict, Mapping, Optional, Union

import progressbar
import requests

from taigapy.custom_exceptions import (
    TaigaHttpException,
    Taiga404Exception,
    TaigaServerError,
)
from taigapy.types import DatasetVersion, DatasetMetadataDict,DatasetVersionMetadataDict,DataFileMetadata, TaskState, TaskStatus
from taigapy.utils import untangle_dataset_id_with_version

CHUNK_SIZE = 1024 * 1024


def _standard_response_handler(r: requests.Response, params: Optional[Mapping]):
    if r.status_code == 404:
        raise Taiga404Exception(
            "Received a not found error. Are you sure about your credentials and/or the data parameters? params: {}".format(
                params
            )
        )
    elif r.status_code == 500:
        raise TaigaServerError()
    elif r.status_code != 200:
        raise TaigaHttpException("Bad status code: {}".format(r.status_code))

    return r.json()


def _progressbar_init(max_value: Union[int, progressbar.UnknownLength]):
    """
    Initialize the progressbar object with the max_value passed as parameter
    :param max_value: int
    :return: ProgressBar
    """

    widgets = [
        progressbar.Bar(left="[", right="]"),
        progressbar.Percentage(),
        " | ",
        progressbar.FileTransferSpeed(),
        " | ",
        progressbar.DataSize(),
        " / ",
        progressbar.DataSize(variable="max_value"),
        " | ",
        progressbar.ETA(),
    ]
    bar = progressbar.ProgressBar(max_value=max_value, widgets=widgets)
    return bar


class TaigaApi:
    def __init__(self, url: str, token: str):
        self.url = url
        self.token = token

    def _request_get(
        self, api_endpoint: str, params=None, standard_reponse_handling: bool = True
    ):
        url = self.url + api_endpoint
        r = requests.get(
            url,
            stream=True,
            params=params,
            headers=dict(Authorization="Bearer " + self.token),
        )

        if standard_reponse_handling:
            return _standard_response_handler(r, params)
        else:
            return r

    def _request_post(
        self, api_endpoint: str, data: Mapping, standard_reponse_handling: bool = True,
    ):
        assert data is not None

        r = requests.post(
            self.url + api_endpoint,
            json=data,
            headers=dict(Authorization="Bearer " + self.token),
        )

        if standard_reponse_handling:
            return _standard_response_handler(r, data)
        else:
            return r

    @staticmethod
    def _download_file_from_s3(download_url: str, dest: str):
        r = requests.get(download_url, stream=True)

        header_content_length = r.headers.get("Content-Length", None)
        if not header_content_length:
            content_length = (
                progressbar.UnknownLength
            )  # type: Union[progressbar.UnknownLength, int]
        else:
            content_length = int(header_content_length)

        bar = _progressbar_init(max_value=content_length)

        with open(dest, "wb") as handle:
            if not r.ok:
                raise Exception("Error fetching {}".format(download_url))

            total = 0
            for block in r.iter_content(CHUNK_SIZE):
                handle.write(block)

                total += CHUNK_SIZE
                # total can be slightly superior to content_length
                if (
                    content_length == progressbar.UnknownLength
                    or total <= content_length
                ):
                    bar.update(total)
            bar.finish()

    def _poll_task(self, task_id: str) -> TaskStatus:
        api_endpoint = "/api/task_status/{}".format(task_id)
        r = self._request_get(api_endpoint)
        task_status = TaskStatus(r.json())
        while (
            task_status.state != TaskState.SUCCESS
            or task_status.state != TaskState.FAILURE
        ):
            r = self._request_get(api_endpoint)
            task_status = TaskStatus(r.json())
            time.sleep(1)

        return task_status

    def get_datafile_metadata(
        self,
        id_or_permaname: Optional[str],
        dataset_permaname: Optional[str],
        dataset_version: Optional[str],
        datafile_name: Optional[str],
    ) -> DataFileMetadata:
        api_endpoint = "/api/datafile"
        try:
            if dataset_permaname is None:
                (
                    dataset_permaname,
                    dataset_version,
                    datafile_name,
                ) = untangle_dataset_id_with_version(id_or_permaname)
            params = {
                "dataset_permaname": dataset_permaname,
                "version": dataset_version,
                "datafile_name": datafile_name,
                "format": "metadata",
            }
        except Exception:
            raise NotImplementedError

        return DataFileMetadata(self._request_get(api_endpoint, params))

    def get_dataset_version_metadata(
        self, dataset_permaname: str, dataset_version: Optional[DatasetVersion]
    ) -> Union[DatasetMetadataDict, DatasetVersionMetadataDict]:
        api_endpoint = "/api/dataset/{}".format(dataset_permaname)
        if dataset_version is not None:
            api_endpoint = "{}/{}".format(api_endpoint, dataset_version)

        return self._request_get(api_endpoint)

    def upload_dataset(self):
        raise NotImplementedError

    def get_column_types(
        self, dataset_permaname: str, dataset_version: str, datafile_name: str
    ) -> Dict[str, str]:
        api_endpoint = "/api/datafile/column_types"
        params = {
            "dataset_permaname": dataset_permaname,
            "version": dataset_version,
            "datafile_name": datafile_name,
        }
        r = self._request_get(api_endpoint, params, standard_reponse_handling=False)

        if r.status_code == 200:
            return r.json()
        elif r.status_code == 400:
            raise ValueError(
                "Request was not well formed. Please check your credentials and/or parameters. params: {}".format(
                    params
                )
            )
        elif r.status_code == 404:
            raise NotImplementedError
        elif r.status_code == 500:
            raise TaigaServerError()
        else:
            assert False

        raise NotImplementedError

    def download_datafile(
        self,
        dataset_permaname: str,
        dataset_version: str,
        datafile_name: str,
        dest: str,
    ):
        endpoint = "/api/datafile"
        params = {
            "dataset_permaname": dataset_permaname,
            "version": dataset_version,
            "datafile_name": datafile_name,
            "format": "raw_test",
        }

        r = self._request_get(endpoint, params, standard_reponse_handling=False)
        if r.status_code == 200:
            datafile_metadata = DataFileMetadata(r.json())
            download_url = datafile_metadata.urls[0]
            self._download_file_from_s3(download_url, dest)
        elif r.status_code == 202:
            task_status = self._poll_task(r.json())
            if task_status.state == TaskState.FAILURE:
                raise TaigaServerError()
            self.download_datafile(
                dataset_permaname, dataset_version, datafile_name, dest
            )
        elif r.status_code == 400:
            raise ValueError(
                "Request was not well formed. Please check your credentials and/or parameters. params: {}".format(
                    params
                )
            )
        else:
            raise TaigaServerError()
