import re
import requests

from typing import Mapping, Optional

from taigapy.custom_exceptions import Taiga404Exception, TaigaHttpException
from taigapy.types import DatasetVersion, DataFileMetadata
from taigapy.utils import untangle_dataset_id_with_version


def _standard_response_handler(r: requests.Response, params: Optional[Mapping]):
    if r.status_code == 404:
        raise Taiga404Exception(
            "Received a not found error. Are you sure about your credentials and/or the data parameters? params: {}".format(
                params
            )
        )
    elif r.status_code != 200:
        raise TaigaHttpException("Bad status code: {}".format(r.status_code))

    return r.json()


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

    def get_datafile_metadata(
        self,
        id_or_permaname: Optional[str],
        dataset_name: Optional[str],
        dataset_version: Optional[DatasetVersion],
        datafile_name: Optional[str],
    ) -> DataFileMetadata:
        api_endpoint = "/api/datafile"
        try:
            if dataset_name is None:
                (
                    dataset_permaname,
                    dataset_version,
                    datafile_name,
                ) = untangle_dataset_id_with_version(id_or_permaname)
            params = {
                "dataset_permaname": dataset_permaname,
                "version": str(dataset_version),
                "datafile_name": datafile_name,
                "format": "metadata",
            }
        except Exception:
            raise NotImplementedError

        return self._request_get(api_endpoint, params)

    def get_dataset_version_metadata(self):
        raise NotImplementedError

    def upload_dataset(self):
        raise NotImplementedError

    def poll_task(self, task_id: str):
        raise NotImplementedError

    def get_column_types(self):
        raise NotImplementedError

    def download_datafile(self):
        raise NotImplementedError
