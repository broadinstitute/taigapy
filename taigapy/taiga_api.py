import time
from typing import Dict, Mapping, Optional, Union

import progressbar
import requests
import logging

from taigapy.custom_exceptions import (
    Taiga404Exception,
    TaigaHttpException,
    TaigaServerError,
)
from taigapy.types import (
    DataFileMetadata,
    DataFileType,
    DatasetMetadataDict,
    DatasetVersion,
    DatasetVersionMetadataDict,
    S3Credentials,
    TaskState,
    TaskStatus,
    UploadDataFile,
)
from taigapy.utils import (
    format_datafile_id,
    parse_gcs_path,
    untangle_dataset_id_with_version,
)

log = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024


def _standard_response_handler(
    r: requests.Response, params: Optional[Mapping], url=None
):
    if r.status_code == 404:
        raise Taiga404Exception(
            "Received a not found error. Are you sure about your credentials and/or the data parameters? params: {}".format(
                params
            )
        )
    elif r.status_code == 500:
        raise TaigaServerError()
    elif r.status_code != 200:
        raise TaigaHttpException(
            f"Bad status code ({r.status_code}) when POST {url} with params={params}"
        )

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


def run_with_max_retries(call, max_attempts, retry_delay=1.0):
    failed_attempts = 0
    while True:
        try:
            return call()
        except IOError as ex:
            failed_attempts += 1
            if failed_attempts >= max_attempts:
                log.exception("Too many failed attempts. Raising exception")
                raise
            log.warning(
                f"Got exception {ex}, will attempt a retry in {retry_delay} seconds ({failed_attempts}/{max_attempts} attempt"
            )
            time.sleep(retry_delay)
            retry_delay *= 2


class TaigaApi:
    url: str
    token: str
    max_attempts: int

    def __init__(self, url: str, token: str, max_attempts=5):
        self.url = url
        self.token = token
        self.max_attempts = max_attempts

    def _request_get(
        self, api_endpoint: str, params=None, standard_reponse_handling: bool = True
    ):
        def inner():
            nonlocal params
            from taigapy import __version__

            url = self.url + api_endpoint

            if params is None:
                params = {}

            params["taigapy_version"] = __version__

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

        return run_with_max_retries(inner, self.max_attempts)

    def _request_post(
        self, api_endpoint: str, data: Mapping, standard_reponse_handling: bool = True
    ):
        from taigapy import __version__

        assert data is not None

        params = {"taigapy_version": __version__}

        full_url = self.url + api_endpoint
        r = requests.post(
            full_url,
            params=params,
            json=data,
            headers=dict(Authorization="Bearer " + self.token),
        )

        if standard_reponse_handling:
            return _standard_response_handler(
                r, data, url=full_url
            )  # , params=params, json=data)
        else:
            return r

    @staticmethod
    def _download_file_from_gcs(gcs_path: str, dest: str):
        from google.cloud import storage, exceptions

        try:
            storage_client = storage.Client()
            bucket_name, file_name = parse_gcs_path(gcs_path)
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.get_blob(file_name)

            if not blob.size:
                content_length = (
                    progressbar.UnknownLength
                )  # type: Union[progressbar.UnknownLength, int]
            else:
                content_length = int(blob.size)

            bar = _progressbar_init(max_value=blob.size)
            if not blob:
                raise Exception(f"Error fetching {file_name}")

            total = 0
            for block in range(0, blob.size):
                total += block
                # total can be slightly superior to content_length
                if (
                    content_length == progressbar.UnknownLength
                    or total <= content_length
                ):
                    bar.update(total)

            blob.download_to_filename(dest)
            bar.finish()
        except exceptions.NotFound:
            raise Exception(f"Error fetching {file_name}")

    @staticmethod
    def _download_file_from_s3(download_url: str, dest: str):
        log.debug("Downloading %s to %s", download_url, dest)
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
        task_status = TaskStatus(r)
        while (
            task_status.state != TaskState.SUCCESS
            and task_status.state != TaskState.FAILURE
        ):
            r = self._request_get(api_endpoint)
            task_status = TaskStatus(r)
            time.sleep(1)

        return task_status

    def is_connected(self) -> bool:
        try:
            requests.get(self.url)
            return True
        except requests.ConnectionError:
            return False

    def get_user(self):
        api_endpoint = "/api/user"
        return self._request_get(api_endpoint)

    def upload_file_to_taiga(
        self, session_id: str, session_file: Union[UploadDataFile, Dict]
    ):
        if isinstance(session_file, dict):
            api_params = session_file
        else:
            api_params = session_file.to_api_param()

        api_endpoint = "/api/datafile/{}".format(session_id)
        task_id = self._request_post(api_endpoint=api_endpoint, data=api_params)

        if task_id == "done":
            return

        task_status = self._poll_task(task_id)

        if task_status.state == TaskState.SUCCESS:
            return
        else:
            raise ValueError(
                f"Error uploading {api_params.get('filename')}: { task_status.message }"
            )

    def get_datafile_metadata(
        self,
        id_or_permaname: Optional[str],
        dataset_permaname: Optional[str],
        dataset_version: Optional[str],
        datafile_name: Optional[str],
    ) -> DataFileMetadata:
        api_endpoint = "/api/datafile"
        if id_or_permaname is not None and "." in id_or_permaname:
            (
                dataset_permaname,
                dataset_version,
                new_datafile_name,
            ) = untangle_dataset_id_with_version(id_or_permaname)
            if datafile_name is None:
                datafile_name = new_datafile_name

        if dataset_permaname is not None:
            params = {
                "dataset_permaname": dataset_permaname,
                "version": dataset_version,
                "datafile_name": datafile_name,
                "format": "metadata",
            }
        else:
            params = {
                "dataset_version_id": id_or_permaname,
                "datafile_name": datafile_name,
                "format": "metadata",
            }

        return DataFileMetadata(self._request_get(api_endpoint, params))

    def get_dataset_version_metadata(
        self, dataset_permaname: str, dataset_version: Optional[str]
    ) -> Union[DatasetMetadataDict, DatasetVersionMetadataDict]:
        """
        Returns a `DatasetMetadataDict` if only `dataset_permaname` param is provided. Returns `DatasetVersionMetadataDict` if `dataset_version` is provided.
       
        A Taiga dataset id consists of:
            - `permaname`: Unique identifier for dataset group
            - `version`: Numerical version of the Taiga dataset upload
            - `name`: Name of the file in the Taiga dataset group. If provided the dataset id is for that specific file rather than dataset group.

        Params:
        - `dataset_permaname`: The Taiga dataset permaname
        - `dataset_version`: Either the numerical version (if `dataset_permaname` is provided) or the unique Taiga dataset version id (if `dataset_permaname` is not provided)
        """
        api_endpoint = "/api/dataset/{}".format(dataset_permaname)
        if dataset_version is not None:
            api_endpoint = "{}/{}".format(api_endpoint, dataset_version)

        return self._request_get(api_endpoint)

    def get_column_types(
        self, dataset_permaname: str, dataset_version: str, datafile_name: str
    ) -> Optional[Dict[str, str]]:
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
            raise ValueError(
                "No datafile found with for dataile id {}".format(
                    format_datafile_id(
                        dataset_permaname, dataset_version, datafile_name
                    )
                )
            )
        elif r.status_code == 500:
            raise TaigaServerError()

        raise TaigaHttpException(
            "Unrecognized status code for datafile/column_types: {}".format(
                r.status_code
            )
        )

    def download_datafile(
        self,
        dataset_permaname: str,
        dataset_version: str,
        datafile_name: str,
        dest: str,
        *,
        format="raw_test"
            ):
        endpoint = "/api/datafile"
        params = {
            "dataset_permaname": dataset_permaname,
            "version": dataset_version,
            "datafile_name": datafile_name,
            "format": format,
        }

        r = self._request_get(endpoint, params, standard_reponse_handling=False)

        if r.status_code == 200:
            datafile_metadata = DataFileMetadata(r.json())
            download_url = datafile_metadata.urls[0]
            if datafile_metadata.datafile_type == DataFileType.GCS:
                self._download_file_from_gcs(datafile_metadata.gcs_path, dest)
            else:
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

    def get_folder(self, folder_id: str):
        api_endpoint = "/api/folder/{}".format(folder_id)
        return self._request_get(api_endpoint)

    def get_s3_credentials(self) -> S3Credentials:
        api_endpoint = "/api/credentials_s3"
        return S3Credentials(self._request_get(api_endpoint))

    def create_upload_session(self) -> str:
        api_endpoint = "/api/upload_session"
        return self._request_get(api_endpoint, params=None)

    def create_dataset(
        self,
        upload_session_id: str,
        folder_id: str,
        dataset_name: str,
        dataset_description: Optional[str],
    ) -> str:
        api_endpoint = "/api/dataset"

        new_dataset_params = {
            "sessionId": upload_session_id,
            "currentFolderId": folder_id,
            "datasetName": dataset_name,
            "datasetDescription": dataset_description,
        }

        return self._request_post(api_endpoint, data=new_dataset_params)

    def update_dataset(
        self,
        dataset_id: str,
        session_id: str,
        description: str,
        changes_description: Optional[str],
        dataset_version: Optional[str],
        add_existing_files: bool = False,
    ) -> str:
        params = {
            "datasetId": dataset_id,
            "sessionId": session_id,
            "newDescription": description,
            "datasetVersion": dataset_version,
            "addExistingFiles": add_existing_files,
        }
        if changes_description is not None:
            params["changesDescription"] = changes_description

        api_endpoint = "/api/datasetVersion"

        new_dataset_version_id = self._request_post(
            api_endpoint=api_endpoint, data=params
        )

        return new_dataset_version_id

    def upload_to_gcs(self, datafile_id: str, dest_gcs_path: str):
        api_endpoint = "/api/datafile/copy_to_google_bucket"
        params = {"datafile_id": datafile_id, "gcs_path": dest_gcs_path}
        task_id = self._request_post(api_endpoint=api_endpoint, data=params)

        if task_id == "done":
            return

        task_status = self._poll_task(task_id)

        if task_status.state == TaskState.SUCCESS:
            return
        else:
            raise ValueError(
                "Error uploading {}: {}.".format(datafile_id, task_status.message)
            )
