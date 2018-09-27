import boto3
import colorful
import requests
import pandas
import os
import tempfile
import time
import sys

from taigapy.UploadFile import UploadFile

__version__ = "2.4.1"

class Taiga1Client:
    def __init__(self, url="http://taiga.broadinstitute.org", user_key=None, cache_dir="~/.taigapy"):
        self.url = url
        self.user_key = user_key
        self.cache_dir = os.path.expanduser(cache_dir)

    def get_dataset_id_by_name(self, name, md5=None, version=None):
        params = dict(fetch="id", name=name)
        if md5 is not None:
            params['md5'] = md5
        if version is not None:
            params['version'] = str(version)

        r = requests.get(self.url + "/rest/v0/namedDataset", params=params)
        if r.status_code == 404:
            return None
        return r.text

    def download_to_cache(self, id=None, name=None, version=None, format="csv"):
        if id is None:
            assert name is not None, "id or name must be specified"
            id = self.get_dataset_id_by_name(name, version=version)
            if id is None:
                return None

        local_file = os.path.join(self.cache_dir, id + "." + format)
        if not os.path.exists(local_file):
            if not os.path.exists(self.cache_dir):
                os.makedirs(self.cache_dir)

            if format == "csv":
                format = "tabular_csv"

            r = requests.get(self.url + "/rest/v0/datasets/" + id + "?format=" + format, stream=True)
            if r.status_code == 404:
                return None

            if r.status_code != 200 and format == "tabular_csv":
                # hack: If this couldn't be fetched as tabular_csv try just csv
                r = requests.get(self.url + "/rest/v0/datasets/" + id + "?format=csv", stream=True)

            assert r.status_code == 200

            with tempfile.NamedTemporaryFile(dir=self.cache_dir, suffix=".tmpdl", delete=False) as fd:
                # print("read...")
                for chunk in r.iter_content(chunk_size=100000):
                    fd.write(chunk)
            os.rename(fd.name, local_file)
        return local_file

    def get(self, id=None, name=None, version=None):
        local_file = self.download_to_cache(id, name, version)
        return pandas.read_csv(local_file, index_col=0)


# global variable to allow people to globally override the location before initializing client
# which is often useful in adhoc scripts being submitted onto the cluster.
DEFAULT_CACHE_DIR = "~/.taiga"


class Taiga2Client:
    def __init__(self, url="https://cds.team/taiga", cache_dir=None, token_path=None):
        self.formats = ["NumericMatrixCSV",
                        "NumericMatrixTSV",
                        "TableCSV",
                        "TableTSV",
                        "GCT",
                        "Raw", ]
        self.url = url

        if cache_dir is None:
            cache_dir = DEFAULT_CACHE_DIR
        self.cache_dir = os.path.expanduser(cache_dir)

        if token_path is None:
            token_path = self._find_first_existing(["./.taiga-token", os.path.join(self.cache_dir, "token")])

        with open(token_path, "rt") as r:
            self.token = r.readline().strip()

    def _find_first_existing(self, paths):
        for path in paths:
            if os.path.exists(path):
                return path
        raise Exception("No token file found. Checked the following locations: {}".format(paths))

    def get_dataset_id_by_name(self, name, md5=None, version=None):
        """Deprecated"""
        params = dict(fetch="id", name=name)
        if md5 is not None:
            params['md5'] = md5
        if version is not None:
            params['version'] = str(version)

        r = requests.get(self.url + "/rest/v0/namedDataset", params=params)
        if r.status_code == 404:
            return None
        return r.text

    def _untangle_dataset_id_with_version(self, id):
        """File can be optional in the id"""
        name, version = id.split('.', 1)
        assert name
        assert version

        if '/' in version:
            version, file = version.split('/', 1)
            assert file
        else:
            file = None

        return name, version, file

    def _get_params_dict(self, id, name, version, file, force=None, format=None):
        """Parse the params into a dict we can use for GET/POST requests"""
        params = dict(format=format)

        if id is not None:
            if '.' in id:
                name, version, file = self._untangle_dataset_id_with_version(id)
                params['dataset_permaname'] = name
                params['datafile_name'] = file
                params['version'] = version

            else:
                params['dataset_version_id'] = id
                assert version is None
        else:
            assert name is not None
            params['dataset_permaname'] = name
            if version is not None:
                params['version'] = str(version)

        if file is not None:
            params['datafile_name'] = file

        if force:
            params['force'] = 'Y'

        return params

    def _get_data_file_json(self, id, name, version, file, force, format):
        params = self._get_params_dict(id=id, name=name, version=version,
                                       file=file, force=force, format=format)

        api_endpoint = "/api/datafile"
        return self.request_get(api_endpoint, params)

    def _get_data_file_summary(self, id, name, version, file):
        """Get the summary of a datafile"""
        params = self._get_params_dict(id=id, name=name, version=version, file=file)

        api_endpoint = "/api/datafile/short_summary"
        return self.request_get(api_endpoint=api_endpoint, params=params)

    def get_datafile_types(self, dataset_id, version):
        r = requests.get(self.url + "/api/dataset/" + dataset_id + "/" + str(version),
                         headers=dict(Authorization="Bearer " + self.token))
        assert r.status_code == 200
        metadata = r.json()
        type_by_name = dict(
            [(datafile['name'], datafile['type']) for datafile in metadata['datasetVersion']['datafiles']])
        return type_by_name

    def _get_data_file_type(self, dataset_id, version, filename):
        type_by_name = self.get_datafile_types(dataset_id, version)
        return type_by_name[filename]

    def _get_dataset_version_id_from_permaname_version(self, name, version=None):
        assert name, "If not id is given, we need the permaname of the dataset and the version"
        api_endpoint_get_dataset_version_id = "/api/dataset/{datasetId}".format(datasetId=name)
        request = self.request_get(api_endpoint=api_endpoint_get_dataset_version_id)

        id = None

        versions = request['versions']
        # Sort versions to get the latest at the end
        versions = sorted(versions, key=lambda x: x['name'])

        if version is None:
            # If no version provided, we fetch the latest version
            id = versions[-1]['id']
        else:
            for dataset_version in versions:
                if dataset_version['name'] == str(version):
                    id = dataset_version['id']

        if not id:
            raise Exception("The version {} does not exist in the dataset permaname {}".format(version, name))

        return id

    def _get_allowed_conversion_type_from_dataset_version(self, file, id=None, name=None, version=None):
        # Get the id and version id of name/version
        if not id:
            # Need to get the dataset version from id
            id = self._get_dataset_version_id_from_permaname_version(name=name, version=version)
        # else:
        else:
            if '.' in id:
                # TODO: Check also if file already filled out
                # We need to check if we have a dataset id with version or a dataset version id
                name, version, file = self._untangle_dataset_id_with_version(id)
                assert name, "Id {} passed does not match any dataset".format(id)
                assert version, "No version found for this id {}".format(id)
                try:
                    id = self._get_dataset_version_id_from_permaname_version(name, version)
                except:
                    print(sys.exc_info()[0])

        api_endpoint = "/api/dataset/{datasetId}/{datasetVersionId}".format(datasetId=None,
                                                                            datasetVersionId=id)

        result = self.request_get(api_endpoint=api_endpoint)

        # Get the file
        full_dataset_version_datafiles = result['datasetVersion']['datafiles']

        # Get the allowed conversion type
        if not file:
            return full_dataset_version_datafiles[0]['allowed_conversion_type']
        else:
            for datafile in full_dataset_version_datafiles:
                if datafile['name'] == file:
                    return datafile['allowed_conversion_type']

        return Exception("Error...check the file name?")

    def _dl_file(self, id, name, version, file, force, format, destination):
        first_attempt = True
        prev_status = None
        delay_between_polls = 1
        waiting_for_conversion = True
        while waiting_for_conversion:
            response = self._get_data_file_json(id, name, version, file, force, format)
            force = False

            if response.get("urls", None) is None:
                if first_attempt:
                    print("Taiga needs to convert data to rds before we can fetch it.  Waiting...\n")
                else:
                    if prev_status != response['status']:
                        print("Status: {}".format(response['status']))

                prev_status = response['status']
                first_attempt = False
                time.sleep(delay_between_polls)
            else:
                waiting_for_conversion = False

        urls = response['urls']
        assert len(urls) == 1
        r = requests.get(urls[0], stream=True)
        with open(destination, 'wb') as handle:
            if not r.ok:
                raise Exception("Error fetching {}".format(urls[0]))

            for block in r.iter_content(1024 * 100):
                handle.write(block)

    def _resolve_and_download(self, id=None, name=None, version=None, file=None, force=False, format="csv"):
        # returns a file in the cache with the data
        if id is None:
            assert name is not None, "id or name must be specified"

        metadata = self._get_data_file_json(id, name, version, file, force, "metadata")
        if metadata is None:
            raise Exception("No data for the given parameters. Please check your inputs are correct.")

        data_id = metadata['dataset_version_id']
        data_name = metadata['dataset_permaname']
        data_version = metadata['dataset_version']
        data_file = metadata["datafile_name"]
        data_state = metadata["state"]
        data_reason_state = metadata["reason_state"]

        assert data_id is not None
        assert data_name is not None
        assert data_version is not None
        assert data_file is not None

        # TODO: Add enum to manage deprecation/approval/deletion
        if data_state == 'Deprecated':
            print(colorful.orange(
                "WARNING: This version is deprecated. Please use with caution, and see below the reason:"))
            print(colorful.orange(
                "\t{}".format(data_reason_state)))

        local_file = os.path.join(self.cache_dir, data_id + "_" + data_file + "." + format)
        if not os.path.exists(local_file) or force:
            if not os.path.exists(self.cache_dir):
                os.makedirs(self.cache_dir)

            with tempfile.NamedTemporaryFile(dir=self.cache_dir, suffix=".tmpdl", delete=False) as fd:
                #            def _dl_file(self, id, name, version, file, force, destination):

                self._dl_file(data_id, None, None, data_file, force, format, fd.name)
            os.rename(fd.name, local_file)
        return data_id, data_name, data_version, data_file, local_file

    def is_valid_dataset(self, id=None, name=None, version=None, file=None, force=False, format='raw'):
        try:
            self._get_data_file_json(id, name, version, file, force, format)
            return True
        except:
            return False

    def download_to_cache(self, id=None, name=None, version=None, file=None, force=False, format="raw"):
        data_id, data_name, data_version, data_file, local_file = self._resolve_and_download(id, name, version, file,
                                                                                             force, format)
        return local_file

    def get(self, id=None, name=None, version=None, file=None, force=False, encoding=None):
        """Resolve and download a (converted if needed) csv file. Throw an exception if we ask for a raw file"""
        # We first check if we can convert to a csv
        allowed_conversion_type = self._get_allowed_conversion_type_from_dataset_version(
            id=id, name=name, version=version, file=file)
        for conv_type in allowed_conversion_type:
            if conv_type == 'raw':
                raise Exception(
                    "The file is a Raw one, please use instead `download_to_cache` with the same parameters")

        # return a pandas dataframe with the data
        data_id, data_name, data_version, data_file, local_file = self._resolve_and_download(id, name, version, file,
                                                                                             force, format='csv')
        type = self._get_data_file_type(data_name, data_version, data_file)
        if type == "Columnar":
            return pandas.read_csv(local_file, encoding=encoding)
        else:
            return pandas.read_csv(local_file, index_col=0, encoding=encoding)

    def get_short_summary(self, id=None, name=None, version=None, file=None):
        """Get the short summary of a datafile, given the the id/file or name/version/file"""
        if id:
            assert file is not None, "Dataset id must be provided with a specific file"
        if name:
            assert file is not None, \
                "Dataset permaname must be provided with a specific file"
        assert id or name is not None, "Either id or name should be provided, with the corresponding params"

        short_summary = self._get_data_file_summary(id=id, name=name, version=version, file=file)

        return short_summary

    # <editor-fold desc="Upload">

    def upload_session_files(self, upload_file_path_dict):
        # We first create a new session, and fetch its id
        new_session_api_endpoint = "/api/upload_session"

        new_session_id = self.request_get(api_endpoint=new_session_api_endpoint, params=None)

        # We upload the files to S3 following the frontend way of doing it, in Taiga Web
        s3_credentials_endpoint = "/api/credentials_s3"
        s3_credentials = self.request_get(api_endpoint=s3_credentials_endpoint)
        # print(s3_credentials)

        bucket = s3_credentials['bucket']
        partial_prefix = s3_credentials['prefix']
        full_prefix = partial_prefix + new_session_id + "/"

        # Configuration of the Boto3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=s3_credentials['accessKeyId'],
            aws_secret_access_key=s3_credentials['secretAccessKey'],
            aws_session_token=s3_credentials['sessionToken'],
        )

        # We add every temp_datafile in the session
        new_datafile_api_endpoint = "/api/datafile/" + new_session_id

        for upload_file_path, format in upload_file_path_dict.items():
            upload_file_object = UploadFile(prefix=full_prefix, file_path=upload_file_path, format=format)
            print("Uploading {}...".format(upload_file_object.file_name))

            s3_client.upload_file(upload_file_path, bucket,
                                  upload_file_object.prefix_and_file_name)

            S3UploadedData = s3_client.get_object(Bucket=bucket, Key=upload_file_object.prefix_and_file_name)

            # We now organize the conversion and the reupload
            data_create_upload_session_file = dict()
            data_create_upload_session_file = {
                'location': '',
                'eTag': S3UploadedData['ETag'],
                'bucket': str(bucket),
                'key': str(upload_file_object.prefix_and_file_name),
                'filename': upload_file_object.file_name,
                'filetype': upload_file_object.format.name
            }

            current_task_id = self.request_post(api_endpoint=new_datafile_api_endpoint,
                                                data=data_create_upload_session_file)

            task_status_api_endpoint = '/api/task_status/' + current_task_id

            print("Conversion and upload...:")
            task_status = self.request_get(api_endpoint=task_status_api_endpoint)

            while (task_status['state'] != 'SUCCESS' and
                   task_status['state'] != 'FAILURE'):
                print("\t {}".format(task_status['message']))
                time.sleep(1)
                task_status = self.request_get(api_endpoint=task_status_api_endpoint)

            if task_status['state'] == 'SUCCESS':
                print("\n\t Done: {} properly converted and uploaded".format(upload_file_object.file_name))
            else:
                print("\n\t While processing {}, we got this error {}".format(upload_file_object.file_name,
                                                                              task_status.message))

        return new_session_id

    # TODO: Add the creation of a folder, given a path relative to home ('~')
    def create_dataset(self, dataset_name=None, dataset_description=None,
                       upload_file_path_dict=None, folder_id=None):
        # TODO: Add the folder id to put the files into
        """Upload multiples files to Taiga, by default in the Public folder

        :param dataset_name: str
        :param dataset_description: str
        :param upload_file_path_dict: Dict[str, str] => Key is the file_path, value is the format

        :return dataset_id: str
        """
        assert len(upload_file_path_dict) != 0
        if folder_id is None:
            folder_id = 'public'
            prompt = "Warning: Your dataset will be created in Public. Are you sure? y/n (otherwise use folder_id parameter) "
            try:
                user_continue = raw_input(prompt)
            except NameError:
                user_continue = input(prompt)

            if user_continue != 'y':
                return

        new_session_id = self.upload_session_files(upload_file_path_dict=upload_file_path_dict)

        # We create the dataset since all files have been uploaded
        create_dataset_api_endpoint = '/api/dataset'
        # TODO: Get user id from token
        data_create_dataset = {
            'sessionId': new_session_id,
            'datasetName': dataset_name,
            'currentFolderId': folder_id,
            'datasetDescription': dataset_description
        }
        dataset_id = self.request_post(api_endpoint=create_dataset_api_endpoint, data=data_create_dataset)
        print(
            "\nCongratulations! Your dataset `{}` has been created in the {} folder with the id {}. You can directly access to it with this url: {}\n"
                .format(dataset_name, folder_id, dataset_id, self.url + "/dataset/" + dataset_id))

        return dataset_id

    def update_dataset(self, dataset_id=None, dataset_permaname=None, dataset_version=None, dataset_description=None,
                       upload_file_path_dict=None, force_keep=False, force_remove=False):
        """Create a new version of the dataset. By default will be interactive. Use force_keep or force_remove to
        keep/remove the previous files. If using dataset_id, will get the latest dataset version and create a new one
        from it.

        :param dataset_id: str => Id of a dataset, don't use with dataset_permaname/dataset_version
        :param dataset_permaname: str => Permaname of a dataset. Will retrieve latest dataset version if no dataset_version provided
        :param dataset_version: int => version of a dataset. Use with dataset_permaname
        :param dataset_description: str
        :param upload_file_path_dict: Dict[str, str] => Key is the file_path, value is the format
        :param force_keep: boolean
        :param force_remove: boolean

        :return new_dataset_version_id:
        """
        assert (not dataset_id and dataset_permaname) or \
               (dataset_id and not dataset_permaname and not dataset_version)
        assert ((force_keep and not force_remove) or \
                (force_remove and not force_keep) or \
                (not force_keep and not force_remove))
        assert len(upload_file_path_dict) != 0

        dataset_json = None
        dataset_version_json = None
        keep_datafile_id_list = []

        if dataset_permaname and dataset_version:
            # We retrieve the dataset version given
            get_dataset_with_permaname_and_version_api_endpoint = "/api/dataset/" + dataset_permaname + "/" + dataset_version
            result = self.request_get(api_endpoint=get_dataset_with_permaname_and_version_api_endpoint)
            dataset_json = result["dataset"]
            dataset_version_json = result["datasetVersion"]

            dataset_id = dataset_json["id"]
        elif dataset_permaname and not dataset_version:
            # We retrieve the latest dataset version
            get_latest_dataset_version_id_api_endpoint = "/api/dataset/" + dataset_permaname
            result = self.request_get(api_endpoint=get_latest_dataset_version_id_api_endpoint)
            dataset_versions_only_permaname = result['versions']
            get_latest_version_summary = ('', 0)
            for current_version in dataset_versions_only_permaname:
                current_version_number = int(current_version['name'])
                if current_version_number > int(get_latest_version_summary[1]):
                    get_latest_version_summary = (current_version['id'], current_version['name'])

            get_latest_dataset_version_api_endpoint = "/api/dataset/" + dataset_permaname + \
                                                      "/" + get_latest_version_summary[1]
            result = self.request_get(api_endpoint=get_latest_dataset_version_api_endpoint)
            dataset_json = result["dataset"]
            dataset_version_json = result["datasetVersion"]
            dataset_id = dataset_json["id"]
        else:
            get_dataset_with_id_api_endpoint = "/api/dataset/" + dataset_id + "/last"
            result = self.request_get(api_endpoint=get_dataset_with_id_api_endpoint)
            dataset_version_json = result

        datafiles = dataset_version_json['datafiles']

        if not force_keep and not force_remove:
            # We will be interactive at asking which files they want to keep or remove
            print("Now choosing the datasets you would want to keep or remove:")
            for datafile in datafiles:
                prompt = "\tKeep " + datafile['name'] + " ? (y/n) "
                try:
                    keep_answer = raw_input(prompt)
                except NameError:
                    keep_answer = input(prompt)

                if keep_answer == 'y':
                    # Add to the keep list
                    keep_datafile_id_list.append(datafile['id'])
                else:
                    print("\tNot keeping " + datafile['name'])

        if force_keep and not force_remove:
            keep_datafile_id_list = [datafile['id'] for datafile in datafiles]

        if force_remove and not force_keep:
            keep_datafile_id_list = []

        new_session_id = self.upload_session_files(upload_file_path_dict=upload_file_path_dict)

        new_dataset_version_params = dict()
        new_dataset_version_params['sessionId'] = new_session_id
        new_dataset_version_params['datasetId'] = dataset_id

        if dataset_description:
            new_dataset_version_params['newDescription'] = dataset_description
        else:
            new_dataset_version_params['newDescription'] = dataset_version_json['description']

        new_dataset_version_params['datafileIds'] = keep_datafile_id_list

        print("Creating the new version with these files:")
        for new_file_path, format in upload_file_path_dict.items():
            new_file_name = UploadFile.get_file_name(UploadFile.drop_extension(new_file_path))
            print("\tNEW: " + new_file_name + " - " + format)
        for datafile in datafiles:
            if datafile['id'] in keep_datafile_id_list:
                print("\tKEEP: " + datafile["name"] + " - " + datafile["type"])

        new_dataset_version_api_endpoint = "/api/datasetVersion"

        new_dataset_version_id = self.request_post(api_endpoint=new_dataset_version_api_endpoint,
                                                   data=new_dataset_version_params)

        print("\nDataset version with id {} created. You can access to this dataset version directly with this url: {}"
              .format(new_dataset_version_id, self.url + "/dataset_version/" + new_dataset_version_id))

        return new_dataset_version_id

    # </editor-fold>

    # <editor-fold desc="Utilities">
    def request_get(self, api_endpoint, params=None):
        r = requests.get(self.url + api_endpoint, stream=True, params=params,
                         headers=dict(Authorization="Bearer " + self.token))

        if r.status_code == 404:
            raise Exception(
                "Received a not found error. Are you sure about your credentials and/or the data parameters?")
        elif r.status_code != 200:
            raise Exception("Bad status code: {}".format(r.status_code))

        return r.json()

    def request_post(self, api_endpoint, data):
        assert data is not None

        r = requests.post(self.url + api_endpoint, json=data,
                          headers=dict(Authorization="Bearer " + self.token))

        if r.status_code == 404:
            return None
        elif r.status_code != 200:
            raise Exception("Bad status code: {}".format(r.status_code))

        return r.json()
        # </editor-fold>


TaigaClient = Taiga2Client
