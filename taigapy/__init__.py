import requests
import pandas
import os
import tempfile
import time

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

        r = requests.get(self.url+"/rest/v0/namedDataset", params=params)
        if r.status_code == 404:
            return None
        return r.text

    def get(self, id=None, name=None, version=None):
        if id is None:
            assert name is not None, "id or name must be specified"
            id = self.get_dataset_id_by_name(name, version=version)
            if id is None:
                return None
                
        local_file = os.path.join(self.cache_dir, id+".csv")
        if not os.path.exists(local_file):
            if not os.path.exists(self.cache_dir):
                os.makedirs(self.cache_dir)

            r = requests.get(self.url+"/rest/v0/datasets/"+id+"?format=tabular_csv", stream=True)
            if r.status_code == 404:
                return None
            
            if r.status_code != 200:
                # hack: If this couldn't be fetched as tabular_csv try just csv
                r = requests.get(self.url+"/rest/v0/datasets/"+id+"?format=csv", stream=True)
                assert r.status_code == 200
                    
            with tempfile.NamedTemporaryFile(dir=self.cache_dir, suffix=".tmpdl", delete=False) as fd:
                for chunk in r.iter_content(chunk_size=100000):
                    fd.write(chunk)
            os.rename(fd.name, local_file)
        
        return pandas.read_csv(local_file)





class Taiga2Client:
    def __init__(self, url="https://cds.team/taiga", cache_dir="~/.taiga", token_path=None):
        self.url = url
        self.cache_dir = os.path.expanduser(cache_dir)
        if token_path is None:
            token_path = os.path.join(self.cache_dir, "token")
        if not os.path.exists(token_path):
            raise Exception("No token file: {}".format(token_path))
        with open(token_path, "rt") as r:
            self.token = r.readline().strip()

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

    def _get_data_file_json(self, id, name, version, file, force, format):
        params = dict(format=format)

        if id is not None:
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

        r = requests.get(self.url + "/api/datafile", stream=True, params=params, headers=dict(Authorization="Bearer "+self.token))
        if r.status_code == 404:
            return None
        elif r.status_code != 200:
            raise Exception("Bad status code: {}".format(r.status_code))

        return r.json()

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

            for block in r.iter_content(1024*100):
                handle.write(block)

    def get(self, id=None, name=None, version=None, file=None, force=False):
        if id is None:
            assert name is not None, "id or name must be specified"

        metadata = self._get_data_file_json(id, name, version, file, force, "metadata")
        if metadata is None:
            return None

        data_id = metadata['dataset_version_id']
        data_name = metadata['dataset_permaname']
        data_version = metadata['dataset_version']
        data_file = metadata["datafile_name"]

        assert data_id is not None
        assert data_name is not None
        assert data_version is not None
        assert data_file is not None

        local_file = os.path.join(self.cache_dir, data_id + ".csv")
        if not os.path.exists(local_file):
            if not os.path.exists(self.cache_dir):
                os.makedirs(self.cache_dir)

            with tempfile.NamedTemporaryFile(dir=self.cache_dir, suffix=".tmpdl", delete=False) as fd:
    #            def _dl_file(self, id, name, version, file, force, destination):

                self._dl_file(data_id, None, None, data_file, False, "csv", fd.name)
            os.rename(fd.name, local_file)

        return pandas.read_csv(local_file)

TaigaClient = Taiga2Client