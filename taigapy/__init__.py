import requests
import pandas
import os
import tempfile

class TaigaClient:
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
        
