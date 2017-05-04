# taigapy
Library for reading from taiga in python

run

```
python setup.py install
```

You can then fetch from taiga in python.  Example:

```
from taigapy import TaigaClient

c = TaigaClient(cache_dir=cache_dir)

# fetch by ID
df = c.get(id='6d9a6104-e2f8-45cf-9002-df3bcedcb80b')

# fetch by name
df1 = c.get(name='achilles-v2-4-6', version=4)

# download to destination
destination_dir = '/directory/that/exists'
file_path = c.download(id='6d9a6104-e2f8-45cf-9002-df3bcedcb80b', format='hdf5', destination=destination_dir)
```

Confluence: https://confluence.broadinstitute.org/display/CPDS/Taiga