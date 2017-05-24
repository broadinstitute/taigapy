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

# fetch a specific data file
df = c.get(name='taigr-data-40f2', version=1, file="tiny_table")

# name and version can serve as the id using name.version
df = c.get(id='achilles-v2-4-6.4')

# the file can also be specified in the id using name.version/file
# not supported in taigr (as of 052417)
# id/file (as in 6d9a6104-e2f8-45cf-9002-df3bcedcb80b/tiny_table) is also not supported in either
df = c.get(id='taigr-data-40f2.1/tiny_table')

```

Confluence: https://confluence.broadinstitute.org/display/CPDS/Taiga