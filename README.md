# taigapy
Library for reading from taiga in python

## Token set up

First, you need to get your authorization token so the client library can make requests on your behalf.   Go to:

https://cds.team/taiga/token/

And click on the "Copy" button to copy your token. Paste the token into a file named `~/.taiga/token`

## Installing Taigapy

run 

```
pip install taigapy
```

## Running Taigapy

run

```
python setup.py install
```

You can then fetch from taiga in python.  Example:

```
from taigapy import TaigaClient

c = TaigaClient()

# fetch by ID
df = c.get(id='6d9a6104-e2f8-45cf-9002-df3bcedcb80b')

# fetch by name
df1 = c.get(name='achilles-v2-4-6', version=4)

# fetch a specific data file
df = c.get(name='taigr-data-40f2', version=1, file="tiny_table")

# name and version can serve as the id using name.version
df = c.get(id='achilles-v2-4-6.4')

# the file can also be specified in the id using name.version/file
# id/file (as in 6d9a6104-e2f8-45cf-9002-df3bcedcb80b/tiny_table) is also not supported in either
df = c.get(id='taigr-data-40f2.1/tiny_table')

```

You can also upload data into taiga (see below for available formats). Example:

- Create a new dataset in folder public (you can find the folder_id in the url of Taiga web)

```python
from taigapy import TaigaClient

c = TaigaClient()

# Create a new dataset in public
c.create_dataset(dataset_name='My Dataset Name',
    dataset_description='My Dataset Description',
    upload_file_path_dict={'file_one_path': 'format'}, folder_id='public')
```

- Update a dataset with new files, interactively, in public folder (default)

```python
from taigapy import TaigaClient

c = TaigaClient()
c.update_dataset(dataset_id=dataset_id, upload_file_path_dict={'file_updated_or_new_path': 'format'},
                 dataset_description="Interactive test")

```

- Update a dataset with new files, keeping all previous files, in a specific folder:

```python
from taigapy import TaigaClient

c = TaigaClient()
c.update_dataset(dataset_id=dataset_id, upload_file_path_dict={'file_new_path': 'format'},
                 dataset_description="Force Keep",
                 force_keep=True)
```

- Update a dataset with new files, removing all previous files, in a specific folder:

```python
from taigapy import TaigaClient

c = TaigaClient()
c.update_dataset(dataset_id=dataset_id, upload_file_path_dict={'file_updated_or_new_path': 'format'},
                 dataset_description="Force Remove",
                 force_remove=True)
```

- Update a dataset with new files, based on its permaname and version

```python
from taigapy import TaigaClient

c = TaigaClient()
c.update_dataset(dataset_permaname=dataset_permaname, dataset_version=2,
                 upload_file_path_dict={'file_updated_or_new_path': 'format'},
                 dataset_description="Update a specific version")
```

- Update a dataset with new files, based on its permaname only (will update from the latest version)

```python
from taigapy import TaigaClient

c = TaigaClient()
c.update_dataset(dataset_permaname=dataset_permaname,
                 upload_file_path_dict={'file_updated_or_new_path': 'format'},
                 dataset_description="Update from latest")
```

### Available formats

Formats available currently are:

- NumericMatrixCSV
- NumericMatrixTSV
- TableCSV
- TableTSV
- GCT
- Raw


Confluence: https://confluence.broadinstitute.org/display/CPDS/Taiga