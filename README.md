# taigapy
Library for reading from taiga in python

See [here](https://confluence.broadinstitute.org/display/CPDS/Taiga) for installing taigr, the library for reading from taiga in R

## Token set up

First, you need to get your authorization token so the client library can make requests on your behalf. Go to https://cds.team/taiga/token/ and click on the "Copy" button to copy your token. Paste your token in a file at `~/.taiga/token`.

```
mkdir ~/.taiga/
echo YOUR_TOKEN_HERE > ~/.taiga/token
```

## Installing Taigapy

If you are only using Taigapy and not making modifications to it, run

```
pip install taigapy
```

If you are developing Taigapy, check out the repo and run

```
python setup.py develop
```

## Use Taigapy

You can now fetch from taiga in python.  

### Main methods

#### Download

- If you need a specific file (table or matrix) from a specific dataset version, use `.get` method
- If you need all the files from a specific dataset version, use `.get_all` method
- If you need a raw file, we will give you the path to it with `.download_to_cache` method since we don't know what the format of your file is

Example:

```python
from taigapy import TaigaClient

tc = TaigaClient() # These two steps could be merged in one with `from taigapy import default_tc as tc`

# fetch by ID a full dataset
df = tc.get(id='6d9a6104-e2f8-45cf-9002-df3bcedcb80b')

# fetch by name a full version of a dataset
df1 = tc.get(name='achilles-v2-4-6', version=4)

# fetch a specific data file
# If Raw file, use download_to_cache, which will give you the path of the file
raw_path = tc.download_to_cache(name='taigr-data-40f2', version=3, file="raw_file")

# Else, if CSV convertible
df = tc.get(name='taigr-data-40f2', version=1, file="tiny_table")

# name and version can serve as the id using name.version
df = tc.get(id='achilles-v2-4-6.4')

# the file can also be specified in the id using name.version/file
# id/file (as in 6d9a6104-e2f8-45cf-9002-df3bcedcb80b/tiny_table) is also not supported in either
df = tc.get(id='taigr-data-40f2.1/tiny_table')

```

#### Upload

You can also upload data into taiga (see below for available formats). Methods are:
- Create a dataset with `create_dataset`
- Update a dataset with `update_dataset`

Example:

- Create a new dataset in folder public (you can find the folder_id in the url of Taiga web)

```python
from taigapy import TaigaClient

tc = TaigaClient()

# Create a new dataset in public
tc.create_dataset(dataset_name='My Dataset Name',
    dataset_description='My Dataset Description',
    upload_file_path_dict={'file_one_path': 'format'}, folder_id='public')
```

- Update a dataset with new files, interactively, in public folder (default)

```python
from taigapy import TaigaClient

tc = TaigaClient()
tc.update_dataset(dataset_id=dataset_id, upload_file_path_dict={'file_updated_or_new_path': 'format'},
                 dataset_description="Interactive test")

```

- Update a dataset with new files, keeping all previous files, in a specific folder:

```python
from taigapy import TaigaClient

tc = TaigaClient()
tc.update_dataset(dataset_id=dataset_id, upload_file_path_dict={'file_new_path': 'format'},
                 dataset_description="Force Keep",
                 force_keep=True)
```

- Update a dataset with new files, removing all previous files, in a specific folder:

```python
from taigapy import TaigaClient

tc = TaigaClient()
tc.update_dataset(dataset_id=dataset_id, upload_file_path_dict={'file_updated_or_new_path': 'format'},
                 dataset_description="Force Remove",
                 force_remove=True)
```

- Update a dataset with new files, based on its permaname and version

```python
from taigapy import TaigaClient

tc = TaigaClient()
tc.update_dataset(dataset_permaname=dataset_permaname, dataset_version=2,
                 upload_file_path_dict={'file_updated_or_new_path': 'format'},
                 dataset_description="Update a specific version")
```

- Update a dataset with new files, based on its permaname only (will update from the latest version)

```python
from taigapy import TaigaClient

tc = TaigaClient()
tc.update_dataset(dataset_permaname=dataset_permaname,
                 upload_file_path_dict={'file_updated_or_new_path': 'format'},
                 dataset_description="Update from latest")
```

#### Virtual dataset creation and update
Requires version 2.8.1 of taigapy

```python
from taigapy import TaigaClient
tc = TaigaClient()
tc.update_virtual_dataset('internal-19q2-9504', new_aliases=[('CCLE_gene_cn', 'segmented-cn-wes-prioritzed-7fe1.25/CCLE_internal_19q2_gene_cn')])


To create a virtual dataset:
tc.create_virtual_dataset(name="internal_19Q2", description="The DepMap internal 19Q2 release", aliases=[
    ("CCLE_gene_cn", "segmented-cn-wes-prioritzed-7fe1.24/CCLE_internal_19q2_gene_cn"),
    ("CCLE_segmented_cn", "segmented-cn-wes-prioritzed-7fe1.24/CCLE_internal_19q2_segmented_cn")
], folder_id="21eeb52951984bfa9219b2c251c27df3")
```

### Available formats

Formats available currently are:

- NumericMatrixCSV
- NumericMatrixTSV
- TableCSV
- TableTSV
- GCT
- Raw

## Running Taigapy via Command line

Run `python -m taigapy -h` to have an up to date help.

### Create a new dataset

`python -m taigapy create -n dataset_name -f {'file_path_one': 'format', ...}`

### Update an existing dataset 

`python -m taigapy update -p dataset_permaname -v dataset_version -f {'file_path_one': 'format', ...}`

### Get a dataset from Taiga

`python -m taigapy get -p dataset_permaname -v dataset_version -f file_name -t format`

[Important] Please choose a format available for this specific file in taiga Web UI


## Publish Taigapy on pypi
`pip install twine` (not to be confused with the interactive fiction software called twine)

Execute: `publish_new_taigapy_pypi.sh` which will do the following:

1. `rm -r dist/`
2. `python setup.py bdist_wheel --universal`
3. `twine upload dist/*`

## More Taigapy information:

Confluence: https://confluence.broadinstitute.org/display/CPDS/Taiga

## Running tests:
`pytest`