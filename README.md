# taigapy
![Run tests](https://github.com/broadinstitute/taigapy/workflows/Run%20tests/badge.svg)

The Taiga client is a Python library for interacting with [Taiga](https://github.com/broadinstitute/taiga), a service designed for storing and versioning datasets used in data science workflows. The client provides functionality for uploading, downloading, and managing datasets and their versions.

See [taigr](https://github.com/broadinstitute/taigr) for the R client.

## Table of Contents
- [taigapy](#taigapy)
  - [Table of Contents](#table-of-contents)
  - [Quickstart](#quickstart)
    - [Prerequisites](#prerequisites)
    - [Installing](#installing)
    - [Sample](#sample)
  - [Development](#development)
    - [Setup](#setup)
    - [Running Tests](#running-tests)
    - [Versioning and Publishing Taigapy](#versioning-and-publishing-taigapy)
    - [Commit Conventions](#commit-conventions)
      - [Pull requests](#pull-requests)
    - [Installing taigapy from the Google Artifact Registry using poetry](#installing-taigapy-from-the-google-artifact-registry-using-poetry)


## Quickstart

### Prerequisites
First, you need to get your authorization token so the client library can make requests on your behalf. Go to https://cds.team/taiga/token/ and click on the "Copy" button to copy your token. Paste your token in a file at `~/.taiga/token`.

```bash
mkdir ~/.taiga/
echo YOUR_TOKEN_HERE > ~/.taiga/token
```

### Installing
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install taigapy from Google Artifact registry. 

```
pip install --extra-index-url=https://us-central1-python.pkg.dev/cds-artifacts/public-python/simple/ taigapy
```

### Sample
The below is a short example of fetching data from taiga into a pandas data frame.

```
from taigapy import create_taiga_client_v3

# instantiate the client
tc = create_taiga_client_v3()

# download the table to the cache on disk (if not already there) and then load it into memory
df = tc.get("hgnc-gene-table-e250.3/hgnc_complete_set")
```

See [docs](docs/) for the complete documentation on how to use the client.

## Development
### Setup

Run `poetry install`

Then you can run `poetry shell` to get an environment with the module
installed.

### Running Tests
The fetch (i.e. `get`, `download_to_cache`, `get_dataset_metadata`, etc.) will run against the production Taiga server. The create and update dataset tests will run against your locally hosted Taiga.

To run the fetch tests, run `pytest`.

To run all the tests, [set up Taiga locally](https://github.com/broadinstitute/taiga#installing), then run `pytest --runlocal`.

### Versioning and Publishing Taigapy
### Commit Conventions
We use a tool called [commitizen-tools/commitizen](https://github.com/commitizen-tools/commitizen) for versioning. The way commitizen works is by checking if there are any new commits that follow the formatting rules defined in our `pyproject.toml`'s `bump_pattern` and `bump_map` variables. By default, commitizen uses [conventional commits](https://www.conventionalcommits.org/), however, we have selected a subset of rules to fit most of our current use cases.

In general, when making commits, especially directly to master, please try to adhere to our defined rules so we can ensure versioning is being updated properly:

- fix: COMMIT_MESSAGE -> Correlates with PATCH in SemVer
- build: COMMIT_MESSAGE -> Correlates with PATCH in SemVer
- chore: COMMIT_MESSAGE -> Correlates with PATCH in SemVer
- feat: COMMIT_MESSAGE -> Correlates with MINOR in SemVer
- fix!: COMMIT_MESSAGE -> Correlates with MAJOR in SemVer
- build!: COMMIT_MESSAGE -> Correlates with MAJOR in SemVer
- chore!: COMMIT_MESSAGE -> Correlates with MAJOR in SemVer
- feat!: COMMIT_MESSAGE -> Correlates with MAJOR in SemVer

In addition, we also have `test`, `refactor`, `style`, `docs`, `perf`, `ci` commit types available. While these commit types are not used for determining versioning, they may be helpful in helping organize our commits more.

If these rules are hard to remember, you can also use commitizen's CLI to help format your commits by calling:

    cz c

Instead of

    git commit -m "feat: New feature"

#### Pull requests

Pull request titles with master as target branch should also adhere to our defined rules for commits, especially for squash merges. This is because on Github, we will ultimately use the pull request title as the default commit message.

**NOTE: Our CI/CD pipeline includes a Github actions workflow `run_tests_autobump.yml` that auto-versions and publishes taigapy client. The below instructions are only for if you want to publish locally though this is not recommended!**
Note: this will publish the resulting module to an internal package repo. Before you do this,
you'll need to set yourself up to be able to publish to `python-public`:

To setup for publishing (Based on https://medium.com/google-cloud/python-packages-via-gcps-artifact-registry-ce1714f8e7c1 )

```
poetry self add keyrings.google-artifactregistry-auth
poetry config repositories.public-python https://us-central1-python.pkg.dev/cds-artifacts/public-python/
# also make sure you've authentication via "gcloud auth login" if you haven't already
```


### Installing taigapy from the Google Artifact Registry using poetry

To install taigapy using poetry in your repo run the following:
1. Update your poetry version, and install keyring, and the GCP Artifact Registry backend in the core poetry virtual environment: `poetry self update && poetry self add keyrings.google-artifactregistry-auth`
2. Note that you may need to authenticate with gcloud application by running `gcloud auth application-default login` if you are not already authenticated. 
3. Configure the package source as an explicit package source for your project: `poetry source add --priority=explicit gcp-artifact-registry https://us-central1-python.pkg.dev/cds-artifacts/public-python/simple`
4. Add the python package: `poetry add --source gcp-artifact-registry taigapy`

This should install taigapy from the CDS' internal public-python atrifact registry in your poetry env.
