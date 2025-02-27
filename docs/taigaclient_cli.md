# Taiga CLI Tool Documentation

A command-line interface for interacting with Taiga datasets and files.


## Commands

### copy

Copy files from a source dataset to a new destination dataset using Taiga references. This creates a new dataset that references the original files without duplicating the data.

#### Usage:

```
taigaclient copy <source-dataset> <destination-dataset>
```

#### Examples:

**Copy a dataset:**
```
taigaclient copy hgnc-gene-table-e250 "new_gene_table"
```


