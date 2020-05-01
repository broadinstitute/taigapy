# Taiga and taigapy terms and definitions

## DataFile Formats
When uploading a file to Taiga, a _format_ must be specified. These can be one of the following:
- NumericMatrixCSV\
  CSV with a header row, a header column, and values that are all numbers or nans. Column and row names must be nonempty strings.
- TableCSV\
  CSV with a header row and values that are numbers, booleans, strings, or nans. Column and row names must be nonempty strings. Note that numbers will be converted to `float` and all other column types will be converted to `string`. Please make sure to do any necessary type conversions before using the DataFrames returned from `TaigaClient.get`.
- Raw\
  Raw file (not parsed).

## DataFile Type
Once uploaded, datafiles in Taiga have an associated _type_ based on the inital _format_. The names are historical, and no longer representative of how files are stored internally.
- HDF5\
  NumericMatrixCSV datafiles
- Columnar\
  TableCSV datafiles
- Raw\
  Raw datafiles

## CSV
A comma-separated values file. Valid CSVs must adhere to the following rules:
- All column names/headers must be unique. For NumericMatrix files, they must also be nonempty.
- No comments should be present.
- Fields with commas in the value must be wrapped in quotes.
Some other notes and caveats:
- All column names will be parsed as strings

## Column type inference
For TableCSV/Columnar files, Taiga will attempt to infer the type of all the columns according to the following rules:
- Fields with values  in `["", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "n/a", "nan", "null"]` will be treated as NA. This is consistent with pandas's default interpretation of CSVs.
- If all values can be parsed as floats, the column with be assigned type float.
- Otherwise, the column will be assigned type string.
When fetching a file using taigapy (or taigr), the DataFrame returned will have these types. Programs using the DataFrame should cast types as appropriate.
