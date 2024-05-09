# schema-validator

One of the biggest data quality issues are schema inconsistencies. To reduce the schema issues one can simply
write test cases, that compare the schema of a table after a transformation with the desired schema. However, 
this can quickly become time consuming and lead to syncronization issues between the schema defined in the DDL and 
the schema checked by the unittest. With the SchemaValidator, you can easily use your DDLs as the desired schema 
and let the validator, load, parse, and compare the schemas for you.

## Supported Packages

At the moment, the SchemaValidator only supports reading SQL schemas and only works on Pyspark dataframes. We are
actively working on extending this to directly integrate with data catalogues like the Glue Catalogue from AWS and more.

## Installation

### From Source

* clone source: `git clone git@github.com:Matze99/schema-validator.git`
* download poetry
* install dependencies `poetry install`
* run build `poetry build`
* install package in your env`pip install /path/to/schema_validator-0.1.0-py3-none-any.whl`

## Usage

```python
from schema_validator import SchemaStore, PysparkValidatorFactory

schema_store = SchemaStore("path/to/ddl/files")

df = spark_session.read.csv("path/to/csv", header=True, inferSchema=True)

table_schema = schema_store.get_table_schema("schema.table")
validator = PysparkValidatorFactory.get_validator(table_schema)

is_valid, message = validator.validate(df)

print(f"schema check successful {is_valid} with message {message}")
```

## Limitations

* alter is only supported for defining primary and foreign keys
* only sql ddl files are supported
* only pyspark can be currently validated





