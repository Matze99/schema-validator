# schema-validator

One of the biggest data quality issues are schema inconsistencies. To reduce the schema issues one can simply
write test cases, that compare the schema of a table after a transformation with the desired schema. However, 
this can quickly become time consuming and lead to syncronization issues between the schema defined in the DDL and 
the schema checked by the unittest. With the SchemaValidator, you can easily use your DDLs as the desired schema 
and let the validator, load, parse, and compare the schemas for you.

## Supported Packages

At the moment, the SchemaValidator only supports reading SQL schemas and only works on Pyspark dataframes. We are
actively working on extending this to directly integrate with data catalogues like the Glue Catalogue from AWS and more.

## Usage

## Make DQ Dimensions 
https://icedq.com/6-data-quality-dimensions




