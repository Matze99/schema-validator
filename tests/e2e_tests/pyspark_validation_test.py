import os
import sys

import pytest
from pyspark.sql import DataFrame, SparkSession
import pydeequ

from schema_validator.ddl.schema_store import SchemaStore
from schema_validator.validator.pyspark_validator import PysparkValidatorFactory

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

prefix = "."


@pytest.fixture(scope="session")
def schema_store():
    _schema_store = SchemaStore(f"{prefix}/data/test-ddls/simple-example-ddl.sql")
    return _schema_store


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    spark = (SparkSession.builder.master("local[*]")
             .config("spark.jars.packages", pydeequ.deequ_maven_coord)
             .config("spark.jars.excludes", pydeequ.f2j_maven_coord).getOrCreate())
    yield spark
    spark.sparkContext.stop()
    spark.stop()


@pytest.mark.parametrize(
    "desired_is_valid,csv_dir_path,table_schema_name",
    [
        (True, f"{prefix}/data/test-csv/simple-books.csv", "Books1"),
        (False, f"{prefix}/data/test-csv/simple-books.csv", "Books2"),
        (False, f"{prefix}/data/test-csv/simple-books.csv", "Books3"),
    ],
)
def test_pyspark_validator(
        spark_session: SparkSession,
        schema_store: SchemaStore,
        desired_is_valid: bool,
        csv_dir_path: str,
        table_schema_name: str,
):
    df: DataFrame = spark_session.read.csv(csv_dir_path, header=True, inferSchema=True)

    table_schema = schema_store.get_table_schema(table_schema_name)
    validator = PysparkValidatorFactory.get_validator(table_schema, spark_session)

    is_valid, _ = validator.validate(df)
    assert desired_is_valid == is_valid
