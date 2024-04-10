import os
import sys

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, VarcharType, FloatType, \
    DataTypeSingleton, DecimalType
from sqlglot.expressions import DataType

from schema_validator.model.column_schema import ColumnSchema
from schema_validator.model.table_schema import TableSchema
from schema_validator.validator.pyspark_validator import PysparkValidator, PysparkValidatorFactory

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    yield spark
    spark.sparkContext.stop()
    spark.stop()


@pytest.mark.parametrize(
    "desired_schema, actual_schema, values, is_equal",
    [
        (
            StructType(
                [
                    StructField("firstname", StringType(), True),
                    StructField("middlename", StringType(), True),
                    StructField("lastname", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("gender", StringType(), True),
                    StructField("salary", IntegerType(), True),
                ]
            ),
            StructType(
                [
                    StructField("firstname", StringType(), True),
                    StructField("middlename", StringType(), True),
                    StructField("lastname", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("gender", StringType(), True),
                    StructField("salary", IntegerType(), True),
                ]
            ),
            [
                ("John", "Doe", "Smith", "1", "M", 5000),
                ("Jane", "Doe", "Smith", "2", "F", 6000),
            ],
            True,
        ),
        (
            StructType(
                [
                    StructField("firstname", StringType(), True),
                    StructField("middlename", StringType(), True),
                    StructField("lastname", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("gender", StringType(), True),
                ]
            ),
            StructType(
                [
                    StructField("firstname", StringType(), True),
                    StructField("middlename", StringType(), True),
                    StructField("lastname", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("gender", StringType(), True),
                    StructField("salary", IntegerType(), True),
                ]
            ),
            [
                ("John", "Doe", "Smith", "1", "M", 5000),
                ("Jane", "Doe", "Smith", "2", "F", 6000),
            ],
            False,
        ),
        (
            StructType(
                [
                    StructField("firstname", IntegerType(), True),
                    StructField("middlename", StringType(), True),
                    StructField("lastname", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("gender", StringType(), True),
                ]
            ),
            StructType(
                [
                    StructField("firstname", StringType(), True),
                    StructField("middlename", StringType(), True),
                    StructField("lastname", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("gender", StringType(), True),
                ]
            ),
            [
                ("John", "Doe", "Smith", "1", "M"),
                ("Jane", "Doe", "Smith", "2", "F"),
            ],
            False,
        ),
        (
            StructType(
                [
                    StructField("firstname", VarcharType(1), True),
                    StructField("middlename", StringType(), True),
                    StructField("lastname", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("gender", StringType(), True),
                ]
            ),
            StructType(
                [
                    StructField("firstname", StringType(), True),
                    StructField("middlename", StringType(), True),
                    StructField("lastname", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("gender", StringType(), True),
                ]
            ),
            [
                ("John", "Doe", "Smith", "1", "M"),
                ("Jane", "Doe", "Smith", "2", "F"),
            ],
            True,
        ),
    ],
)
def test_pyspark_validator(
    spark_session: SparkSession,
    desired_schema: StructType,
    actual_schema: StructType,
    values: list[tuple],
    is_equal: bool,
):
    table = spark_session.createDataFrame(data=values, schema=actual_schema)
    results = table.cache().collect()

    validator = PysparkValidator(desired_schema)

    result, _ = validator.validate(table)
    assert result == is_equal


@pytest.mark.parametrize(
    "type_, pyspark_type, size",
    [
        (DataType.Type.TEXT, StringType(), None),
        (DataType.Type.VARCHAR, VarcharType(10), 10),
        (DataType.Type.INT, IntegerType(), None),
        (DataType.Type.FLOAT, FloatType(), None),
    ],
)
def test_pyspark_validator_factory_convert_type(type_, pyspark_type, size):
    converted_type = PysparkValidatorFactory._convert_type_to_pyspark(type_, size)
    assert converted_type == pyspark_type


@pytest.mark.parametrize(
    "column_schema, struct_field",
    [
        (
            ColumnSchema("Id", DataType.Type.INT, False, True, False),
            StructField("Id", IntegerType(), False)
        ),
        (
            ColumnSchema(
                "Name",
                DataType.Type.VARCHAR,
                False,
                False,
                False,
                type_args={"size": 50},
            ),
            StructField("Name", VarcharType(50), False)
        ),
        (
            ColumnSchema(
                "Example",
                DataType.Type.DECIMAL,
                False,
                False,
                False,
                type_args={"size": (50, 3)},
            ),
            StructField("Example", DecimalType(50, 3), False)
        ),
        (
            ColumnSchema("Price", DataType.Type.INT, True, False, False),
            StructField("Price", IntegerType(), True)
        ),
        (
            ColumnSchema(
                "Percent",
                DataType.Type.FLOAT,
                True,
                False,
                False,
                # type_args={"size": (30, 5)},
            ),
            StructField("Percent", FloatType(), True)
        )
    ]
)
def test_pyspark_validator_factory_convert_column_to_pyspark(column_schema: ColumnSchema, struct_field: StructField):
    converted_column = PysparkValidatorFactory._convert_column_to_pyspark(column_schema)
    assert converted_column == struct_field


@pytest.mark.parametrize(
    "schema, column_schemas",
    [
        (
            StructType([
                StructField("Id", IntegerType(), False),
                StructField("Name", VarcharType(50), False),
                StructField("Price", IntegerType(), True),
                StructField("Percent", FloatType(), True)
            ]),
            [
                ColumnSchema("Id", DataType.Type.INT, False, True, False),
                ColumnSchema(
                    "Name",
                    DataType.Type.VARCHAR,
                    False,
                    False,
                    False,
                    type_args={"size": 50},
                ),
                ColumnSchema("Price", DataType.Type.INT, True, False, False),
                ColumnSchema(
                    "Percent",
                    DataType.Type.FLOAT,
                    True,
                    False,
                    False,
                    # type_args={"size": (30, 5)},
                ),
            ]
        )
    ]
)
def test_pyspark_validator_factory_get_validator(schema: StructType, column_schemas: list[ColumnSchema]):
    table_schema = TableSchema("example")
    for column_schema in column_schemas:
        table_schema.add_column(column_schema)

    validator = PysparkValidatorFactory.get_validator(table_schema)
    assert validator.schema == schema
