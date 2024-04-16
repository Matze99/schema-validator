from pyspark.sql.types import DataType as SparkDataType
from pyspark.sql.types import (
    CharType,
    VarcharType,
    BinaryType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    ArrayType,
    MapType,
    StructType,
)

from schema_validator.model.column_schema import DataType

PYSPARK_TYPE_MAPPING: dict[DataType, SparkDataType] = dict(
    [  # type: ignore
        (DataType.CHAR, CharType),
        (DataType.VARCHAR, VarcharType),
        (DataType.BINARY, BinaryType),
        (DataType.VARBINARY, BinaryType),
        (DataType.TEXT, StringType),
        (DataType.DATE, DateType),
        (DataType.TIME, TimestampType),
        (DataType.DATETIME, TimestampType),
        (DataType.TIMESTAMP, TimestampType),
        (DataType.BIT, BooleanType),
        (DataType.TINYINT, ShortType),
        (DataType.INT, IntegerType),
        (DataType.SMALLINT, ShortType),
        (DataType.MEDIUMINT, IntegerType),
        (DataType.FLOAT, FloatType),
        (DataType.DOUBLE, DoubleType),
        (DataType.DECIMAL, DecimalType),
        (DataType.NVARCHAR, VarcharType),
        (DataType.ARRAY, ArrayType),
        (DataType.MAP, MapType),
        (DataType.STRUCT, StructType),
        (DataType.BIGINT, LongType),
        (DataType.BOOLEAN, BooleanType),
    ]
)
