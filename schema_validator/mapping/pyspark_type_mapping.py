from pyspark.sql.types import DataType as SparkDataType
import pyspark
from pyspark.sql.types import (
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
        (DataType.ARRAY, ArrayType),
        (DataType.MAP, MapType),
        (DataType.STRUCT, StructType),
        (DataType.BIGINT, LongType),
        (DataType.BOOLEAN, BooleanType),
        (DataType.NVARCHAR, StringType),
        (DataType.CHAR, StringType),
        (DataType.VARCHAR, StringType),
    ]
)

if pyspark.__version__ >= "3.5.0":
    from pyspark.sql.types import CharType, VarcharType

    # noinspection PyTypeChecker
    PYSPARK_TYPE_MAPPING.update(
        dict(
            [
                # type: ignore
                (DataType.NVARCHAR, VarcharType),
                (DataType.CHAR, CharType),
                (DataType.VARCHAR, VarcharType),
            ]
        )
    )
