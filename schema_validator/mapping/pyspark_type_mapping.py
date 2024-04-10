from sqlglot.expressions import DataType
from pyspark.sql.types import DataType as SparkDataType
from pyspark.sql.types import (CharType, VarcharType, BinaryType, BooleanType, DateType, TimestampType, DecimalType,
                               DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, ArrayType, MapType,
                               StructType)


PYSPARK_TYPE_MAPPING: dict[DataType.Type, SparkDataType] = dict([
        (DataType.Type.CHAR, CharType),
        (DataType.Type.VARCHAR, VarcharType),
        (DataType.Type.BINARY, BinaryType),
        (DataType.Type.VARBINARY, BinaryType),
        (DataType.Type.TEXT, StringType),
        (DataType.Type.DATE, DateType),
        (DataType.Type.TIME, TimestampType),
        (DataType.Type.DATETIME, TimestampType),
        (DataType.Type.TIMESTAMP, TimestampType),
        (DataType.Type.BIT, BooleanType),
        (DataType.Type.TINYINT, ShortType),
        (DataType.Type.INT, IntegerType),
        (DataType.Type.SMALLINT, ShortType),
        (DataType.Type.MEDIUMINT, IntegerType),
        (DataType.Type.FLOAT, FloatType),
        (DataType.Type.DOUBLE, DoubleType),
        (DataType.Type.DECIMAL, DecimalType),
        (DataType.Type.NVARCHAR, VarcharType),
        (DataType.Type.ARRAY, ArrayType),
        (DataType.Type.MAP, MapType),
        (DataType.Type.STRUCT, StructType),
        (DataType.Type.BIGINT, LongType),
        (DataType.Type.BOOLEAN, BooleanType),
])
