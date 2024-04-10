import pytest
from sqlglot.expressions import DataType

from schema_validator.model.column_schema import data_type_has_attributes


@pytest.mark.parametrize(
    "type_,has_size",
    [
        (DataType.Type.INT, True),
        (DataType.Type.BIGINT, True),
        (DataType.Type.FLOAT, True),
        (DataType.Type.DOUBLE, True),
        (DataType.Type.DECIMAL, True),
        (DataType.Type.VARCHAR, True),
        (DataType.Type.CHAR, True),
        (DataType.Type.TEXT, True),
        (DataType.Type.BINARY, True),
        (DataType.Type.VARBINARY, True),
        (DataType.Type.DATE, True),
        (DataType.Type.TIME, True),
        (DataType.Type.TIMESTAMP, True),
        (DataType.Type.INTERVAL, False),
        (DataType.Type.BOOLEAN, False),
        (DataType.Type.ARRAY, True),
        (DataType.Type.MAP, True),
        (DataType.Type.STRUCT, True),
        (DataType.Type.NULL, False),
        (DataType.Type.UNKNOWN, False),
        (DataType.Type.VARCHAR, True),
        (DataType.Type.NVARCHAR, True),
        (DataType.Type.TINYINT, True),
        (DataType.Type.SMALLINT, True),
        (DataType.Type.MEDIUMINT, True),
        (DataType.Type.BIT, True),
        (DataType.Type.DATETIME, True),
    ],
)
def test_data_type_has_attributes(type_: DataType.Type, has_size: bool):
    assert data_type_has_attributes(type_) == has_size
