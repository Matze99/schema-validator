from dataclasses import dataclass, field

from sqlglot.expressions import DataType


def data_type_has_attributes(type_: DataType.Type) -> bool:
    return type_ in (
        DataType.Type.CHAR,
        DataType.Type.VARCHAR,
        DataType.Type.BINARY,
        DataType.Type.VARBINARY,
        DataType.Type.TEXT,
        DataType.Type.DATE,
        DataType.Type.TIME,
        DataType.Type.DATETIME,
        DataType.Type.TIMESTAMP,
        DataType.Type.BIT,
        DataType.Type.TINYINT,
        DataType.Type.INT,
        DataType.Type.SMALLINT,
        DataType.Type.MEDIUMINT,
        DataType.Type.FLOAT,
        DataType.Type.DOUBLE,
        DataType.Type.DECIMAL,
        DataType.Type.NVARCHAR,
        DataType.Type.ARRAY,
        DataType.Type.MAP,
        DataType.Type.STRUCT,
        DataType.Type.BIGINT,
    )


@dataclass
class ColumnSchema:
    name: str
    type_: DataType.Type
    nullable: bool
    primary_key: bool
    foreign_key: bool
    unique: bool = False
    type_args: dict[str, int | tuple[int, int]] = field(default_factory=dict[str, int | tuple[int, int]])

    def __eq__(self, other):
        if not isinstance(other, ColumnSchema):
            return False

        if len(self.type_args) != len(other.type_args):
            return False

        self_type_args = sorted(self.type_args.items(), key=lambda item: item[0])
        other_type_args = sorted(other.type_args.items(), key=lambda item: item[0])

        return (
            self.name == other.name
            and self.type_ == other.type_
            and self.nullable == other.nullable
            and self.primary_key == other.primary_key
            and self.foreign_key == other.foreign_key
            and self.unique == other.unique
            and self_type_args == other_type_args
        )
