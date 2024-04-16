from dataclasses import dataclass, field
from enum import Enum

from pydantic import BaseModel


class DataType(Enum):
    INT: str = "INT" # type: ignore
    VARCHAR: str = "VARCHAR" # type: ignore
    TEXT: str = "TEXT" # type: ignore
    DATE: str = "DATE" # type: ignore
    TIME: str = "TIME" # type: ignore
    FLOAT: str = "FLOAT" # type: ignore
    DOUBLE: str = "DOUBLE" # type: ignore
    DECIMAL: str = "DECIMAL" # type: ignore
    BIT: str = "BIT" # type: ignore
    TINYINT: str = "TINYINT" # type: ignore
    SMALLINT: str = "SMALLINT" # type: ignore
    CHAR: str = "CHAR" # type: ignore
    BINARY: str = "BINARY" # type: ignore
    VARBINARY: str = "VARBINARY" # type: ignore
    DATETIME: str = "DATETIME" # type: ignore
    TIMESTAMP: str = "TIMESTAMP" # type: ignore
    MEDIUMINT: str = "MEDIUMINT" # type: ignore
    NVARCHAR: str = "NVARCHAR" # type: ignore
    ARRAY: str = "ARRAY" # type: ignore
    MAP: str = "MAP" # type: ignore
    STRUCT: str = "STRUCT" # type: ignore
    BIGINT: str = "BIGINT" # type: ignore
    BOOLEAN: str = "BOOLEAN" # type: ignore


class ColumnModel(BaseModel):
    check: str | None
    default: str | None
    name: str
    nullable: bool
    references: str | None
    size: int | tuple[int, int] | None
    unique: bool
    type: str


@dataclass
class ColumnSchema:
    name: str
    type_: DataType
    nullable: bool
    primary_key: bool
    foreign_key: bool
    unique: bool = False
    type_args: dict[str, int | tuple[int, int]] = field(
        default_factory=dict[str, int | tuple[int, int]]
    )

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

    @staticmethod
    def from_model(model: ColumnModel) -> "ColumnSchema":
        type_args = {} if model.size is None else {"size": model.size}
        return ColumnSchema(
            name=model.name,
            type_=DataType(model.type),
            nullable=model.nullable,
            primary_key=False,
            foreign_key=False,
            unique=model.unique,
            type_args=type_args,
        )
