from dataclasses import dataclass, field

from pydantic import BaseModel

from schema_validator.model.column_schema import ColumnSchema, ColumnModel


class TableModel(BaseModel):
    table_name: str
    schema: str | None # type: ignore
    primary_key: list[str]
    columns: list[ColumnModel]
    alter: dict
    checks: list
    index: list
    partitioned_by: list
    tablespace: str | None


@dataclass
class TableSchema:
    table_ref: str
    _columns: list[ColumnSchema] = field(default_factory=list[ColumnSchema])
    _name_to_column: dict[str, ColumnSchema] = field(
        default_factory=dict[str, ColumnSchema]
    )

    def get_column(self, name: str):
        return self._name_to_column[name]

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, columns: list[ColumnSchema]):
        self._columns = columns
        self._name_to_column = {column.name: column for column in columns}

    def add_column(self, column: ColumnSchema):
        self._columns.append(column)
        self._name_to_column[column.name] = column

    def __eq__(self, other):
        if not isinstance(other, TableSchema):
            return False

        sorted_columns = sorted(self.columns, key=lambda column: column.name)
        sorted_other_columns = sorted(other.columns, key=lambda column: column.name)

        return (
            sorted_columns == sorted_other_columns and self.table_ref == other.table_ref
        )
