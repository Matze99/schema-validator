from dataclasses import dataclass, field

from pydantic import BaseModel

from schema_validator.model.alter_table_model import (
    AlterTableModel,
    AlterTableModelColumns,
    AlterTableModelPrimaryKey,
    PrimaryKeyModel,
)
from schema_validator.model.column_schema import (
    ColumnSchema,
    ColumnModel,
    AlterColumnModel,
)
from schema_validator.model.empty_model import EmptyModel


class TableModel(BaseModel):
    table_name: str
    schema: str | None  # type: ignore
    primary_key: list[str]
    columns: list[ColumnModel] | EmptyModel
    alter: AlterTableModel | EmptyModel | None
    checks: list
    index: list
    partitioned_by: list
    tablespace: str | None

    def apply_alter(self):
        if self.alter is None:
            return
        if isinstance(self.alter, EmptyModel):
            return

        if self.alter.columns is not None:
            self._apply_column_alter(self.alter.columns)
        if self.alter.primary_keys is not None:
            self._apply_primary_key_alter(self.alter.primary_keys)

    def _apply_column_alter(self, alter_columns: list[AlterColumnModel]):
        for alter_column in alter_columns:
            column = self._get_column_from_name(alter_column.name)

            column.references = alter_column.references

    def _get_column_from_name(self, column_name: str) -> ColumnModel:
        if isinstance(self.columns, EmptyModel):
            raise ValueError(f"Table {self.table_name} has no columns")

        for column in self.columns:
            if column.name == column_name:
                return column

        raise ValueError(f"Column {column_name} not found in table {self.table_name}")

    def _apply_primary_key_alter(self, primary_keys: list[PrimaryKeyModel]):
        for prim_key in primary_keys:
            for column in prim_key.columns:
                self._add_to_primary_key(column)

    def _add_to_primary_key(self, column: str):
        if not self._column_name_exists(column):
            raise ValueError(f"Column {column} not found in table {self.table_name}")
        if column in self.primary_key:
            return
        self.primary_key.append(column)

    def _column_name_exists(self, column_name: str):
        if isinstance(self.columns, EmptyModel):
            return False

        col_names = [column.name for column in self.columns]
        return column_name in col_names


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
