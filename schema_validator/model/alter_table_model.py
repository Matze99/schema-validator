from pydantic import BaseModel

from schema_validator.model.column_schema import AlterColumnModel


class AlterTableModelColumns(BaseModel):
    columns: list[AlterColumnModel]


class PrimaryKeyModel(BaseModel):
    columns: list[str]
    constraint_name: str


class AlterTableModelPrimaryKey(BaseModel):
    primary_keys: list[PrimaryKeyModel]


class AlterTableModel(BaseModel):
    primary_keys: list[PrimaryKeyModel] | None = None
    columns: list[AlterColumnModel] | None = None
