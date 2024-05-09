import json

from simple_ddl_parser import DDLParser

from schema_validator.model.column_schema import (
    ColumnSchema,
    ColumnModel,
    ColumnReferenceModel,
    AlterColumnModel,
)
from schema_validator.model.empty_model import EmptyModel
from schema_validator.model.table_schema import TableSchema, TableModel


class ParseDdl:
    @staticmethod
    def load_ddl(path: str):
        with open(path, "r") as f:
            sql = f.read()
        return sql

    @staticmethod
    def parse_column_expr(
        column: ColumnModel, primary_keys: list[str], table: TableSchema
    ):
        column_schema = ColumnSchema.from_model(column)

        if column_schema.name in primary_keys:
            column_schema.primary_key = True
            column_schema.nullable = False

        table.add_column(column_schema)

    @staticmethod
    def parse_table_ref(table_model: TableModel):
        if table_model.schema is None or len(table_model.schema) == 0:
            return table_model.table_name

        return f"{table_model.schema}.{table_model.table_name}"

    @staticmethod
    def parse_create_table_expr(table_model: TableModel) -> TableSchema:
        table_ref = ParseDdl.parse_table_ref(table_model)
        table = TableSchema(table_ref)

        if isinstance(table_model, EmptyModel):
            return table

        for column_expr in table_model.columns:
            # for pyright to be happy
            if isinstance(column_expr, tuple):
                continue
            ParseDdl.parse_column_expr(column_expr, table_model.primary_key, table)

        return table

    @staticmethod
    def parse_expr(table_model: TableModel, tables: dict[str, TableSchema]):
        new_table = ParseDdl.parse_create_table_expr(table_model)
        tables[new_table.table_ref] = new_table

    @staticmethod
    def parse_sql(sql: str, dialect: str = "sql") -> dict[str, TableSchema]:

        parsed_ddl = DDLParser(sql).run(output_mode=dialect)

        tables = {}
        for expr in parsed_ddl:
            table_model = TableModel(**expr)
            table_model.apply_alter()
            ParseDdl.parse_expr(table_model, tables)

        return tables
