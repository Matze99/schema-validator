import json

import sqlglot
from simple_ddl_parser import DDLParser
from sqlglot.expressions import (
    Table,
    ColumnDef,
    PrimaryKey,
    NotNullColumnConstraint,
    PrimaryKeyColumnConstraint,
    Expression,
    Create,
    Identifier,
    DataType,
)

from schema_validator.model.column_schema import ColumnSchema, data_type_has_attributes
from schema_validator.model.table_schema import TableSchema


class ParseDdl:
    @staticmethod
    def load_ddl(path: str):
        with open(path, "r") as f:
            sql = f.read()
        return sql

    @staticmethod
    def parse_data_type_args(type_: DataType | None) -> dict[str, int | tuple[int, int]]:
        args = {}
        if type_ is None:
            return args

        string_expression = f"create table example(example {type_.__str__()})"
        parsed_expression = DDLParser(string_expression).run()[0]["columns"][0]
        args["size"] = parsed_expression["size"]
        return args

    @staticmethod
    def parse_column_expr(column_expr: ColumnDef | PrimaryKey, table: TableSchema):
        if isinstance(column_expr, ColumnDef):
            nullable = True
            primary_key = False

            # go over constraints
            for constraint in column_expr.constraints:
                if isinstance(constraint.kind, PrimaryKeyColumnConstraint):
                    nullable = False
                    primary_key = True
                elif isinstance(constraint.kind, NotNullColumnConstraint):
                    nullable = False
                    primary_key = False

            if column_expr.kind is None:
                raise Exception(f"kind is None for {column_expr}")

            column_schema = ColumnSchema(
                column_expr.name, column_expr.kind.this, nullable, primary_key, False
            )

            if (
                data_type_has_attributes(column_expr.kind.this)
                and column_expr.kind.expressions
            ):
                column_schema.type_args = ParseDdl.parse_data_type_args(
                    column_expr.kind
                )

            table.add_column(column_schema)
        if isinstance(column_expr, PrimaryKey):
            # TODO see if necessary and test
            identifier = column_expr.expressions[0].name
            table.get_column(identifier).primary_key = True
            table.get_column(identifier).nullable = False

    @staticmethod
    def parse_table_ref(table_expr: Table):
        if table_expr.db is None or len(table_expr.db) == 0:
            return table_expr.name

        return f"{table_expr.db}.{table_expr.name}"

    @staticmethod
    def parse_create_table_expr(expr: Create) -> TableSchema:
        table_expr: Table = expr.this.this
        column_exprs: list[ColumnDef | PrimaryKey] = expr.this.expressions
        table_ref = ParseDdl.parse_table_ref(table_expr)
        table = TableSchema(table_ref)

        for column_expr in column_exprs:
            ParseDdl.parse_column_expr(column_expr, table)

        return table

    @staticmethod
    def parse_expr(expr: Expression | None, tables: dict[str, TableSchema]):
        if isinstance(expr, Create):
            new_table = ParseDdl.parse_create_table_expr(expr)
            tables[new_table.table_ref] = new_table

        # TODO this does not handle alter

    @staticmethod
    def parse_sql(sql: str) -> dict[str, TableSchema]:
        sql_expressions = sqlglot.parse(sql)

        tables = {}
        for expr in sql_expressions:
            ParseDdl.parse_expr(expr, tables)

        return tables

    @staticmethod
    def is_create_table_expr(expr: Expression | None) -> bool:
        return isinstance(expr, Create)
