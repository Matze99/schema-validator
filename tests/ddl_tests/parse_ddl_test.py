import pytest

from schema_validator.ddl.parse_ddl import ParseDdl
from schema_validator.model.alter_table_model import AlterTableModel, PrimaryKeyModel
from schema_validator.model.column_schema import (
    ColumnSchema,
    ColumnModel,
    DataType,
    ColumnReferenceModel,
    ForeignKeyReference,
    AlterColumnModel,
)
from schema_validator.model.table_schema import TableSchema, TableModel


@pytest.mark.parametrize(
    "table_expr,desired_ref",
    [
        (
            TableModel(
                table_name="test",
                schema=None,
                primary_key=[],
                columns=[],
                alter={},
                checks=[],
                index=[],
                partitioned_by=[],
                tablespace=None,
            ),
            "test",
        ),
        (
            TableModel(
                table_name="test",
                schema="",
                primary_key=[],
                columns=[],
                alter={},
                checks=[],
                index=[],
                partitioned_by=[],
                tablespace=None,
            ),
            "test",
        ),
        (
            TableModel(
                table_name="test",
                schema="schema",
                primary_key=[],
                columns=[],
                alter={},
                checks=[],
                index=[],
                partitioned_by=[],
                tablespace=None,
            ),
            "schema.test",
        ),
    ],
)
def test_parse_table_ref(table_expr: TableModel, desired_ref: str):
    actual_ref = ParseDdl.parse_table_ref(table_expr)
    assert actual_ref == desired_ref


@pytest.mark.parametrize(
    "column_expr,primary_keys,desired_column",
    [
        (
            ColumnModel(
                check=None,
                default=None,
                name="Id",
                nullable=True,
                references=None,
                size=None,
                type="INT",
                unique=False,
            ),
            ["Test"],
            ColumnSchema("Id", DataType.INT, True, True, False),
        ),
        (
            ColumnModel(
                check=None,
                default=None,
                name="Id",
                nullable=False,
                references=None,
                size=None,
                type="INT",
                unique=False,
            ),
            ["Id"],
            ColumnSchema("Id", DataType.FLOAT, False, True, False),
        ),
        (
            ColumnModel(
                check=None,
                default=None,
                name="Id",
                nullable=False,
                references=None,
                size=None,
                type="int",
                unique=False,
            ),
            [],
            ColumnSchema("Id", DataType.INT, False, True, False),
        ),
        (
            ColumnModel(
                check=None,
                default=None,
                name="Id",
                nullable=False,
                references=ColumnReferenceModel(
                    table="foreign_table",
                    schema="foreign_schema",
                    on_delete=None,
                    on_update=None,
                    deferrable_initially=None,
                    column="foreign_column",
                ),
                size=None,
                type="int",
                unique=False,
            ),
            ["Id"],
            ColumnSchema(
                "Id",
                DataType.INT,
                False,
                True,
                False,
                foreign_key_reference=ForeignKeyReference(
                    "foreign_table", "foreign_schem_a", "foreign_column"
                ),
            ),
        ),
    ],
)
def test_parse_column_expr_unequal(
    column_expr: ColumnModel, primary_keys: list[str], desired_column: ColumnSchema
):
    table = TableSchema("test.table")
    ParseDdl.parse_column_expr(column_expr, primary_keys, table)

    assert table.get_column(desired_column.name) != desired_column


@pytest.mark.parametrize(
    "column_expr,primary_keys,desired_column",
    [
        (
            ColumnModel(
                check=None,
                default=None,
                name="Id",
                nullable=True,
                references=None,
                size=None,
                type="INT",
                unique=False,
            ),
            ["Test"],
            ColumnSchema("Id", DataType.INT, True, False, False),
        ),
        (
            ColumnModel(
                check=None,
                default=None,
                name="Id",
                nullable=False,
                references=None,
                size=None,
                type="INT",
                unique=False,
            ),
            ["Id"],
            ColumnSchema("Id", DataType.INT, False, True, False),
        ),
        (
            ColumnModel(
                check=None,
                default=None,
                name="Id",
                nullable=False,
                references=None,
                size=None,
                type="int",
                unique=False,
            ),
            ["Id"],
            ColumnSchema("Id", DataType.INT, False, True, False),
        ),
        (
            ColumnModel(
                check=None,
                default=None,
                name="Id",
                nullable=False,
                references=ColumnReferenceModel(
                    table="foreign_table",
                    schema="foreign_schema",
                    on_delete=None,
                    on_update=None,
                    deferrable_initially=None,
                    column="foreign_column",
                ),
                size=None,
                type="int",
                unique=False,
            ),
            ["Id"],
            ColumnSchema(
                "Id",
                DataType.INT,
                False,
                True,
                False,
                foreign_key_reference=ForeignKeyReference(
                    "foreign_table", "foreign_schema", "foreign_column"
                ),
            ),
        ),
    ],
)
def test_parse_column_expr(
    column_expr: ColumnModel, primary_keys: list[str], desired_column: ColumnSchema
):
    table = TableSchema("test.table")
    ParseDdl.parse_column_expr(column_expr, primary_keys, table)

    assert table.get_column(desired_column.name) == desired_column
    assert len(table.columns) == 1
    assert len(table._name_to_column.values()) == 1


# @pytest.mark.parametrize(
#     "column_expr, primary_keys",
#     [
#         Table(**{"this": Identifier(**{"quoted": False, "this": "test"})}),
#         Table(
#             **{
#                 "this": Identifier(**{"quoted": False, "this": "test"}),
#                 "db": Identifier(**{"quoted": False, "this": "schema"}),
#             }
#         ),
#     ],
# )
# def test_parse_column_expr_non_rel(column_expr: ColumnModel, primary_keys: list[str]):
#     table = TableSchema("test.table")
#     ParseDdl.parse_column_expr(column_expr, primary_keys, table)
#
#     assert table.columns == []


@pytest.mark.parametrize(
    "expr,desired_table_ref,desired_table_columns",
    [
        (
            TableModel(
                table_name="test",
                schema="schema",
                primary_key=["Id"],
                columns=[
                    ColumnModel(
                        check=None,
                        default=None,
                        name="Id",
                        nullable=False,
                        references=None,
                        size=None,
                        type="INT",
                        unique=False,
                    ),
                    ColumnModel(
                        check=None,
                        default=None,
                        name="Name",
                        nullable=False,
                        references=None,
                        size=50,
                        type="VARCHAR",
                        unique=False,
                    ),
                    ColumnModel(
                        check=None,
                        default=None,
                        name="Price",
                        nullable=True,
                        references=None,
                        size=None,
                        type="INT",
                        unique=False,
                    ),
                    ColumnModel(
                        check=None,
                        default=None,
                        name="Percent",
                        nullable=True,
                        references=None,
                        size=(30, 5),
                        type="FLOAT",
                        unique=False,
                    ),
                ],
                alter={},
                checks=[],
                index=[],
                partitioned_by=[],
                tablespace=None,
            ),
            "schema.test",
            [
                ColumnSchema("Id", DataType.INT, False, True, False),
                ColumnSchema(
                    "Name",
                    DataType.VARCHAR,
                    False,
                    False,
                    False,
                    type_args={"size": 50},
                ),
                ColumnSchema("Price", DataType.INT, True, False, False),
                ColumnSchema(
                    "Percent",
                    DataType.FLOAT,
                    True,
                    False,
                    False,
                    type_args={"size": (30, 5)},
                ),
            ],
        ),
        (
            TableModel(
                table_name="test",
                schema="schema",
                primary_key=[],
                columns=[
                    ColumnModel(
                        check=None,
                        default=None,
                        name="Id",
                        nullable=False,
                        references=None,
                        size=None,
                        type="INT",
                        unique=False,
                    ),
                ],
                alter=AlterTableModel(
                    primary_keys=[
                        PrimaryKeyModel(columns=["Id"], constraint_name="test")
                    ]
                ),
                checks=[],
                index=[],
                partitioned_by=[],
                tablespace=None,
            ),
            "schema.test",
            [
                ColumnSchema("Id", DataType.INT, False, True, False),
            ],
        ),
        (
            TableModel(
                table_name="test",
                schema="schema",
                primary_key=[],
                columns=[
                    ColumnModel(
                        check=None,
                        default=None,
                        name="Id",
                        nullable=False,
                        references=None,
                        size=None,
                        type="INT",
                        unique=False,
                    ),
                    ColumnModel(
                        check=None,
                        default=None,
                        name="Percent",
                        nullable=True,
                        references=None,
                        size=(30, 5),
                        type="FLOAT",
                        unique=False,
                    ),
                ],
                alter=AlterTableModel(
                    primary_keys=[
                        PrimaryKeyModel(columns=["Id"], constraint_name="test")
                    ],
                    columns=[
                        AlterColumnModel(
                            name="Percent",
                            references=ColumnReferenceModel(
                                table="foreign_table",
                                schema="foreign_schema",
                                on_delete=None,
                                on_update=None,
                                deferrable_initially=None,
                                column="foreign",
                            ),
                        )
                    ],
                ),
                checks=[],
                index=[],
                partitioned_by=[],
                tablespace=None,
            ),
            "schema.test",
            [
                ColumnSchema("Id", DataType.INT, False, True, False),
                ColumnSchema(
                    "Percent",
                    DataType.FLOAT,
                    True,
                    False,
                    False,
                    type_args={"size": (30, 5)},
                    foreign_key_reference=ForeignKeyReference(
                        "foreign_table", "foreign_schema", "foreign"
                    ),
                ),
            ],
        ),
    ],
)
def test_parse_create_table_expr(
    expr: TableModel, desired_table_ref: str, desired_table_columns: list[ColumnSchema]
):
    desired_table = TableSchema(desired_table_ref)
    for column in desired_table_columns:
        desired_table.add_column(column)

    expr.apply_alter()
    table = ParseDdl.parse_create_table_expr(expr)

    assert table == desired_table
