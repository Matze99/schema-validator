import pytest
from sqlglot.expressions import (
    Table,
    Identifier,
    Expression,
    ColumnDef,
    DataType,
    ColumnConstraint,
    PrimaryKeyColumnConstraint,
    GeneratedAsIdentityColumnConstraint,
    Literal,
    Create,
    Schema,
    NotNullColumnConstraint,
    DataTypeParam,
)

from schema_validator.ddl.parse_ddl import ParseDdl
from schema_validator.model.column_schema import ColumnSchema
from schema_validator.model.table_schema import TableSchema


@pytest.mark.parametrize(
    "table_expr,desired_ref",
    [
        (Table(**{"this": Identifier(**{"quoted": False, "this": "test"})}), "test"),
        (
            Table(
                **{
                    "this": Identifier(**{"quoted": False, "this": "test"}),
                    "db": Identifier(**{"quoted": False, "this": "schema"}),
                }
            ),
            "schema.test",
        ),
    ],
)
def test_parse_table_ref(table_expr: Table, desired_ref: str):
    actual_ref = ParseDdl.parse_table_ref(table_expr)
    assert actual_ref == desired_ref


@pytest.mark.parametrize(
    "column_expr,desired_column",
    [
        (
            ColumnDef(
                **{
                    "this": Identifier(**{"this": "Id", "quoted": False}),
                    "kind": DataType(**{"this": DataType.Type.INT, "nested": False}),
                    "constraints": [],
                }
            ),
            ColumnSchema("Id", DataType.Type.INT, True, False, False),
        ),
        (
            ColumnDef(
                **{
                    "this": Identifier(**{"this": "Id", "quoted": False}),
                    "kind": DataType(**{"this": DataType.Type.INT, "nested": False}),
                    "constraints": [
                        ColumnConstraint(
                            **{
                                "kind": PrimaryKeyColumnConstraint(),
                            }
                        ),
                        ColumnConstraint(
                            **{
                                "kind": GeneratedAsIdentityColumnConstraint(
                                    **{
                                        "start": Literal(
                                            **{"this": 1, "is_string": False}
                                        ),
                                        "increment": Literal(
                                            **{"this": 1, "is_string": False}
                                        ),
                                    }
                                ),
                            }
                        ),
                    ],
                }
            ),
            ColumnSchema("Id", DataType.Type.INT, False, True, False),
        ),
    ],
)
def test_parse_column_expr(column_expr: Expression, desired_column: ColumnSchema):
    table = TableSchema("test.table")
    ParseDdl.parse_column_expr(column_expr, table)

    assert table.get_column(desired_column.name) == desired_column
    assert len(table.columns) == 1
    assert len(table._name_to_column.values()) == 1


@pytest.mark.parametrize(
    "column_expr",
    [
        Table(**{"this": Identifier(**{"quoted": False, "this": "test"})}),
        Table(
            **{
                "this": Identifier(**{"quoted": False, "this": "test"}),
                "db": Identifier(**{"quoted": False, "this": "schema"}),
            }
        ),
    ],
)
def test_parse_column_expr_non_rel(column_expr: Expression):
    table = TableSchema("test.table")
    ParseDdl.parse_column_expr(column_expr, table)

    assert table.columns == []


@pytest.mark.parametrize(
    "expr,desired_table_ref,desired_table_columns",
    [
        (
            Create(
                **{
                    "kind": "TABLE",
                    "this": Schema(
                        **{
                            "this": Table(
                                **{
                                    "this": Identifier(
                                        **{"quoted": False, "this": "test"}
                                    ),
                                    "db": Identifier(
                                        **{"quoted": False, "this": "schema"}
                                    ),
                                }
                            ),
                            "expressions": [
                                ColumnDef(
                                    **{
                                        "this": Identifier(
                                            **{"this": "Id", "quoted": False}
                                        ),
                                        "kind": DataType(
                                            **{
                                                "this": DataType.Type.INT,
                                                "nested": False,
                                            }
                                        ),
                                        "constraints": [
                                            ColumnConstraint(
                                                **{
                                                    "kind": PrimaryKeyColumnConstraint(),
                                                }
                                            ),
                                            ColumnConstraint(
                                                **{
                                                    "kind": GeneratedAsIdentityColumnConstraint(
                                                        **{
                                                            "start": Literal(
                                                                **{
                                                                    "this": 1,
                                                                    "is_string": False,
                                                                }
                                                            ),
                                                            "increment": Literal(
                                                                **{
                                                                    "this": 1,
                                                                    "is_string": False,
                                                                }
                                                            ),
                                                        }
                                                    ),
                                                }
                                            ),
                                        ],
                                    }
                                ),
                                ColumnDef(
                                    **{
                                        "this": Identifier(
                                            **{"this": "Name", "quoted": False}
                                        ),
                                        "kind": DataType(
                                            **{
                                                "this": DataType.Type.VARCHAR,
                                                "nested": False,
                                                "expressions": [
                                                    DataTypeParam(
                                                        **{
                                                            "this": Literal(
                                                                **{
                                                                    "this": 50,
                                                                    "is_string": False,
                                                                }
                                                            ),
                                                            "nested": False,
                                                        }
                                                    )
                                                ],
                                            }
                                        ),
                                        "constraints": [
                                            ColumnConstraint(
                                                **{
                                                    "kind": NotNullColumnConstraint(),
                                                }
                                            ),
                                        ],
                                    }
                                ),
                                ColumnDef(
                                    **{
                                        "this": Identifier(
                                            **{"this": "Price", "quoted": False}
                                        ),
                                        "kind": DataType(
                                            **{
                                                "this": DataType.Type.INT,
                                                "nested": False,
                                            }
                                        ),
                                        "constraints": [],
                                    }
                                ),
                                ColumnDef(
                                    **{
                                        "this": Identifier(
                                            **{"this": "Percent", "quoted": False}
                                        ),
                                        "kind": DataType(
                                            **{
                                                "this": DataType.Type.FLOAT,
                                                "nested": False,
                                                "expressions": [
                                                    DataTypeParam(
                                                        **{
                                                            "this": Literal(
                                                                **{
                                                                    "this": 30,
                                                                    "is_string": False,
                                                                }
                                                            ),
                                                            "nested": False,
                                                        }
                                                    ),
                                                    DataTypeParam(
                                                        **{
                                                            "this": Literal(
                                                                **{
                                                                    "this": 5,
                                                                    "is_string": False,
                                                                }
                                                            ),
                                                            "nested": False,
                                                        }
                                                    ),
                                                ],
                                            }
                                        ),
                                        "constraints": [],
                                    }
                                ),
                            ],
                        }
                    ),
                }
            ),
            "schema.test",
            [
                ColumnSchema("Id", DataType.Type.INT, False, True, False),
                ColumnSchema(
                    "Name",
                    DataType.Type.VARCHAR,
                    False,
                    False,
                    False,
                    type_args={"size": 50},
                ),
                ColumnSchema("Price", DataType.Type.INT, True, False, False),
                ColumnSchema(
                    "Percent",
                    DataType.Type.FLOAT,
                    True,
                    False,
                    False,
                    type_args={"size": (30, 5)},
                ),
            ],
        ),
    ],
)
def test_parse_create_table_expr(
    expr: Expression, desired_table_ref: str, desired_table_columns: list[ColumnSchema]
):
    desired_table = TableSchema(desired_table_ref)
    for column in desired_table_columns:
        desired_table.add_column(column)

    table = ParseDdl.parse_create_table_expr(expr)

    assert table == desired_table
