from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, DataType as SparkDataType, StructField, StringType, VarcharType, Row
from sqlglot.expressions import DataType

from schema_validator.mapping.pyspark_type_mapping import PYSPARK_TYPE_MAPPING
from schema_validator.model.column_schema import ColumnSchema
from schema_validator.model.table_schema import TableSchema
from schema_validator.validator.schema_validator import SchemaValidator


class PysparkValidator(SchemaValidator):
    def __init__(self, schema: StructType):
        self.schema = schema
        self.column_names = sorted([field.name for field in schema.fields])

    def _has_none(self, results: list[Row]):
        for result in results:
            for value in result:
                if value is None:
                    return True
        return False

    def validate(self, dataframe: DataFrame) -> tuple[bool, str]:
        if len(dataframe.schema.fields) != len(self.schema.fields):
            return False, "Different number of fields in to validate and true schema"

        df_col_names = sorted([field.name for field in dataframe.schema.fields])

        if df_col_names != self.column_names:
            return False, f"Different column names desired: {self.column_names} actual: {df_col_names}"

        try:
            column_casts = [col(field.name).cast(field.dataType) for field in self.schema.fields]
            cast_df = dataframe.select(*column_casts)
            results = cast_df.collect()
        except Exception as e:
            return False, str(e)

        if self._has_none(results):
            return False, "Types are incompatible"
        return True, "Valid"


class PysparkValidatorFactory:

    @staticmethod
    def _convert_type_to_pyspark(type_: DataType.Type, size: int | tuple[int, int] | None = None) -> SparkDataType:
        pyspark_type: SparkDataType = PYSPARK_TYPE_MAPPING[type_]

        if size is None:
            return pyspark_type()
        elif isinstance(size, tuple):
            return pyspark_type(*size)
        else:
            return pyspark_type(size)

    @staticmethod
    def _convert_column_to_pyspark(column: ColumnSchema) -> StructField:
        return StructField(
            name=column.name,
            dataType=PysparkValidatorFactory._convert_type_to_pyspark(column.type_, column.type_args.get("size")),
            nullable=column.nullable
        )

    @staticmethod
    def get_validator(table_schema: TableSchema) -> PysparkValidator:
        schema = StructType(
            [PysparkValidatorFactory._convert_column_to_pyspark(column) for column in table_schema.columns]
        )

        return PysparkValidator(schema)
