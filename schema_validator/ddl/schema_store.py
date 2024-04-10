import os

from schema_validator.ddl.parse_ddl import ParseDdl
from schema_validator.model.table_schema import TableSchema


class SchemaStore:
    def __init__(self, path: str):
        """
        the schema store loads all sql files located under path and parses them,
        it then extracts all the schema definitions and formats them into a TableSchema object.

        :param path:
        """
        self.path = path
        self.tables = {}

        if path[-3:] == "sql":
            all_files = [path]
        else:
            # load files from path and parse
            all_files = [
                os.path.join(path, name)
                for path, subdirs, files in os.walk(path)
                for name in files
            ]
        for file in all_files:
            string_sql = ParseDdl.load_ddl(file)
            new_tables = ParseDdl.parse_sql(string_sql)

            for new_table in new_tables.keys():
                if new_table in self.tables.keys():
                    raise Exception(f"Table {new_table} already exists")
            # TODO handle table alter in different file

            self.tables.update(new_tables)

    def get_table_schema(self, name: str) -> TableSchema:
        if not name in self.tables.keys():
            raise Exception(f"table {name} not found")

        return self.tables[name]


if __name__ == "__main__":
    schema_store = SchemaStore("../../data/test-ddls/simple-ddls")
    print(schema_store.get_table_schema("Books"))
