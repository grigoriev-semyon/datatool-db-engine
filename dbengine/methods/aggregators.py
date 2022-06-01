from typing import Tuple

from dbengine.models.entity import DbTable, DbTableAttributes, DbColumnAttributes, DbColumn


def table_aggregator(table: DbTable, attr: DbTableAttributes):
    return {
        "id": table.id,
        "name": attr.name
    }


def column_aggregator(column: DbColumn, attr: DbColumnAttributes):
    return {
        "id": column.id,
        "table_id": column.table_id,
        "name": attr.name,
        "datatype": attr.datatype
    }
