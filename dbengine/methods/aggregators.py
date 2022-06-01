from typing import Tuple

from dbengine.models.entity import DbTable, DbTableAttributes, DbColumnAttributes, DbColumn


def table_aggregator(table_and_attr: Tuple[DbTable, DbTableAttributes]):
    return {
        "id": table_and_attr[0].id,
        "name": table_and_attr[1].name
    }


def column_aggregator(column_and_attr: Tuple[DbColumn, DbColumnAttributes]):
    return {
        "id": column_and_attr[0].id,
        "table_id": column_and_attr[0].table_id,
        "name": column_and_attr[1].name,
        "datatype": column_and_attr[1].datatype
    }
