from typing import Tuple, List

from fastapi import APIRouter
from fastapi_sqlalchemy import db
from fastapi.exceptions import HTTPException

import dbengine.models
from dbengine.exceptions import ColumnDoesntExists
from dbengine.methods import create_column, delete_column, get_branch, get_column, get_table, update_column
from dbengine.methods.column import get_columns
from dbengine.routes.models import Table, Column
from dbengine.methods.aggregators import column_aggregator

column_router = APIRouter(prefix="/table/{table_id}/column", tags=["Column"])


@column_router.post("", response_model=Column)
async def http_create_column(
        branch_id: int, table_id: int, name: str, datatype: str):
    branch = get_branch(branch_id, session=db.session)
    table = get_table(branch, table_id, session=db.session)
    result = create_column(branch, table[0], name=name, datatype=datatype, session=db.session)
    return column_aggregator((result[0], result[1]))


@column_router.get("/{column_id}", response_model=Column)
async def http_get_column(branch_id: int, column_id: int):
    branch = get_branch(branch_id, session=db.session)
    result = get_column(branch, column_id, session=db.session)
    return column_aggregator(result)


@column_router.patch("/{column_id}", response_model=Column)
async def http_update_column(
        branch_id: int, column_id: int, name: str, datatype: str):
    branch = get_branch(branch_id, session=db.session)
    column = get_column(branch, column_id, session=db.session)
    result = update_column(branch, column[0], name=name, datatype=datatype, session=db.session)
    return column_aggregator((result[0], result[1]))


@column_router.delete("/{column_id}", response_model=str)
async def http_delete_column(branch_id: int, column_id: int):
    branch = get_branch(branch_id, session=db.session)
    column = get_column(branch, column_id, session=db.session)
    delete_column(branch, column[0], session=db.session)
    return "Table deleted"


@column_router.get("", response_model=List[Column])
async def http_get_columns_in_branch(branch_id: int, table_id: int):
    branch = get_branch(branch_id, session=db.session)
    table = get_table(branch, table_id, session=db.session)[0]
    column_ids = get_columns(branch, table, session=db.session)
    result = []
    for row in column_ids:
        column = get_column(branch, row, session=db.session)
        result.append(column_aggregator((column[0], column[1])))
    return result
