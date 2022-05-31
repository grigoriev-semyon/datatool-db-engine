from typing import Tuple

from fastapi import APIRouter
from fastapi_sqlalchemy import db

from dbengine.branch import get_branch
from dbengine.column import create_column, delete_column, get_column, update_column
from dbengine.exceptions import ColumnDoesntExists
from dbengine.models import Commit, DbColumn, DbColumnAttributes
from dbengine.table import get_table


column_router = APIRouter(prefix="/table/{table_id}/column", tags=["Column"])


@column_router.post("/")
async def http_create_column(
    branch_id: int, table_id: int, name: str, datatype: str
) -> Tuple[DbColumn, DbColumnAttributes, Commit]:
    branch = get_branch(branch_id, session=db.session)
    table = get_table(branch, table_id, session=db.session)
    return create_column(branch, table[0], name=name, datatype=datatype, session=db.session)


@column_router.get("/{column_id}")
async def http_get_column(branch_id: int, column_id: int) -> Tuple[DbColumn, DbColumnAttributes]:
    branch = get_branch(branch_id, session=db.session)
    return get_column(branch, column_id, session=db.session)


@column_router.patch("/{column_id}")
async def http_update_column(
    branch_id: int, column_id: int, name: str, datatype: str
) -> Tuple[DbColumn, DbColumnAttributes, Commit]:
    branch = get_branch(branch_id, session=db.session)
    column = get_column(branch, column_id, session=db.session)
    return update_column(branch, column[0], name=name, datatype=datatype, session=db.session)


@column_router.delete("/{column_id}")
async def http_delete_column(branch_id: int, column_id: int) -> Commit:
    branch = get_branch(branch_id, session=db.session)
    column = get_column(branch, column_id, session=db.session)
    return delete_column(branch, column[0], session=db.session)


@column_router.post("")
async def http_get_columns_in_branch(branch_id: int, table_id: int):
    column_ids = db.session.query(DbColumn).filter(DbColumn.table_id == table_id).all()
    branch = get_branch(branch_id, session=db.session)
    result = []
    for row in column_ids:
        try:
            result.append(get_column(branch, row.id, session=db.session))
        except ColumnDoesntExists:
            pass
    return result
