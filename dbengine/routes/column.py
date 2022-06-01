from typing import Tuple, List

from fastapi import APIRouter
from fastapi_sqlalchemy import db
from fastapi.exceptions import HTTPException

import dbengine.models
from dbengine.exceptions import ColumnDoesntExists
from dbengine.methods import create_column, delete_column, get_branch, get_column, get_table, update_column
from dbengine.routes.models import DbColumn, DbColumnAttributes, Commit

column_router = APIRouter(prefix="/table/{table_id}/column", tags=["Column"])


@column_router.post("", response_model=Tuple[DbColumn, DbColumnAttributes, Commit])
async def http_create_column(
        branch_id: int, table_id: int, name: str, datatype: str):
    branch = get_branch(branch_id, session=db.session)
    table = get_table(branch, table_id, session=db.session)
    return create_column(branch, table[0], name=name, datatype=datatype, session=db.session)


@column_router.get("/{column_id}", response_model=Tuple[DbColumn, DbColumnAttributes])
async def http_get_column(branch_id: int, column_id: int):
    branch = get_branch(branch_id, session=db.session)
    return get_column(branch, column_id, session=db.session)


@column_router.patch("/{column_id}", response_model=Tuple[DbColumn, DbColumnAttributes, Commit])
async def http_update_column(
        branch_id: int, column_id: int, name: str, datatype: str):
    branch = get_branch(branch_id, session=db.session)
    column = get_column(branch, column_id, session=db.session)
    return update_column(branch, column[0], name=name, datatype=datatype, session=db.session)


@column_router.delete("/{column_id}", response_model=Commit)
async def http_delete_column(branch_id: int, column_id: int):
    branch = get_branch(branch_id, session=db.session)
    column = get_column(branch, column_id, session=db.session)
    return delete_column(branch, column[0], session=db.session)


@column_router.get("", response_model=List[Tuple[DbColumn, DbColumnAttributes]])
async def http_get_columns_in_branch(branch_id: int, table_id: int):
    column_ids = db.session.query(dbengine.models.DbColumn.table_id).filter(dbengine.models.DbColumn.table_id == table_id).all()
    branch = get_branch(branch_id, session=db.session)
    result = []
    for row in column_ids:
        try:
            result.append(get_column(branch, row, session=db.session))
        except ColumnDoesntExists:
            raise HTTPException(status_code=404, detail="Not found")
    return result
