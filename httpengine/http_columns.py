from fastapi import APIRouter
from . import session
from dbengine.column import get_column, delete_column, update_column, create_column
from dbengine.table import get_table
from dbengine.branch import get_branch

column_router = APIRouter(prefix="/{branch_id}/column")


@column_router.post("/create/{table_id}/{name}/{datatype}")
async def http_create_column(branch_id: int, table_id: int, name: str, datatype: str):
    branch = get_branch(branch_id, session=session)
    table = get_table(branch, table_id, session=session)
    return create_column(branch, table[0], name=name, datatype=datatype, session=session)


@column_router.get("/get/{column_id}")
async def http_get_column(branch_id: int, column_id: int):
    branch = get_branch(branch_id, session=session)
    return get_column(branch, column_id, session=session)


@column_router.patch("/update/{column_id}/{name}/{datatype}")
async def http_update_column(branch_id: int, column_id: int, name: str, datatype: str):
    branch = get_branch(branch_id, session=session)
    column = get_column(branch, column_id, session=session)
    return update_column(branch, column[0], name=name, datatype=datatype, session=session)


@column_router.delete("/delete/{column_id}")
async def http_delete_column(branch_id: int, column_id: int):
    branch = get_branch(branch_id, session=session)
    column = get_column(branch, column_id, session=session)
    return delete_column(branch, column[0], session=session)
