from fastapi import APIRouter
from dbengine.branch import get_branch
from . import session
from dbengine.table import create_table, delete_table, update_table, get_table

table_router = APIRouter(prefix="/{branch_id}/table")


@table_router.post("/create/{table_name}")
async def http_create_table(branch_id: int, table_name: str):
    branch = get_branch(branch_id, session=session)
    return create_table(branch, table_name, session=session)


@table_router.get("/get/{table_id}")
async def http_get_table(branch_id: int, table_id: int):
    branch = get_branch(branch_id, session=session)
    return get_table(branch, table_id, session=session)


@table_router.patch("/update/{table_id}/{name}")
async def http_update_table(branch_id: int, table_id: int, name: str):
    branch = get_branch(branch_id, session=session)
    table = get_table(branch, table_id, session=session)
    return update_table(branch, table[0], name)


@table_router.delete("/delete/{table_id}")
async def http_delete_table(branch_id: int, table_id: int):
    branch = get_branch(branch_id, session=session)
    table = get_table(branch, table_id, session=session)
    return delete_table(branch, table[0], session=session)
