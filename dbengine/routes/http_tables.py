from typing import Tuple

from fastapi import APIRouter
from dbengine.branch import get_branch
from . import session
from dbengine.table import create_table, delete_table, update_table, get_table
from dbengine.models import DbTable, DbTableAttributes, Commit
from ..exceptions import TableDoesntExists

table_router = APIRouter(prefix="/table")


@table_router.post("/")
async def http_create_table(branch_id: int, table_name: str) -> Tuple[DbTable, DbTableAttributes, Commit]:
    branch = get_branch(branch_id, session=session)
    return create_table(branch, table_name, session=session)


@table_router.get("/{table_id}")
async def http_get_table(branch_id: int, table_id: int) -> Tuple[DbTable, DbTableAttributes]:
    branch = get_branch(branch_id, session=session)
    return get_table(branch, table_id, session=session)


@table_router.patch("/{table_id}")
async def http_update_table(branch_id: int, table_id: int, name: str) -> Tuple[DbTable, DbTableAttributes, Commit]:
    branch = get_branch(branch_id, session=session)
    table = get_table(branch, table_id, session=session)
    return update_table(branch, table[0], name, session=session)


@table_router.delete("/{table_id}")
async def http_delete_table(branch_id: int, table_id: int) -> Commit:
    branch = get_branch(branch_id, session=session)
    table = get_table(branch, table_id, session=session)
    return delete_table(branch, table[0], session=session)


@table_router.get("/")
async def http_get_tables_in_branch(branch_id: int):
    table_ids = session.query(DbTable).all()
    branch = get_branch(branch_id, session=session)
    result = []
    for row in table_ids:
        try:
            result.append(get_table(branch, row.id, session=session))
        except TableDoesntExists:
            pass
    return result