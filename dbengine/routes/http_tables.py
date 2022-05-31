from typing import Tuple

from fastapi import APIRouter
from fastapi_sqlalchemy import db

from dbengine.branch import get_branch
from dbengine.table import create_table, delete_table, update_table, get_table
from dbengine.models import DbTable, DbTableAttributes, Commit
from dbengine.exceptions import TableDoesntExists
from dbengine.models import Commit, DbTable, DbTableAttributes
from dbengine.table import create_table, delete_table, get_table, update_table, get_tables


table_router = APIRouter(prefix="/table", tags=["Table"])


@table_router.post("")
async def http_create_table(branch_id: int, table_name: str) -> Tuple[DbTable, DbTableAttributes, Commit]:
    branch = get_branch(branch_id, session=db.session)
    return create_table(branch, table_name, session=db.session)


@table_router.get("/{table_id}")
async def http_get_table(branch_id: int, table_id: int) -> Tuple[DbTable, DbTableAttributes]:
    branch = get_branch(branch_id, session=db.session)
    return get_table(branch, table_id, session=db.session)


@table_router.patch("/{table_id}")
async def http_update_table(branch_id: int, table_id: int, name: str) -> Tuple[DbTable, DbTableAttributes, Commit]:
    branch = get_branch(branch_id, session=db.session)
    table = get_table(branch, table_id, session=db.session)
    return update_table(branch, table[0], name, session=db.session)


@table_router.delete("/{table_id}")
async def http_delete_table(branch_id: int, table_id: int) -> Commit:
    branch = get_branch(branch_id, session=db.session)
    table = get_table(branch, table_id, session=db.session)
    return delete_table(branch, table[0], session=db.session)


@table_router.get("")
async def http_get_tables_in_branch(branch_id: int):
    branch = get_branch(branch_id, session=db.session)
    table_ids = get_tables(branch, session=db.session)
    result = []
    for row in table_ids:
        try:
            result.append(get_table(branch, row, session=db.session))
        except TableDoesntExists:
            pass
    return result
