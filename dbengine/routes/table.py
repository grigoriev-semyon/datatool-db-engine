from typing import List, Tuple

from fastapi import APIRouter
from fastapi_sqlalchemy import db

from dbengine.exceptions import TableDoesntExists
from dbengine.methods import create_table, delete_table, get_branch, get_table, get_tables, update_table
from dbengine.routes.models import Table
from dbengine.methods.aggregators import table_aggregator


table_router = APIRouter(prefix="/table", tags=["Table"])


@table_router.post("", response_model=Table)
async def http_create_table(branch_id: int, table_name: str):
    branch = get_branch(branch_id, session=db.session)
    result = create_table(branch, table_name, session=db.session)
    return table_aggregator((result[0], result[1]))


@table_router.get("/{table_id}", response_model=Table)
async def http_get_table(branch_id: int, table_id: int):
    branch = get_branch(branch_id, session=db.session)
    return table_aggregator(get_table(branch, table_id, session=db.session))


@table_router.patch("/{table_id}", response_model=Table)
async def http_update_table(branch_id: int, table_id: int, name: str):
    branch = get_branch(branch_id, session=db.session)
    table = get_table(branch, table_id, session=db.session)
    result = update_table(branch, table[0], name, session=db.session)
    return table_aggregator((result[0], result[1]))


@table_router.delete("/{table_id}", response_model=str)
async def http_delete_table(branch_id: int, table_id: int):
    branch = get_branch(branch_id, session=db.session)
    table = get_table(branch, table_id, session=db.session)
    delete_table(branch, table[0], session=db.session)
    return "Table deleted"


@table_router.get("", response_model=List[Table])
async def http_get_tables_in_branch(branch_id: int):
    branch = get_branch(branch_id, session=db.session)
    table_ids = get_tables(branch, session=db.session)
    result = []
    for row in table_ids:
        table = get_table(branch, row, session=db.session)
        result.append(table_aggregator(table))
    return result
