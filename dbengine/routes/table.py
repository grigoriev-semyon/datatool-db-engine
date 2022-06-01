from typing import List

from fastapi import APIRouter, HTTPException
from fastapi_sqlalchemy import db

from dbengine.exceptions import TableDoesntExists, TableDeleted, BranchError, ProhibitedActionInBranch
from dbengine.methods import create_table, delete_table, get_branch, get_table, get_tables, update_table
from dbengine.methods.aggregators import table_aggregator
from dbengine.routes.models import Table

table_router = APIRouter(prefix="/table", tags=["Table"])


@table_router.post("", response_model=Table)
async def http_create_table(branch_id: int, table_name: str):
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchError:
        raise HTTPException(status_code=404, detail="Branch not found")
    try:
        result = create_table(branch, table_name, session=db.session)
    except ProhibitedActionInBranch as e:
        raise HTTPException(status_code=403, detail="Forbidden")
    return table_aggregator(result[0], result[1])


@table_router.get("/{table_id}", response_model=Table)
async def http_get_table(branch_id: int, table_id: int):
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchError:
        raise HTTPException(status_code=404, detail="Branch not found")
    try:
        table = get_table(branch, table_id, session=db.session)
        return table_aggregator(table[0], table[1])
    except TableDoesntExists as e:
        raise HTTPException(status_code=404, detail=e)
    except TableDeleted as e:
        raise HTTPException(status_code=410, detail=e)


@table_router.patch("/{table_id}", response_model=Table)
async def http_update_table(branch_id: int, table_id: int, name: str):
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchError:
        raise HTTPException(status_code=404, detail="Branch not found")
    try:
        table = get_table(branch, table_id, session=db.session)
    except TableDoesntExists as e:
        raise HTTPException(status_code=404, detail=e)
    except TableDeleted as e:
        raise HTTPException(status_code=410, detail=e)
    try:
        result = update_table(branch, table[0], name, session=db.session)
    except ProhibitedActionInBranch as e:
        raise HTTPException(status_code=403, detail="Forbidden")
    return table_aggregator(result[0], result[1])


@table_router.delete("/{table_id}")
async def http_delete_table(branch_id: int, table_id: int):
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchError:
        raise HTTPException(status_code=404, detail="Branch not found")
    try:
        table = get_table(branch, table_id, session=db.session)
    except TableDoesntExists as e:
        raise HTTPException(status_code=404, detail=e)
    except TableDeleted:
        raise HTTPException(status_code=403, detail="Forbidden")
    try:
        delete_table(branch, table[0], session=db.session)
    except ProhibitedActionInBranch as e:
        raise HTTPException(status_code=410, detail=e)


@table_router.get("", response_model=List[Table])
async def http_get_tables_in_branch(branch_id: int):
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchError:
        raise HTTPException(status_code=404, detail="Branch not found")
    table_ids = get_tables(branch, session=db.session)
    result = []
    for row in table_ids:
        table = get_table(branch, row, session=db.session)
        result.append(table_aggregator(table[0], table[1]))
    return result
