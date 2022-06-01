from typing import List

from fastapi import APIRouter
from fastapi.exceptions import HTTPException
from fastapi_sqlalchemy import db

from dbengine.exceptions import BranchNotFoundError, TableDoesntExists, TableDeleted, \
    ProhibitedActionInBranch
from dbengine.methods import create_column, delete_column, get_branch, get_column, get_table, update_column
from dbengine.methods.aggregators import column_aggregator
from dbengine.methods.column import get_columns
from dbengine.routes.models import Column

column_router = APIRouter(prefix="/table/{table_id}/column", tags=["Column"])


@column_router.post("", response_model=Column)
async def http_create_column(
        branch_id: int, table_id: int, name: str, datatype: str):
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchNotFoundError:
        raise HTTPException(status_code=404, detail="Branch not found")
    try:
        table = get_table(branch, table_id, session=db.session)
    except TableDoesntExists as e:
        raise HTTPException(status_code=404, detail=e)
    except TableDeleted as e:
        raise HTTPException(status_code=410, detail=e)
    try:
        result = create_column(branch, table[0], name=name, datatype=datatype, session=db.session)
    except ProhibitedActionInBranch as e:
        raise HTTPException(status_code=403, detail=e)
    return column_aggregator(result[0], result[1])


@column_router.get("/{column_id}", response_model=Column)
async def http_get_column(branch_id: int, column_id: int):
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchNotFoundError:
        raise HTTPException(status_code=404, detail="Branch not found")
    result = get_column(branch, column_id, session=db.session)
    return column_aggregator(result[0], result[1])


@column_router.patch("/{column_id}", response_model=Column)
async def http_update_column(
        branch_id: int, column_id: int, name: str, datatype: str):
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchNotFoundError:
        raise HTTPException(status_code=404, detail="Branch not found")
    column = get_column(branch, column_id, session=db.session)
    try:
        result = update_column(branch, column[0], name=name, datatype=datatype, session=db.session)
    except ProhibitedActionInBranch:
        raise HTTPException(status_code=403, detail="Forbidden")
    return column_aggregator(result[0], result[1])


@column_router.delete("/{column_id}")
async def http_delete_column(branch_id: int, column_id: int):
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchNotFoundError:
        raise HTTPException(status_code=404, detail="Branch not found")
    column = get_column(branch, column_id, session=db.session)
    try:
        delete_column(branch, column[0], session=db.session)
    except ProhibitedActionInBranch:
        raise HTTPException(status_code=403, detail="Forbidden")


@column_router.get("", response_model=List[Column])
async def http_get_columns_in_branch(branch_id: int, table_id: int):
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchNotFoundError:
        raise HTTPException(status_code=404, detail="Branch not found")
    try:
        table = get_table(branch, table_id, session=db.session)[0]
    except TableDoesntExists as e:
        raise HTTPException(status_code=404, detail=e)
    except TableDeleted as e:
        raise HTTPException(status_code=410, detail=e)
    column_ids = get_columns(branch, table, session=db.session)
    result = []
    for row in column_ids:
        column = get_column(branch, row, session=db.session)
        result.append(column_aggregator(column[0], column[1]))
    return result
