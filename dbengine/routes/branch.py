from typing import List

from fastapi import APIRouter
from fastapi.exceptions import HTTPException
from fastapi_sqlalchemy import db

from dbengine.db_connector import CONNECTOR_DICT
from dbengine.exceptions import BranchError, BranchNotFoundError
from dbengine.methods import create_branch, get_branch, ok_branch, request_merge_branch, unrequest_merge_branch
from dbengine.models import BranchTypes
import dbengine.models
from dbengine.routes.models import Branch
from dbengine.settings import Settings

settings = Settings()

test_connector = CONNECTOR_DICT[settings.DWH_CONNECTION_TEST.scheme](settings.DWH_CONNECTION_TEST)
prod_connector = CONNECTOR_DICT[settings.DWH_CONNECTION_PROD.scheme](settings.DWH_CONNECTION_PROD)

branch_router = APIRouter(prefix="/branch", tags=["Branch"])


@branch_router.get("/{branch_id}", response_model=Branch)
async def http_get_branch(branch_id: int):
    try:
        return get_branch(branch_id, session=db.session)
    except BranchNotFoundError:
        raise HTTPException(status_code=404, detail="Branch not found")


@branch_router.post("/{branch_name}", response_model=Branch)
async def http_create_branch_by_name(branch_name: str) -> Branch:
    return create_branch(branch_name, session=db.session)


@branch_router.post("/{branch_id}/merge/request", response_model=Branch)
async def http_request_merge_branch(branch_id: int) -> Branch:
    test_connector.connect()
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchNotFoundError:
        raise HTTPException(status_code=404, detail="Branch not found")
    return request_merge_branch(branch, session=db.session, test_connector=test_connector)


@branch_router.post("/{branch_id}/merge/unrequest", response_model=Branch)
async def http_unreguest_merge_branch(branch_id: int) -> Branch:
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchNotFoundError:
        raise HTTPException(status_code=404, detail="Branch not found")
    return unrequest_merge_branch(branch, session=db.session)


@branch_router.post("/{branch_id}/merge/approve", response_model=Branch)
async def http_merge_branch(branch_id: int) -> Branch:
    prod_connector.connect()
    test_connector.connect()
    try:
        branch = get_branch(branch_id, session=db.session)
    except BranchNotFoundError:
        raise HTTPException(status_code=404, detail="Branch not found")
    return ok_branch(branch, session=db.session, prod_connector=prod_connector, test_connector=test_connector)


@branch_router.get("", response_model=List[Branch])
async def http_get_all_branches():
    return db.session.query(dbengine.models.Branch).all()


@branch_router.post("", response_model=Branch)
async def http_create_branch():
    try:
        return create_branch(name="default name", session=db.session)
    except BranchError as e:
        raise HTTPException(status_code=403, detail=e)


@branch_router.patch("/{branch_id}", response_model=Branch)
async def patch_branch(branch_id: int, name: str):
    branch = db.session.query(dbengine.models.Branch).get(branch_id)
    if not branch:
        raise HTTPException(status_code=404)
    if branch.type == BranchTypes.MAIN:
        raise HTTPException(status_code=403)
    branch.name = name
    db.session.flush()
