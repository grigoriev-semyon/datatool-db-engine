from fastapi import APIRouter
from dbengine.branch import get_branch, request_merge_branch, unrequest_merge_branch, ok_branch, create_branch
from . import session
from dbengine.models import Branch

branch_router = APIRouter(prefix="/branch")


@branch_router.get("/get/{branch_id}")
async def http_get_branch(branch_id: int) -> Branch:
    return get_branch(branch_id, session=session)


@branch_router.post("/create/{branch_name}")
async def http_create_branch(branch_name: str) -> Branch:
    return create_branch(branch_name, session=session)


@branch_router.patch("/request/{branch_id}")
async def http_request_merge_branch(branch_id: int) -> Branch:
    branch = get_branch(branch_id, session=session)
    return request_merge_branch(branch, session=session)


@branch_router.patch("/unrequest/{branch_id}")
async def http_unreguest_merge_branch(branch_id: int) -> Branch:
    branch = get_branch(branch_id, session=session)
    return unrequest_merge_branch(branch, session=session)


@branch_router.patch("/merge/{branch_id}")
async def http_merge_branch(branch_id: int) -> Branch:
    branch = get_branch(branch_id, session=session)
    return ok_branch(branch, session=session)
