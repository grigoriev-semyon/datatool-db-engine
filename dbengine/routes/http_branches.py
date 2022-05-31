from fastapi import APIRouter
from fastapi.exceptions import HTTPException
from sqlalchemy.exc import NoResultFound

from dbengine.branch import get_branch, request_merge_branch, unrequest_merge_branch, ok_branch, create_branch
from dbengine.models import Branch
from . import session

branch_router = APIRouter(prefix="/branch")


@branch_router.get("/{branch_id}")
async def http_get_branch(branch_id: int) -> Branch:
    return get_branch(branch_id, session=session)


@branch_router.post("/{branch_name}")
async def http_create_branch_by_name(branch_name: str) -> Branch:
    return create_branch(branch_name, session=session)


@branch_router.post("/{branch_id}/merge/request")
async def http_request_merge_branch(branch_id: int) -> Branch:
    branch = get_branch(branch_id, session=session)
    return request_merge_branch(branch, session=session)


@branch_router.post("/{branch_id}/merge/unrequest")
async def http_unreguest_merge_branch(branch_id: int) -> Branch:
    branch = get_branch(branch_id, session=session)
    return unrequest_merge_branch(branch, session=session)


@branch_router.post("/{branch_id}/merge/approve")
async def http_merge_branch(branch_id: int) -> Branch:
    branch = get_branch(branch_id, session=session)
    return ok_branch(branch, session=session)


@branch_router.get("/")
async def http_get_all_branches():
    return session.query(Branch).all()


@branch_router.post("/")
async def http_create_branch():
    return create_branch(name="default name", session=session)


@branch_router.patch("/{branch_id}")
async def patch_branch(branch_id: int, name: str):
    try:
        return session.query(Branch).filter(Branch.id == branch_id).update({name: name})
    except NoResultFound:
        raise HTTPException(status_code=404, detail="Not found")