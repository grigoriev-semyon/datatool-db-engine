from fastapi import APIRouter

branch_router = APIRouter(prefix="/branch")


@branch_router.get("/get/{branch_id}")
async def http_get_branch(branch_id: int):
    pass


@branch_router.post("/create/{branch_name}")
async def http_create_branch(branch_name: str):
    pass


@branch_router.patch("/request/{branch_id}")
async def http_request_merge_branch(branch_id: int):
    pass


@branch_router.patch("/unrequest/{branch_id}")
async def http_unreguest_merge_branch(branch_id: int):
    pass


@branch_router.patch("/merge/{branch_id}")
async def http_merge_branch(branch_id: int):
    pass
