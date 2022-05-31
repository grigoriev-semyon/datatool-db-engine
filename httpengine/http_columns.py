from fastapi import APIRouter

column_router = APIRouter(prefix="/{branch_id}/column")


@column_router.post("/create/{table_id}/{name}/{datatype}")
async def http_create_column(branch_id: int, table_id: int, name: str, datatype: str):
    pass


@column_router.get("/get/{column_id}")
async def http_get_column(branch_id: int, column_id: int):
    pass


@column_router.patch("/update/{column_id}/{name}/{datatype}")
async def http_update_column(branch_id: int, column_id: int, name: str, datatype: str):
    pass


@column_router.delete("/delete/{column_id}")
async def http_delete_column(branch_id: int, column_id: int):
    pass
