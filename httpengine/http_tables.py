from fastapi import APIRouter

table_router = APIRouter(prefix="/{branch_id}/table")


@table_router.post("//create/{table_name}")
async def http_create_table(branch_id: int, table_name: str):
    pass


@table_router.get("/get/{table_id}")
async def http_get_table(branch_id: int, table_id: int):
    pass


@table_router.patch("/update/{table_id}/{name}")
async def http_update_table(branch_id: int, table_id: int, name: str):
    pass


@table_router.delete("/delete/{table_id}")
async def http_delete_table(branch_id: int, table_id: int):
    pass
