from fastapi import FastAPI
from .http_branches import branch_router
from .http_tables import table_router
from .http_columns import column_router

app = FastAPI()

app.include_router(branch_router)
app.include_router(table_router)
app.include_router(column_router)