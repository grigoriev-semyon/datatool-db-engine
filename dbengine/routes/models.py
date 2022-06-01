from datetime import datetime
from typing import Union

from pydantic import BaseModel, Field

from dbengine.models import BranchTypes


class MyModel(BaseModel):
    class Config:
        orm_mode = True


class Branch(MyModel):
    id: int = Field(..., title="Branch id")
    type: BranchTypes
    name: str
    create_ts: datetime


class Table(MyModel):
    id: int = Field(...)
    name: str


class Column(MyModel):
    id: int = Field(...)
    table_id: int = Field(...)
    name: str
    datatype: str
