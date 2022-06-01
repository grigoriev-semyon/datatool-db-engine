from datetime import datetime
from typing import Union

from pydantic import BaseModel, Field

from dbengine.models import BranchTypes


class MyModel(BaseModel):
    class Config:
        orm_mode = True


class Commit(MyModel):
    id: int = Field(...)
    prev_commit_id: Union[int, None]
    dev_branch_id: Union[int, None]
    branch_id: int
    attribute_id_in: Union[int, None]
    attribute_id_out: Union[int, None]
    create_ts: datetime


class Branch(MyModel):
    id: int = Field(..., title="Branch id")
    type: BranchTypes
    name: str
    create_ts: datetime


class DbTable(MyModel):
    id: int = Field(..., )


class DbColumn(MyModel):
    id: int = Field(...)
    table_id: int = Field(...)


class DbTableAttributes(MyModel):
    id: int = Field(...)
    table_id: int = Field(...)
    name: str


class DbColumnAttributes(MyModel):
    id: int = Field(...)
    column_id: int = Field(...)
    name: str
    datatype: str
