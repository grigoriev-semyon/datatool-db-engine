from datetime import datetime
from typing import Union

from pydantic import BaseModel, Field

from dbengine.models import BranchTypes


class Commit(BaseModel):
    id: int = Field(...)
    prev_commit_id: Union[int, None]
    dev_branch_id: Union[int, None]
    branch_id: int
    attribute_id_in: Union[int, None]
    attribute_id_out: Union[int, None]
    create_ts: datetime

    class Config:
        orm_mode = True


class Branch(BaseModel):
    id: int = Field(..., title="Branch id")
    type: BranchTypes
    name: str
    create_ts: datetime

    class Config:
        orm_mode = True


class DbTable(BaseModel):
    id: int = Field(..., )

    class Config:
        orm_mode = True


class DbColumn(BaseModel):
    id: int = Field(...)
    table_id: int = Field(...)


class DbTableAttributes(BaseModel):
    id: int = Field(...)
    table_id: int = Field(...)
    name: str

    class Config:
        orm_mode = True


class DbColumnAttributes(BaseModel):
    id: int = Field(...)
    column_id: int = Field(...)
    name: str
    datatype: str

    class Config:
        orm_mode = True
