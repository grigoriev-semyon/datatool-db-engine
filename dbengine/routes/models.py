from datetime import datetime

from pydantic import BaseModel, Field

from dbengine.models import BranchTypes


class Commit(BaseModel):
    id: int = Field(..., alias="Commit id")
    prev_commit_id: int
    dev_branch_id: int
    branch_id: int
    attribute_id_in: int
    attribute_id_out: int
    create_ts: datetime


class Branch(BaseModel):
    id: int = Field(..., alias="Branch id")
    type: BranchTypes
    name: str
    create_ts: datetime


class DbTable(BaseModel):
    id: int = Field(..., alias="Table id")


class DbColumn(BaseModel):
    id: int = Field(..., alias="Column id")
    table_id: int = Field(..., alias="Table id")


class DbTableAttributes(BaseModel):
    id: int = Field(..., alias="Table attribute id")
    table_id: int = Field(..., alias="Table id")
    name: str


class DbColumnAttributes(BaseModel):
    id: int = Field(..., alias="Column attribute id")
    column_id: int = Field(..., alias="Column id")
    name: str
    datatype: str
