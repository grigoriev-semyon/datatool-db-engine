from datetime import datetime
from enum import Enum

from sqlalchemy import Column, DateTime
from sqlalchemy import Enum as EnumDb
from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from .base import Base


class BranchTypes(str, Enum):
    MAIN = "MAIN BRANCH"
    WIP = "WORK IN PROGRESS"
    MR = "MERGE REQUEST"
    MERGED = "MERGED"


class Branch(Base):
    id = Column(Integer, primary_key=True)
    type = Column(EnumDb(BranchTypes, native_enum=False), default=BranchTypes.WIP)
    name = Column(String)
    create_ts = Column(DateTime, default=datetime.utcnow, nullable=False)


class Commit(Base):
    id = Column(Integer, primary_key=True)
    prev_commit_id = Column(Integer, ForeignKey("commit.id"))
    dev_branch_id = Column(Integer, ForeignKey("branch.id"), default=None)
    branch_id = Column(Integer)
    attribute_id_in = Column(Integer, ForeignKey("db_attributes.id"))
    attribute_id_out = Column(Integer, ForeignKey("db_attributes.id"))
    create_ts = Column(DateTime, default=datetime.utcnow, nullable=False)
