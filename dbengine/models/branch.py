from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Iterator, List

from sqlalchemy import Column, DateTime
from sqlalchemy import Enum as EnumDb
from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, Session

from .base import Base
from dbengine.methods import get_tables, get_table
from ..methods.column import get_columns, get_column


class BranchTypes(str, Enum):
    MAIN = "MAIN BRANCH"
    WIP = "WORK IN PROGRESS"
    MR = "MERGE REQUEST"
    MERGED = "MERGED"


class CommitActionTypes(str, Enum):
    ALTER = "ALTER"
    DROP = "DROP"
    CREATE = "CREATE"


class Branch(Base):
    id = Column(Integer, primary_key=True)
    type = Column(EnumDb(BranchTypes, native_enum=False), default=BranchTypes.WIP)
    name = Column(String)
    create_ts = Column(DateTime, default=datetime.utcnow, nullable=False)

    _commits: List[Commit] = relationship(
        "Commit", foreign_keys="Commit.branch_id", order_by="desc(Commit.create_ts)"
    )

    @hybrid_property
    def last_commit(self) -> Commit:
        return self._commits[0]

    @hybrid_property
    def commits(self) -> Iterator[Commit]:
        commit = self.last_commit
        yield commit
        while commit := commit.prev_commit:
            yield commit

    def __repr__(self):
        return f"<Branch id={self.id} type={self.type}>"

    def get_all_tables_and_columns_in_branch(self, session: Session):
        table_ids = get_tables(self, session=session)
        tables = []
        columns = []
        result = []
        for tablerow in table_ids:
            table = get_table(self, tablerow, session=session)
            tables.append(table)
            column_ids = get_columns(self, table[0], session=session)
            for columnrow in column_ids:
                column = get_column(self, columnrow, session=session)
                columns.append(column)
            result.append((table, tuple(columns)))
            columns = []
        return result


class Commit(Base):
    id = Column(Integer, primary_key=True)
    prev_commit_id = Column(Integer, ForeignKey("commit.id"))
    dev_branch_id = Column(Integer, ForeignKey("branch.id"), default=None)
    branch_id = Column(Integer, ForeignKey("branch.id"))
    attribute_id_in = Column(Integer, ForeignKey("db_attributes.id"))
    attribute_id_out = Column(Integer, ForeignKey("db_attributes.id"))
    create_ts = Column(DateTime, default=datetime.utcnow, nullable=False)
    sql_up = Column(String)
    sql_down = Column(String)

    next_commit: List[Commit]
    prev_commit: Commit = relationship("Commit", foreign_keys=[prev_commit_id], backref="next_commit", remote_side=id)

    branch: Branch = relationship("Branch", foreign_keys=[branch_id], back_populates="_commits")

    def __repr__(self):
        return f"<Commit id={self.id} branch_id={self.branch_id}>"
