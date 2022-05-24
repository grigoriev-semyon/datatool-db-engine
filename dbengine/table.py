import logging
from typing import List, Optional, Tuple, Union

from sqlalchemy import and_

from .exceptions import ProhibitedActionInBranch
from .models import Branch, Commit, DbColumn, DbTable, DbTableAttributes, BranchTypes, session, AttributeTypes

logger = logging.getLogger(__name__)


def create_table(
        branch: Branch, name: str
) -> Tuple[DbTable, DbTableAttributes, Commit]:
    """Create table in branch with optional columns"""
    logging.debug("create_table")
    if branch.type != BranchTypes.MAIN:
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s.previous_commit_id + 1
        new_commit.attribute_id_in = None
        new_table = DbTable()
        new_table.type = AttributeTypes.TABLE
        session.add(new_table)
        session.flush()
        session.commit()
        new_table_attribute = DbTableAttributes()
        new_table_attribute.type = AttributeTypes.TABLE
        new_table_attribute.table_id = new_table.id
        new_table_attribute.name = name
        session.add(new_table_attribute)
        session.flush()
        session.commit()
        new_commit.attribute_id_out = new_table.id
        session.add(new_commit)
        session.flush()
        session.commit()
    else:
        raise ProhibitedActionInBranch("Table creating", branch.name)
    return new_table, new_table_attribute, new_commit


def get_table(
        branch: Branch, id: int
) -> Tuple[DbTable, DbTableAttributes]:
    """Return table and last attributes in branch by id or name"""
    logging.debug("get_table")
    s = session.query(Commit).filter(and_(Commit.branch_id == branch.id, Commit.attribute_id_out == id)).order_by(
        Commit.id.desc()).first()
    return session.query(DbTable).filter(DbTable.id == s.attribute_id_out).one(), session.query(
        DbTableAttributes).filter(
        DbTableAttributes.table_id == s.attribute_id_out).one()


def update_table(
        branch: Branch, table: DbTable, name: str
) -> Tuple[DbTable, DbTableAttributes, Commit]:
    """Change name of table and commit to branch"""
    logging.debug("update_table")
    if branch.type != BranchTypes.MAIN:
        s = (session.query(Commit).filter(Commit.branch_id == branch.id)).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s.previous_commit_id + 1
        new_commit.attribute_id_in = table.id
        new_table = DbTable()
        new_table.type = AttributeTypes.TABLE
        session.add(new_table)
        session.flush()
        session.commit()
        new_table_attribute = DbTableAttributes()
        new_table_attribute.type = AttributeTypes.TABLE
        new_table_attribute.table_id = new_table.id
        new_table_attribute.name = name
        session.add(new_table_attribute)
        session.flush()
        session.commit()
        new_commit.attribute_id_out = new_table.id
        session.add(new_commit)
        session.flush()
        session.commit()
    else:
        raise ProhibitedActionInBranch("Table altering", branch.name)
    return new_table, new_table_attribute, new_commit


def delete_table(branch: Branch, table: DbTable) -> Commit:
    """Delete table in branch"""
    logging.debug("delete_table")
    if branch.type != BranchTypes.MAIN:
        s = session.query(Commit).filter(Commit.branch_id == table.id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.attribute_id_in = table.id
        if s:
            new_commit.prev_commit_id = s.previous_commit_id + 1
        new_commit.attribute_id_out = None
        session.add(new_commit)
        session.flush()
        session.commit()
    else:
        raise ProhibitedActionInBranch("Deleting table", branch.name)
    return new_commit
