import logging
from typing import List, Optional, Tuple, Union

import sqlalchemy.exc
from sqlalchemy import and_

from .exceptions import ProhibitedActionInBranch, TableDoesntExists, TableDeleted
from .models import Branch, Commit, DbColumn, DbTable, DbTableAttributes, BranchTypes, session, AttributeTypes

logger = logging.getLogger(__name__)


def create_table(
        branch: Branch, name: str
) -> Tuple[DbTable, DbTableAttributes, Commit]:
    """Create table in branch with optional columns"""
    logging.debug("create_table")
    try:
        if branch.type != BranchTypes.WIP:
            raise ProhibitedActionInBranch("Table creating", branch.name)
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s.id
        new_commit.attribute_id_in = None
        new_table = DbTable()
        new_table.type = AttributeTypes.TABLE
        session.add(new_table)
        session.flush()
        new_table_attribute = DbTableAttributes()
        new_table_attribute.type = AttributeTypes.TABLE
        new_table_attribute.table_id = new_table.id
        new_table_attribute.name = name
        session.add(new_table_attribute)
        session.flush()
        new_commit.attribute_id_out = new_table_attribute.id
        session.add(new_commit)
        session.flush()
        session.commit()
        return new_table, new_table_attribute, new_commit
    except AttributeError:
        logging.error(AttributeError, exc_info=True)


def get_table(
        branch: Branch, id: int
) -> Tuple[DbTable, DbTableAttributes]:
    """Return table and last attributes in branch by id or name"""
    logging.debug("get_table")
    try:
        attr_id = session.query(DbTableAttributes).filter(DbTableAttributes.table_id == id).order_by(
            DbTableAttributes.id).first().id
        commits = session.query(Commit).filter(Commit.branch_id == branch.id).filter(
            Commit.attribute_id_out == attr_id).filter(Commit.attribute_id_in.is_(None)).one_or_none()
        if not commits:
            raise TableDoesntExists(id, branch.name)
        while True:
            commits = session.query(Commit).filter(
                    and_(Commit.branch_id == branch.id, Commit.attribute_id_in == attr_id)).one_or_none()
            if not commits:
                break
            if commits.attribute_id_out is None:
                raise TableDeleted(id, branch.name)
            attr_id = commits.attribute_id_out
        return session.query(DbTable).filter(DbTable.id == id).one(), session.query(
            DbTableAttributes).filter(and_(DbTableAttributes.table_id == id, DbTableAttributes.id == attr_id)).one()
    except sqlalchemy.exc.NoResultFound:
        logging.error(sqlalchemy.exc.NoResultFound, exc_info=True)


def update_table(
        branch: Branch, table_and_last_attributes: Tuple[DbTable, DbTableAttributes], name: str
) -> Tuple[DbTable, DbTableAttributes, Commit]:
    """Change name of table and commit to branch"""
    logging.debug("update_table")
    try:
        if branch.type != BranchTypes.WIP:
            raise ProhibitedActionInBranch("Table altering", branch.name)
        s = (session.query(Commit).filter(Commit.branch_id == branch.id)).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s.id
        new_commit.attribute_id_in = table_and_last_attributes[1].id
        new_table_attribute = DbTableAttributes()
        new_table_attribute.type = AttributeTypes.TABLE
        new_table_attribute.table_id = table_and_last_attributes[0].id
        new_table_attribute.name = name
        session.add(new_table_attribute)
        session.flush()
        new_commit.attribute_id_out = new_table_attribute.id
        session.add(new_commit)
        session.flush()
        session.commit()
        return table_and_last_attributes[0], new_table_attribute, new_commit
    except AttributeError:
        logging.error(AttributeError, exc_info=True)


def delete_table(branch: Branch, table_and_last_attributes: Tuple[DbTable, DbTableAttributes]) -> Commit:
    """Delete table in branch"""
    logging.debug("delete_table")
    try:
        if branch.type != BranchTypes.WIP:
            raise ProhibitedActionInBranch("Deleting table", branch.name)
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.attribute_id_in = table_and_last_attributes[1].id
        if s:
            new_commit.prev_commit_id = s.id
        new_commit.attribute_id_out = None
        new_commit.branch_id = branch.id
        session.add(new_commit)
        session.flush()
        session.commit()
        return new_commit
    except AttributeError:
        logging.error(AttributeError, exc_info=True)
