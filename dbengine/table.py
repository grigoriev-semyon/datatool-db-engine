import logging
from typing import Tuple

import sqlalchemy.exc
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from .exceptions import ProhibitedActionInBranch, TableDoesntExists, TableDeleted
from .models import Branch, Commit, DbTable, DbTableAttributes, BranchTypes, AttributeTypes, DbColumn
from .column import delete_column, get_column

logger = logging.getLogger(__name__)


def create_table(branch: Branch, name: str, *, session: Session) -> Tuple[DbTable, DbTableAttributes, Commit]:
    """Create table in branch with optional columns"""
    logging.debug("create_table")
    try:
        if branch.type != BranchTypes.WIP:
            raise ProhibitedActionInBranch("Table creating", branch.name)
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        new_commit = Commit(branch_id=branch.id, attribute_id_in=None)
        if s:
            new_commit.prev_commit_id = s.id
        new_table = DbTable(type=AttributeTypes.TABLE)
        session.add(new_table)
        session.flush()
        new_table_attribute = DbTableAttributes(type=AttributeTypes.TABLE, table_id=new_table.id, name=name)
        session.add(new_table_attribute)
        session.flush()
        new_commit.attribute_id_out = new_table_attribute.id
        session.add(new_commit)
        session.flush()

        return new_table, new_table_attribute, new_commit
    except AttributeError:
        logging.error(AttributeError, exc_info=True)


def get_table(branch: Branch, id: int, *, session: Session) -> Tuple[DbTable, DbTableAttributes]:
    """Return table and last attributes in branch by id or name"""
    logging.debug("get_table")
    try:
        commit_in_branch = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(
            Commit.id.desc()).first()
        prev_commit = commit_in_branch.prev_commit_id
        while True:
            attr_out, attr_in = commit_in_branch.attribute_id_out, commit_in_branch.attribute_id_in
            if attr_in is None and attr_out is None:
                if not prev_commit:
                    raise TableDoesntExists(id, branch.name)
                commit_in_branch = session.query(Commit).filter(and_(Commit.id == prev_commit,
                                                                     or_(Commit.branch_id == 1,
                                                                         Commit.branch_id == branch.id))).one_or_none()
                prev_commit = commit_in_branch.prev_commit_id
            elif attr_in is not None and attr_out is None:
                s = session.query(DbTableAttributes).filter(DbTableAttributes.id == attr_in).one_or_none()
                if s:
                    if s.table_id == id:
                        raise TableDeleted(id, branch.name)
                else:
                    if not prev_commit:
                        raise TableDoesntExists(id, branch.name)
                    commit_in_branch = session.query(Commit).filter(and_(Commit.id == prev_commit,
                                                                         or_(Commit.branch_id == 1,
                                                                             Commit.branch_id == branch.id))).one_or_none()
                    prev_commit = commit_in_branch.prev_commit_id
            elif (attr_in is not None and attr_out is not None) or (attr_in is None and attr_out is not None):
                s = session.query(DbTableAttributes).filter(DbTableAttributes.id == attr_out).one_or_none()
                if s:
                    if s.table_id == id:
                        return (session.query(DbTable).filter(DbTable.id == id).one(),
                                session.query(DbTableAttributes)
                                .filter(and_(DbTableAttributes.table_id == id, DbTableAttributes.id == attr_out))
                                .one())

                commit_in_branch = session.query(Commit).filter(and_(Commit.id == prev_commit,
                                                                     or_(Commit.branch_id == 1,
                                                                         Commit.branch_id == branch.id))).one_or_none()
                prev_commit = commit_in_branch.prev_commit_id
    except sqlalchemy.exc.NoResultFound:
        logging.error(sqlalchemy.exc.NoResultFound, exc_info=True)


def update_table(
        branch: Branch,
        table: DbTable,
        name: str,
        *,
        session: Session,
) -> Tuple[DbTable, DbTableAttributes, Commit]:
    """Change name of table and commit to branch"""
    logging.debug("update_table")
    table_and_last_attributes = get_table(branch, table.id, session=session)
    try:
        if branch.type != BranchTypes.WIP:
            raise ProhibitedActionInBranch("Table altering", branch.name)
        s = (session.query(Commit).filter(
            and_(Commit.branch_id == branch.id,
                 Commit.attribute_id_out == table_and_last_attributes[1].id))).one_or_none()
        s_main = session.query(Commit).filter(
            and_(Commit.branch_id == 1, Commit.attribute_id_out == table_and_last_attributes[1].id)).one_or_none()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s.id
        elif (not s) and s_main:
            new_commit.prev_commit_id = s_main.id
        elif not (s and s_main):
            raise TableDoesntExists(table.id, branch.name)
        new_commit.attribute_id_in = table_and_last_attributes[1].id
        new_table_attribute = DbTableAttributes(type=AttributeTypes.TABLE, table_id=table_and_last_attributes[0].id,
                                                name=name)
        session.add(new_table_attribute)
        session.flush()
        new_commit.attribute_id_out = new_table_attribute.id
        session.add(new_commit)
        session.flush()

        return table_and_last_attributes[0], new_table_attribute, new_commit
    except AttributeError:
        logging.error(AttributeError, exc_info=True)


def delete_table(
        branch: Branch,
        table: DbTable,
        *,
        session: Session,
) -> Commit:
    """Delete table in branch"""
    logging.debug("delete_table")
    table_and_last_attributes = get_table(branch, table.id, session=session)
    try:
        if branch.type != BranchTypes.WIP:
            raise ProhibitedActionInBranch("Deleting table", branch.name)
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        columns = session.query(DbColumn).filter(DbColumn.table_id == table.id).all()
        new_commit = Commit(attribute_id_in=table_and_last_attributes[1].id, attribute_id_out=None, branch_id=branch.id)
        if s:
            new_commit.prev_commit_id = s.id
        session.add(new_commit)
        session.flush()
        for row in columns:
            delete_column(branch, row, session=session)
        return new_commit
    except AttributeError:
        logging.error(AttributeError, exc_info=True)
