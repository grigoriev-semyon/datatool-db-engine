import logging
from typing import Optional, Tuple

import sqlalchemy.exc
from sqlalchemy.orm import Session
from sqlalchemy import and_

from .exceptions import ProhibitedActionInBranch, ColumnDeleted, ColumnDoesntExists
from .models import Branch, Commit, DbColumn, DbColumnAttributes, DbTable, BranchTypes, AttributeTypes

logger = logging.getLogger(__name__)


def create_column(
        branch: Branch, table: DbTable, *, name: str, datatype: str, session: Session
) -> Tuple[DbColumn, DbColumnAttributes, Commit]:
    """Create column in table in branch"""
    logging.debug('create_column')
    try:
        if branch.type != BranchTypes.WIP:
            raise ProhibitedActionInBranch("Column creating", branch.name)
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s.id
        new_commit.attribute_id_in = None
        new_column = DbColumn()
        new_column.type = AttributeTypes.COLUMN
        new_column.table_id = table.id
        session.add(new_column)
        session.flush()
        new_column_attribute = DbColumnAttributes()
        new_column_attribute.type = AttributeTypes.COLUMN
        new_column_attribute.column_id = new_column.id
        new_column_attribute.datatype = datatype
        new_column_attribute.name = name
        session.add(new_column_attribute)
        session.flush()
        new_commit.attribute_id_out = new_column_attribute.id
        session.add(new_commit)
        session.flush()

        return new_column, new_column_attribute, new_commit
    except AttributeError:
        logging.error(AttributeError, exc_info=True)


def get_column(branch: Branch, id: int, *, session: Session) -> Tuple[DbColumn, DbColumnAttributes]:
    """Get last version of column in table in branch"""
    logging.debug('get_column')
    try:
        column_created_in_main: bool = False
        attr_id = (
            session.query(DbColumnAttributes)
                .filter(DbColumnAttributes.column_id == id)
                .order_by(DbColumnAttributes.id)
                .first()
                .id
        )
        if not attr_id:
            raise ColumnDoesntExists(id, branch.name)
        first_commit_in_branch_id = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(
            Commit.id).first().id
        first_commit_in_main = session.query(Commit).filter(
            and_(Commit.branch_id == 1, Commit.id < first_commit_in_branch_id, Commit.attribute_id_in.is_(None),
                 Commit.attribute_id_out == attr_id)).one_or_none()
        if first_commit_in_main:
            column_created_in_main = True
            while True:
                commits = session.query(Commit).filter(
                    and_(Commit.branch_id == 1, Commit.id < first_commit_in_branch_id,
                         Commit.attribute_id_in == attr_id)).one_or_none()
                if not commits:
                    break
                if commits.attribute_id_out is None:
                    raise ColumnDeleted(id, BranchTypes.MAIN)
                attr_id = commits.attribute_id_out
        if column_created_in_main:
            while True:
                commits = session.query(Commit).filter(
                    and_(Commit.branch_id == branch.id, Commit.attribute_id_in == attr_id)).one_or_none()
                if not commits:
                    break
                if commits.attribute_id_out is None:
                    raise ColumnDeleted(id, branch.name)
                attr_id = commits.attribute_id_out
            return (session.query(DbColumn).filter(DbColumn.id == id).one(),
                    session.query(DbColumnAttributes)
                    .filter(and_(DbColumnAttributes.column_id == id, DbColumnAttributes.id == attr_id))
                    .one())
        else:
            commits = (
                session.query(Commit)
                    .filter(Commit.branch_id == branch.id)
                    .filter(Commit.attribute_id_out == attr_id)
                    .filter(Commit.attribute_id_in.is_(None))
                    .one_or_none()
            )
            if not commits:
                raise ColumnDoesntExists(id, branch.name)
            while True:
                commits = (
                    session.query(Commit)
                        .filter(and_(Commit.branch_id == branch.id, Commit.attribute_id_in == attr_id))
                        .one_or_none()
                )
                if not commits:
                    break
                if commits.attribute_id_out is None:
                    raise ColumnDeleted(id, branch.name)
                attr_id = commits.attribute_id_out
            return (session.query(DbColumn).filter(DbColumn.id == id).one(),
                    session.query(DbColumnAttributes)
                    .filter(and_(DbColumnAttributes.column_id == id, DbColumnAttributes.id == attr_id))
                    .one())
    except sqlalchemy.exc.NoResultFound:
        logging.error(sqlalchemy.exc.NoResultFound, exc_info=True)


def update_column(
        branch: Branch,
        column: DbColumn,
        *,
        name: Optional[str] = None,
        datatype: Optional[str] = None,
        session: Session,
) -> Tuple[DbColumn, DbColumnAttributes, Commit]:
    """Update one or more attributes

    То есть создать новые атрибуты и указать начало и конец
    """
    logging.debug('update_column')
    column_and_attributes = get_column(branch, column.id, session=session)
    try:
        if branch.type != BranchTypes.WIP:
            raise ProhibitedActionInBranch("Column altering", branch.name)
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s.id
        new_commit.attribute_id_in = column_and_attributes[1].id
        new_column_attribute = DbColumnAttributes()
        new_column_attribute.type = AttributeTypes.COLUMN
        new_column_attribute.column_id = column_and_attributes[0].id
        new_column_attribute.datatype = datatype
        new_column_attribute.name = name
        session.add(new_column_attribute)
        session.flush()
        new_commit.attribute_id_out = new_column_attribute.id
        session.add(new_commit)
        session.flush()

        return column_and_attributes[0], new_column_attribute, new_commit
    except AttributeError:
        logging.error(AttributeError, exc_info=True)


def delete_column(branch: Branch, column: DbColumn, *, session: Session) -> Commit:
    """Delete column from table from branch

    То есть надо удалить у колонки атрибуты, сам объект колонки останется
    То есть создать коммит с пустым концом
    """
    logging.debug('delete_column')
    column_and_attributes = get_column(branch, column.id, session=session)
    try:
        if branch.type != BranchTypes.WIP:
            raise ProhibitedActionInBranch("Column deleting", branch.name)
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s.id
        new_commit.attribute_id_in = column_and_attributes[1].id
        new_commit.attribute_id_out = None
        session.add(new_commit)
        session.flush()
        return new_commit

    except AttributeError:
        logging.error(AttributeError, exc_info=True)
