import logging
from typing import List, Optional, Tuple

import sqlalchemy.exc
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from dbengine.exceptions import ColumnDeleted, ColumnDoesntExists, ProhibitedActionInBranch
from dbengine.models import AttributeTypes, Branch, BranchTypes, Commit, DbColumn, DbColumnAttributes, DbTable


logger = logging.getLogger(__name__)


def create_column(
    branch: Branch, table: DbTable, *, name: str, datatype: str, session: Session
) -> Tuple[DbColumn, DbColumnAttributes, Commit]:
    """Create column in table in branch"""
    logging.debug('create_column')
    if branch.type != BranchTypes.WIP:
        raise ProhibitedActionInBranch("Column creating", branch.name)
    s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
    new_commit = Commit(branch_id=branch.id, attribute_id_in=None)
    if s:
        new_commit.prev_commit_id = s.id
    new_column = DbColumn(type=AttributeTypes.COLUMN, table_id=table.id)
    session.add(new_column)
    session.flush()
    new_column_attribute = DbColumnAttributes(
        type=AttributeTypes.COLUMN, column_id=new_column.id, datatype=datatype, name=name
    )
    session.add(new_column_attribute)
    session.flush()
    new_commit.attribute_id_out = new_column_attribute.id
    session.add(new_commit)
    session.flush()

    return new_column, new_column_attribute, new_commit


def get_column(branch: Branch, id: int, *, session: Session) -> Tuple[DbColumn, DbColumnAttributes]:
    """Get last version of column in table in branch"""
    logging.debug('get_column')
    try:
        commit_in_branch = (
            session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        )
        prev_commit = commit_in_branch.prev_commit_id
        while True:
            attr_out, attr_in = commit_in_branch.attribute_id_out, commit_in_branch.attribute_id_in
            if attr_in is None and attr_out is None:
                if not prev_commit:
                    raise ColumnDoesntExists(id, branch.name)
                commit_in_branch = (
                    session.query(Commit)
                    .filter(and_(Commit.id == prev_commit, or_(Commit.branch_id == 1, Commit.branch_id == branch.id)))
                    .one_or_none()
                )
                prev_commit = commit_in_branch.prev_commit_id
            elif attr_in is not None and attr_out is None:
                s = session.query(DbColumnAttributes).filter(DbColumnAttributes.id == attr_in).one_or_none()
                if s:
                    if s.column_id == id:
                        raise ColumnDeleted(id, branch.name)
                else:
                    if not prev_commit:
                        raise ColumnDoesntExists(id, branch.name)
                    commit_in_branch = (
                        session.query(Commit)
                        .filter(
                            and_(Commit.id == prev_commit, or_(Commit.branch_id == 1, Commit.branch_id == branch.id))
                        )
                        .one_or_none()
                    )
                    prev_commit = commit_in_branch.prev_commit_id
            elif (attr_in is not None and attr_out is not None) or (attr_in is None and attr_out is not None):
                s = session.query(DbColumnAttributes).filter(DbColumnAttributes.id == attr_out).one_or_none()
                if s:
                    if s.column_id == id:
                        return (
                            session.query(DbColumn).filter(DbColumn.id == id).one(),
                            session.query(DbColumnAttributes)
                            .filter(and_(DbColumnAttributes.column_id == id, DbColumnAttributes.id == attr_out))
                            .one(),
                        )

                commit_in_branch = (
                    session.query(Commit)
                    .filter(and_(Commit.id == prev_commit, or_(Commit.branch_id == 1, Commit.branch_id == branch.id)))
                    .one_or_none()
                )
                prev_commit = commit_in_branch.prev_commit_id
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
    if branch.type != BranchTypes.WIP:
        raise ProhibitedActionInBranch("Column altering", branch.name)
    s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
    new_commit = Commit(branch_id=branch.id, attribute_id_in=column_and_attributes[1].id)
    if s:
        new_commit.prev_commit_id = s.id
    new_column_attribute = DbColumnAttributes(
        type=AttributeTypes.COLUMN, column_id=column_and_attributes[0].id, datatype=datatype, name=name
    )
    session.add(new_column_attribute)
    session.flush()
    new_commit.attribute_id_out = new_column_attribute.id
    session.add(new_commit)
    session.flush()
    return column_and_attributes[0], new_column_attribute, new_commit


def delete_column(branch: Branch, column: DbColumn, *, session: Session) -> Commit:
    """Delete column from table from branch

    То есть надо удалить у колонки атрибуты, сам объект колонки останется
    То есть создать коммит с пустым концом
    """
    logging.debug('delete_column')
    column_and_attributes = get_column(branch, column.id, session=session)
    if branch.type != BranchTypes.WIP:
        raise ProhibitedActionInBranch("Column deleting", branch.name)
    s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
    new_commit = Commit(branch_id=branch.id, attribute_id_in=column_and_attributes[1].id, attribute_id_out=None)
    if s:
        new_commit.prev_commit_id = s.id
    session.add(new_commit)
    session.flush()
    return new_commit


def get_columns(branch: Branch, table: DbTable, session: Session) -> List[int]:
    """
    Get a list of id columns in a table in this branch
    """
    try:
        ids = []
        commit_in_branch = (
            session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        )
        prev_commit = commit_in_branch.prev_commit_id
        while True:
            if not prev_commit:
                return ids
            attr_in, attr_out = commit_in_branch.attribute_id_in, commit_in_branch.attribute_id_out
            if (attr_out is not None and attr_in is not None) or (attr_out is not None and attr_in is None):
                column = session.query(DbColumnAttributes).filter(and_(DbColumnAttributes.id == attr_out)).one_or_none()
                if column:
                    column_id = column.column_id
                    s = session.query(DbColumn).filter(DbColumn.id == column_id).one()
                    if (column_id not in ids) and (s.table_id == table.id):
                        ids.append(column_id)
            commit_in_branch = session.query(Commit).filter(Commit.id == prev_commit).one_or_none()
            prev_commit = commit_in_branch.prev_commit_id
    except sqlalchemy.exc.NoResultFound:
        logging.error(sqlalchemy.exc.NoResultFound, exc_info=True)
