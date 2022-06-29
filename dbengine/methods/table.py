import logging
from typing import List, Optional, Tuple

import sqlalchemy.exc
from sqlalchemy import and_
from sqlalchemy.orm import Session

from dbengine.exceptions import ProhibitedActionInBranch, TableDeleted, TableDoesntExists
from dbengine.methods.converters import name_converter
from dbengine.models import AttributeTypes, Branch, BranchTypes, Commit, DbColumn, DbTable, DbTableAttributes
from .column import create_column, delete_column

logger = logging.getLogger(__name__)


def create_table(
    branch: Branch, name: str, columns_and_attr: Optional[List[Tuple[str, str]]] = None, *, session: Session
) -> Tuple[DbTable, DbTableAttributes, Commit]:
    """Create table in branch with optional columns"""
    logging.debug("create_table")
    if branch.type != BranchTypes.WIP:
        raise ProhibitedActionInBranch("Table creating", branch.name)
    name = name_converter(name)
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
    if columns_and_attr:
        for row in columns_and_attr:
            create_column(branch, new_table, name=row[0], datatype=row[1], session=session)
    return new_table, new_table_attribute, new_commit


def get_table(branch: Branch, id: int, start_from_commit: Optional[Commit] = None) -> Tuple[
    DbTable, DbTableAttributes]:
    """Return table and last attributes in branch by id"""
    commit = branch.last_commit
    attr_out: DbTableAttributes
    if start_from_commit and start_from_commit in branch.commits:
        commit = start_from_commit
    while True:
        attr_out, attr_in = commit.attribute_out, commit.attribute_in
        if attr_in is None and attr_out is None:
            if not commit.prev_commit:
                raise TableDoesntExists(id, branch.name)
            commit = commit.prev_commit
        elif attr_in is not None and attr_out is None:
            if attr_in and attr_in.type == AttributeTypes.TABLE:
                if attr_in.table_id == id:
                    raise TableDeleted(id, branch.name)
            else:
                if not commit.prev_commit:
                    raise TableDoesntExists(id, branch.name)
                commit = commit.prev_commit
        elif (attr_in is not None and attr_out is not None) or (attr_in is None and attr_out is not None):
            if attr_out and attr_out.type == AttributeTypes.TABLE:
                if attr_out.table_id == id:
                    return (
                        attr_out.table, attr_out
                    )
            commit = commit.prev_commit


def update_table(
    branch: Branch,
    table: DbTable,
    name: str,
    *,
    session: Session,
) -> Tuple[DbTable, DbTableAttributes, Commit]:
    """Change name of table and commit to branch"""
    logging.debug("update_table")
    table_and_last_attributes = get_table(branch, table.id)
    name = name_converter(name)
    if branch.type != BranchTypes.WIP:
        raise ProhibitedActionInBranch("Table altering", branch.name)
    s = (
        session.query(Commit).filter(
            and_(Commit.branch_id == branch.id, Commit.attribute_id_out == table_and_last_attributes[1].id)
        )
    ).one_or_none()
    s_main = (
        session.query(Commit)
        .filter(and_(Commit.branch_id == 1, Commit.attribute_id_out == table_and_last_attributes[1].id))
        .one_or_none()
    )
    new_commit = Commit()
    new_commit.branch_id = branch.id
    if s:
        new_commit.prev_commit_id = s.id
    elif (not s) and s_main:
        new_commit.prev_commit_id = s_main.id
    elif not (s and s_main):
        raise TableDoesntExists(table.id, branch.name)
    new_commit.attribute_id_in = table_and_last_attributes[1].id
    new_table_attribute = DbTableAttributes(
        type=AttributeTypes.TABLE, table_id=table_and_last_attributes[0].id, name=name
    )
    session.add(new_table_attribute)
    session.flush()
    new_commit.attribute_id_out = new_table_attribute.id
    session.add(new_commit)
    session.flush()
    return table_and_last_attributes[0], new_table_attribute, new_commit


def delete_table(
    branch: Branch,
    table: DbTable,
    *,
    session: Session,
) -> Commit:
    """Delete table in branch"""
    logging.debug("delete_table")
    table_and_last_attributes = get_table(branch, table.id)
    if branch.type != BranchTypes.WIP:
        raise ProhibitedActionInBranch("Deleting table", branch.name)
    columns = session.query(DbColumn).filter(DbColumn.table_id == table.id).all()
    for row in columns:
        delete_column(branch, row, session=session)
    new_commit = Commit(attribute_id_in=table_and_last_attributes[1].id, attribute_id_out=None, branch_id=branch.id)
    s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
    if s:
        new_commit.prev_commit_id = s.id
    session.add(new_commit)
    session.flush()
    return new_commit


def get_tables(branch: Branch) -> List[int]:
    """
    Get list of table id's in branch
    """
    try:
        ids = []
        deleted_ids = []
        commit_in_branch = branch.last_commit
        while True:
            if not commit_in_branch.prev_commit:
                return ids
            attr_in, attr_out = commit_in_branch.attribute_in, commit_in_branch.attribute_out
            if (attr_out is not None and attr_in is not None) or (attr_out is not None and attr_in is None):
                if attr_out.type == AttributeTypes.TABLE:
                    table_id = attr_out.table.id
                    if table_id not in ids and table_id not in deleted_ids:
                        ids.append(table_id)
            commit_in_branch = commit_in_branch.prev_commit
            if attr_out is None and attr_in is not None:
                if attr_in.type == AttributeTypes.TABLE:
                    deleted_ids.append(attr_in.table.id)
            commit_in_branch = commit_in_branch.prev_commit
    except sqlalchemy.exc.NoResultFound:
        logging.error(sqlalchemy.exc.NoResultFound, exc_info=True)
