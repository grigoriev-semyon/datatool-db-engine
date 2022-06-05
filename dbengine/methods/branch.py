import logging
from typing import Tuple

from sqlalchemy import and_
from sqlalchemy.orm import Session

from dbengine.exceptions import BranchError, IncorrectBranchType, BranchNotFoundError, BranchConflict
from dbengine.models import Branch, BranchTypes, Commit, DbAttributes, DbTableAttributes
from dbengine.methods.table import get_tables, get_table
from dbengine.models.entity import AttributeTypes, DbColumnAttributes, DbColumn
from dbengine.methods.column import get_columns, get_column
from dbengine.models.branch import CommitActionTypes

logger = logging.getLogger(__name__)


def create_main_branch(*, session: Session) -> Branch:
    """Создать ветку main если веток еще не существует.

    Тип ветки MAIN, название ветки Main
    """
    logger.debug("create_main_branch")
    s = session.query(Branch).count()
    if s > 0:
        raise BranchError("Main branch already exists")
    new_branch = Branch(name="MAIN BRANCH", type=BranchTypes.MAIN)
    session.add(new_branch)
    session.flush()
    new_commit = Commit(branch_id=new_branch.id)
    session.add(new_commit)
    session.flush()
    return new_branch


def create_branch(name, *, session: Session) -> Branch:
    """Создать новую ветку из головы main ветки.

    Тип ветки WIP, название ветки `name`
    """
    s = session.query(Branch).filter(Branch.name == BranchTypes.MAIN).one_or_none()
    if not s:
        raise BranchError("Main branch does not exists")
    last_commit = session.query(Commit).filter(Commit.branch_id == 1).order_by(Commit.id.desc()).first()
    new_branch = Branch(name=name, type=BranchTypes.WIP)
    session.add(new_branch)
    session.flush()
    new_commit = Commit(branch_id=new_branch.id, prev_commit_id=last_commit.id)
    session.add(new_commit)
    session.flush()
    logger.debug("create_branch")
    return new_branch


def request_merge_branch(branch: Branch, *, session: Session) -> Branch:
    """Поменять тип ветки на MR

    Только если сейчас WIP
    """
    if branch.type != BranchTypes.WIP:
        raise IncorrectBranchType("request merge", "main")
    branch.type = BranchTypes.MR
    session.query(Branch).filter(Branch.id == branch.id).update({"type": BranchTypes.MR})
    session.flush()
    logger.debug("request_merge_branch")
    return branch


def unrequest_merge_branch(branch: Branch, *, session: Session) -> Branch:
    """Поменять тип ветки на WIP

    Только если сейчас MR
    """
    if branch.type != BranchTypes.MR:
        raise IncorrectBranchType("Unreguest merge", branch.name)
    branch.type = BranchTypes.WIP
    session.query(Branch).filter(Branch.id == branch.id).update({"type": BranchTypes.WIP})
    session.flush()

    logger.debug("unrequest_merge_branch")
    return branch


def ok_branch(branch: Branch, *, session: Session) -> Branch:
    """Поменять тип ветки на MERGED

    Только если сейчас MR
    """
    if branch.type != BranchTypes.MR:
        raise IncorrectBranchType("Confirm merge", branch.name)
    branch.type = BranchTypes.MERGED
    session.query(Branch).filter(Branch.id == branch.id).update({"type": BranchTypes.MERGED})
    session.flush()
    s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id).all()
    for row in s:
        new_commit = Commit(
            dev_branch_id=branch.id,
            attribute_id_in=row.attribute_id_in,
            attribute_id_out=row.attribute_id_out,
            branch_id=1,
        )
        if row == s[0]:
            new_commit.prev_commit_id = row.prev_commit_id
        else:
            prev_commit = (
                session.query(Commit).filter(Commit.dev_branch_id == branch.id).order_by(Commit.id.desc()).first()
            )
            new_commit.prev_commit_id = prev_commit.id
        session.add(new_commit)
        session.flush()

    logger.debug("ok_branch")
    return branch


def get_branch(id: int, *, session: Session) -> Branch:
    """Return branch by id or name"""
    logger.debug("get_branch")
    result = session.query(Branch).get(id)
    if not result:
        raise BranchNotFoundError(id)
    return result


def get_all_tables_and_columns_in_branch(branch: Branch, session: Session):
    table_ids = get_tables(branch, session=session)
    tables = []
    columns = []
    for tablerow in table_ids:
        table = get_table(branch, tablerow, session=session)
        tables.append(table)
        column_ids = get_columns(branch, table[0], session=session)
        for columnrow in column_ids:
            column = get_column(branch, columnrow, session=session)
            columns.append(column)
    return tables, columns


def check_conflicts(branch: Branch, session: Session):
    tables, columns = get_all_tables_and_columns_in_branch(branch, session=session)
    for row in tables:
        table_id = row[0].id
        main_table = get_table(get_branch(1, session=session), table_id, session=session)
        if main_table[1].create_ts > branch.create_ts:
            raise BranchConflict(branch.id)
    for row in columns:
        column_id = row[0].id
        main_column = get_column(get_branch(1, session=session), column_id, session=session)
        if main_column[1].create_ts > branch.create_ts:
            raise BranchConflict(branch.id)
    return True


def get_type_of_commit_object(commit: Commit, session: Session):
    attr_in, attr_out = commit.attribute_id_in, commit.attribute_id_out
    if attr_in is not None:
        s = session.query(DbAttributes).filter(DbAttributes.id == attr_in).one_or_none()
        return s.type
    elif attr_out is not None:
        s = session.query(DbAttributes).filter(DbAttributes.id == attr_out).one_or_none()
        return s.type
    else:
        return None


def get_action_of_commit(commit: Commit):
    attr_in, attr_out = commit.attribute_id_in, commit.attribute_id_out
    if attr_in is not None and attr_out is not None:
        return CommitActionTypes.ALTER
    elif attr_in is not None and attr_out is None:
        return CommitActionTypes.DROP
    elif attr_in is None and attr_out is not None:
        return CommitActionTypes.CREATE


def get_names_table_in_commit(commit: Commit, session: Session):
    attr_in, attr_out = commit.attribute_id_in, commit.attribute_id_out
    name1 = None
    name2 = None
    if get_type_of_commit_object(commit, session=session) == AttributeTypes.TABLE:
        if attr_in is not None:
            name1 = session.query(DbTableAttributes).filter(DbTableAttributes.id == attr_in).one().name
        elif attr_out is not None:
            name2 = session.query(DbTableAttributes).filter(DbTableAttributes.id == attr_out).one().name
    return name1, name2


def get_names_column_in_commit(commit: Commit, session: Session):
    attr_in, attr_out = commit.attribute_id_in, commit.attribute_id_out
    tablename = None
    name1 = None
    name2 = None
    datatype1 = None
    datatype2 = None
    if get_type_of_commit_object(commit, session=session) == AttributeTypes.COLUMN:
        if attr_in is not None:
            s = session.query(DbColumnAttributes).filter(DbColumnAttributes.id == attr_in).one()
            name1 = s.name
            column_id = s.column_id
            datatype1 = s.datatype
            find_table_id = session.query(DbColumn).filter(DbColumn.id == column_id).one_or_none().table_id
            table_attrs = session.query(DbTableAttributes).filter(
                and_(DbTableAttributes.table_id == find_table_id)).order_by(
                DbTableAttributes.id.desc()).all()
            for row in table_attrs:
                if row.id < attr_in:
                    tablename = row.name
        if attr_out is not None:
            s = session.query(DbColumnAttributes).filter(DbColumnAttributes.id == attr_out).one()
            name2 = s.name
            datatype2 = s.datatype
            column_id = s.column_id
            find_table_id = session.query(DbColumn).filter(DbColumn.id == column_id).one_or_none().table_id
            table_attrs = session.query(DbTableAttributes).filter(
                and_(DbTableAttributes.table_id == find_table_id)).order_by(
                DbTableAttributes.id.desc()).all()
            for row in table_attrs:
                if row.id < attr_out:
                    tablename = row.name
    return tablename, name1, datatype1, name2, datatype2
