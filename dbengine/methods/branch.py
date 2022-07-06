import logging
from typing import Tuple, Optional, List

from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from dbengine.exceptions import BranchError, IncorrectBranchType, BranchNotFoundError, MergeError
from dbengine.methods.column import get_columns, get_column
from dbengine.methods.table import get_table, get_tables
from dbengine.models import Branch, BranchTypes, Commit
from dbengine.models.branch import CommitActionTypes
from dbengine.models.entity import AttributeTypes

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


def request_merge_branch(branch: Branch, *, session: Session, test_connector) -> Branch:
    """Поменять тип ветки на MR

    Только если сейчас WIP
    """
    if branch.type != BranchTypes.WIP:
        raise IncorrectBranchType("request merge", "main")
    check_conflicts(branch, session=session)
    commits, lines_up, lines_down, rollback = [], [], [], []
    test_connector.generate_migration(branch)
    session.flush()
    for row in branch.commits:
        if row.sql_up is not None and row.sql_down is not None:
            commits.append(row)
        if row.prev_commit.branch_id == 1:
            break
    for row in commits:
        for line in reversed(row.sql_up.splitlines()):
            lines_up.append(line)
        for line in (row.sql_down.splitlines()):
            lines_down.append(line)
    i = 0
    lines_down.reverse()
    for row in lines_up.__reversed__():
        try:
            test_connector.execute(row)
            rollback.append(lines_down[i])
            i += 1
        except DBAPIError:
            for s in rollback.__reversed__():
                test_connector.execute(s)
            raise MergeError(branch.id)
    for row in rollback.__reversed__():
        try:
            test_connector.execute(row)
        except DBAPIError:
            raise MergeError
    branch.type = BranchTypes.MR
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
    session.flush()

    logger.debug("unrequest_merge_branch")
    return branch


def ok_branch(branch: Branch, *, session: Session, test_connector, prod_connector) -> Branch:
    """Поменять тип ветки на MERGED

    Только если сейчас MR
    """
    if branch.type != BranchTypes.MR:
        raise IncorrectBranchType("Confirm merge", branch.name)
    check_conflicts(branch, session=session)
    commits, lines_up, lines_down, rollback = [], [], [], []
    prod_connector.generate_migration(branch)
    session.flush()
    for row in branch.commits:
        if row.sql_up is not None and row.sql_down is not None:
            commits.append(row)
        if row.prev_commit.branch_id == 1:
            break
    for row in commits:
        for line in reversed(row.sql_up.splitlines()):
            lines_up.append(line)
        for line in (row.sql_down.splitlines()):
            lines_down.append(line)
    i = 0
    lines_down.reverse()
    for row in lines_up.__reversed__():
        try:
            test_connector.execute(row)
            rollback.append(lines_down[i])
            i += 1
        except DBAPIError:
            for s in rollback.__reversed__():
                test_connector.execute(s)
            raise MergeError(branch.id)
    rollback = []
    i = 0
    for row in lines_up.__reversed__():
        try:
            prod_connector.execute(row)
            rollback.append(lines_down[i])
        except DBAPIError:
            for s in rollback.__reversed__():
                prod_connector.execute(s)
            raise MergeError(branch.id)
    branch.type = BranchTypes.MERGED
    session.flush()
    commits_list = []
    for row in branch.commits:
        commits_list.append(row)
        if row.prev_commit.branch.id == 1:
            break
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
    """Return branch by id"""
    logger.debug("get_branch")
    result = session.query(Branch).filter(Branch.id == id).one_or_none()
    if not result:
        raise BranchNotFoundError(id)
    return result


def check_conflicts(branch: Branch, session: Session):
    """
    Checking conflicts with main branch
    """
    attrs = []
    branch_begin_id = None
    for row in branch.commits:
        if row.attribute_id_in:
            attrs.append(row.attribute_id_in)
        if row.attribute_id_out:
            attrs.append(row.attribute_id_out)
        if row.prev_commit.branch_id == 1:
            branch_begin_id = row.prev_commit
            break
    main_branch = get_branch(1, session=session)
    unique_attrs = set(attrs)
    main_commits = []
    for row in main_branch.commits:
        if row == branch_begin_id:
            break
        if row != branch_begin_id:
            main_commits.append(row)
        if row.prev_commit.id == branch_begin_id:
            break
    if not main_commits:
        return True
    for row in main_commits:
        if row.attribute_id_in in unique_attrs or row.attribute_id_out in unique_attrs:
            raise MergeError(branch.id)
    return True


def get_type_of_commit_object(commit: Commit) -> Optional[str]:
    """
    Get type of changing object: Table or Column
    """
    anyattr = commit.attribute_in or commit.attribute_out
    if not anyattr:
        return None
    return anyattr.type


def get_action_of_commit(commit: Commit) -> str:
    """
    Get action if commit: Alter, Drop or Create
    """
    attr_in, attr_out = commit.attribute_id_in, commit.attribute_id_out
    if attr_in is not None and attr_out is not None:
        return CommitActionTypes.ALTER
    elif attr_in is not None and attr_out is None:
        return CommitActionTypes.DROP
    elif attr_in is None and attr_out is not None:
        return CommitActionTypes.CREATE


def get_names_table_in_commit(commit: Commit) -> Tuple:
    """
    Get old and new tablename in commit
    """
    attr_in, attr_out = commit.attribute_in, commit.attribute_out
    name1, name2 = None, None
    if get_type_of_commit_object(commit) == AttributeTypes.TABLE:
        if attr_in is not None:
            name1 = attr_in.name
        if attr_out is not None:
            name2 = attr_out.name
    return name1, name2


def get_names_column_in_commit(commit: Commit) -> Tuple:
    """
    Get old and new columnname in commit
    """
    attr_in, attr_out = commit.attribute_in, commit.attribute_out
    tablename, name1, name2, datatype1, datatype2 = None, None, None, None, None
    if get_type_of_commit_object(commit) == AttributeTypes.COLUMN:
        if attr_in is not None:
            name1, datatype1 = attr_in.name, attr_in.datatype
            tablename = get_table(commit.branch, attr_in.column.table_id, commit)[1].name
        if attr_out is not None:
            name2, datatype2 = attr_out.name, attr_out.datatype
            if not tablename:
                tablename = get_table(commit.branch, attr_out.column.table_id, commit)[1].name
    return tablename, name1, datatype1, name2, datatype2


def get_all_tables_and_columns_in_branch(branch: Branch, session: Session) -> List:
    """
    Get all changed/created objects in branch
    """
    table_ids = get_tables(branch)
    tables = []
    columns = []
    result = []
    for tablerow in table_ids:
        table = get_table(branch, tablerow)
        tables.append(table)
        column_ids = get_columns(branch, table[0], session=session)
        for columnrow in column_ids:
            column = get_column(branch, columnrow)
            columns.append(column)
        result.append((table, tuple(columns)))
        columns = []
    return result
