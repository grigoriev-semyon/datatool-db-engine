import logging
from typing import Tuple

from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from dbengine.exceptions import BranchError, IncorrectBranchType, BranchNotFoundError, MergeError
from dbengine.methods.table import get_table
from dbengine.models import Branch, BranchTypes, Commit, DbAttributes, DbTableAttributes
from dbengine.models.branch import CommitActionTypes
from dbengine.models.entity import AttributeTypes, DbColumnAttributes, DbColumn

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
    if branch.type != BranchTypes.WIP and not check_conflicts(branch, session=session):
        raise IncorrectBranchType("request merge", "main")
    commits = []
    done_commits = []
    test_connector.generate_migration(branch)
    for row in branch.commits:
        if row.sql_up is not None and row.sql_down is not None:
            commits.append(row)
    for row in commits.__reversed__():
        try:
            test_connector.execute(row.sql_up)
            done_commits.append(row)
        except DBAPIError:
            for s in done_commits.__reversed__():
                test_connector.execute(s.sql_down)
            raise MergeError(branch.id)
    for row in commits:
        try:
            test_connector.execute(row.sql_down)
        except DBAPIError:
            raise MergeError
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
    if branch.type != BranchTypes.MR and not check_conflicts(branch, session=session):
        raise IncorrectBranchType("Confirm merge", branch.name)
    commits = []
    done_commits = []
    prod_connector.generate_migration(branch)
    for row in branch.commits:
        if row.sql_up is not None and row.sql_down is not None:
            commits.append(row)
    for row in commits.__reversed__():
        try:
            prod_connector.execute(row.sql_up)
            done_commits.append(row)
        except DBAPIError:
            for s in done_commits.__reversed__():
                prod_connector.execute(s.sql_down)
            raise MergeError(branch.id)
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


def check_conflicts(branch: Branch, session: Session):
    attrs = []
    branch_begin_id = None
    for row in branch.commits:
        if row.attribute_id_in:
            attrs.append(row.attribute_id_in)
        if row.attribute_id_out:
            attrs.append(row.attribute_id_out)
        if row.attribute_id_in is None and row.attribute_id_out is None:
            branch_begin_id = row.prev_commit
    main_branch = get_branch(1, session=session)
    unique_attrs = set(attrs)
    for row in main_branch.commits:
        if row.attribute_id_in in unique_attrs or row.attribute_id_out in unique_attrs:
            raise MergeError(branch.id)
        if row.id == branch_begin_id:
            break
    return True


def get_type_of_commit_object(commit: Commit, session: Session) -> str:
    anyattr = commit.attribute_id_in or commit.attribute_id_out
    s = session.query(DbAttributes).filter(DbAttributes.id == anyattr).one_or_none()
    if not s:
        return None
    return s.type


def get_action_of_commit(commit: Commit) -> str:
    attr_in, attr_out = commit.attribute_id_in, commit.attribute_id_out
    if attr_in is not None and attr_out is not None:
        return CommitActionTypes.ALTER
    elif attr_in is not None and attr_out is None:
        return CommitActionTypes.DROP
    elif attr_in is None and attr_out is not None:
        return CommitActionTypes.CREATE


def get_names_table_in_commit(commit: Commit, session: Session) -> Tuple:
    attr_in, attr_out = commit.attribute_id_in, commit.attribute_id_out
    name1 = None
    name2 = None
    if get_type_of_commit_object(commit, session=session) == AttributeTypes.TABLE:
        if attr_in is not None:
            name1 = session.query(DbTableAttributes).filter(DbTableAttributes.id == attr_in).one().name
        if attr_out is not None:
            name2 = session.query(DbTableAttributes).filter(DbTableAttributes.id == attr_out).one().name
    return name1, name2


def get_names_column_in_commit(commit: Commit, session: Session) -> Tuple:
    attr_in, attr_out = commit.attribute_id_in, commit.attribute_id_out
    tablename, name1, name2, datatype1, datatype2 = None, None, None, None, None
    if get_type_of_commit_object(commit, session=session) == AttributeTypes.COLUMN:
        branch = get_branch(commit.branch_id, session=session)
        if attr_in is not None:
            s = session.query(DbColumnAttributes).filter(DbColumnAttributes.id == attr_in).one()
            name1, datatype1 = s.name, s.datatype
            find_table_id = session.query(DbColumn).filter(DbColumn.id == s.column_id).one().table_id
            tablename = get_table(branch, find_table_id, commit, session=session)[1].name
        if attr_out is not None:
            s = session.query(DbColumnAttributes).filter(DbColumnAttributes.id == attr_out).one()
            name2, datatype2 = s.name, s.datatype
            if not tablename:
                find_table_id = session.query(DbColumn).filter(DbColumn.id == s.column_id).one().table_id
                tablename = get_table(branch, find_table_id, commit, session=session)[1].name
    return tablename, name1, datatype1, name2, datatype2
