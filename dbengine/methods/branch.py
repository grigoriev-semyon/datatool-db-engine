import logging

from sqlalchemy.orm import Session

from dbengine.exceptions import BranchError, IncorrectBranchType, BranchNotFoundError
from dbengine.models import Branch, BranchTypes, Commit

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
