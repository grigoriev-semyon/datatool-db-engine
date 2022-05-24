import logging
from typing import Union

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models import Branch, BranchTypes, Commit
from .settings import Settings
from .exceptions import ProhibitedActionInBranch, IncorrectBranchType, BranchError

from .models import session

logger = logging.getLogger(__name__)


def init_main() -> Branch:
    """Инициализировать ветку main если веток еще не существует.

    Тип ветки MAIN, название ветки Main
    """
    s = session.query(Branch).all()
    if not s:
        new_branch = Branch()
        new_branch.name = "MAIN BRANCH"
        new_branch.type = BranchTypes.MAIN
        session.add(new_branch)
        session.flush()
        session.commit()
        new_commit = Commit()
        new_commit.branch_id = new_branch.id
        session.add(new_commit)
        session.flush()
        session.commit()
    else:
        raise Exception
    logger.debug("init_main")
    return new_branch


def create_branch(name) -> Branch:
    """Создать новую ветку из головы main ветки.

    Тип ветки WIP, название ветки `name`
    """
    s = session.query(Branch).all()
    if s:
        new_branch = Branch()
        new_branch.name = name
        new_branch.type = BranchTypes.WIP
        session.add(new_branch)
        session.flush()
        session.commit()
        new_commit = Commit()
        new_commit.branch_id = new_branch.id
        session.add(new_commit)
        session.flush()
        session.commit()
    else:
        raise BranchError("Main branch does not exists")
    logger.debug("create_branch")
    return new_branch


def request_merge_branch(branch: Branch) -> Branch:
    """Поменять тип ветки на MR

    Только если сейчас WIP
    """
    if branch.type == BranchTypes.WIP:
        branch.type = BranchTypes.MERGED
        session.query(Branch).filter(Branch.id == branch.id).update({"type": BranchTypes.MERGED})
        session.flush()
        session.commit()
    else:
        raise IncorrectBranchType("merge", branch.name)
    logger.debug("request_merge_branch")
    return branch


def unrequest_merge_branch(branch: Branch) -> Branch:
    """Поменять тип ветки на WIP

    Только если сейчас MR
    """
    if branch.type == BranchTypes.MR:
        branch.type = BranchTypes.WIP
        session.query(Branch).filter(Branch.id == branch.id).update({"type": BranchTypes.WIP})
        session.flush()
        session.commit()
    else:
        raise IncorrectBranchType("Unreguest", branch.name)
    logger.debug("unrequest_merge_branch")
    return branch


def ok_branch(branch: Branch) -> Branch:
    """Поменять тип ветки на MERGED

    Только если сейчас MR
    """
    if branch.type == BranchTypes.MR:
        branch.type = BranchTypes.MERGED
        session.query(Branch).filter(Branch.id == branch.id).update({"type": BranchTypes.MERGED})
        session.flush()
        session.commit()
    else:
        raise IncorrectBranchType("Confirm", branch.name)
    logger.debug("ok_branch")
    return branch


def get_branch(id: int) -> Branch:
    """Return branch by id or name"""
    logger.debug("get_branch")
    return session.query(Branch).filter(Branch.id == id).one()


def create_main_branch():
    new_branch = Branch()
    new_branch.name = 'main'
    new_branch.type = BranchTypes.MAIN
    session.add(new_branch)
    session.flush()
    session.commit()
    new_commit = Commit()
    new_commit.branch_id = new_branch.id
    session.add(new_commit)
    session.flush()
    session.commit()
