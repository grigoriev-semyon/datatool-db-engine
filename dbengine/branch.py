import logging
from typing import Union

import sqlalchemy.exc
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
    if s:
        raise BranchError("Main branch already exists")
    new_branch = Branch()
    new_branch.name = "MAIN BRANCH"
    new_branch.type = BranchTypes.MAIN
    session.add(new_branch)
    session.flush()
    new_commit = Commit()
    new_commit.branch_id = new_branch.id
    session.add(new_commit)
    session.flush()
    session.commit()
    logger.debug("init_main")
    return new_branch


def create_branch(name) -> Branch:
    """Создать новую ветку из головы main ветки.

    Тип ветки WIP, название ветки `name`
    """
    s = session.query(Branch).all()
    if not s:
        raise BranchError("Main branch does not exists")
    new_branch = Branch()
    new_branch.name = name
    new_branch.type = BranchTypes.WIP
    session.add(new_branch)
    session.flush()
    new_commit = Commit()
    new_commit.branch_id = new_branch.id
    session.add(new_commit)
    session.flush()
    session.commit()
    logger.debug("create_branch")
    return new_branch


def request_merge_branch(branch: Branch) -> Branch:
    """Поменять тип ветки на MR

    Только если сейчас WIP
    """
    if branch.type != BranchTypes.WIP:
        raise IncorrectBranchType("request merge", "main")
    branch.type = BranchTypes.MR
    session.query(Branch).filter(Branch.id == branch.id).update({"type": BranchTypes.MR})
    session.flush()
    session.commit()
    logger.debug("request_merge_branch")
    return branch


def unrequest_merge_branch(branch: Branch) -> Branch:
    """Поменять тип ветки на WIP

    Только если сейчас MR
    """
    if branch.type != BranchTypes.MR:
        raise IncorrectBranchType("Unreguest merge", branch.name)
    branch.type = BranchTypes.WIP
    session.query(Branch).filter(Branch.id == branch.id).update({"type": BranchTypes.WIP})
    session.flush()
    session.commit()
    logger.debug("unrequest_merge_branch")
    return branch


def ok_branch(branch: Branch) -> Branch:
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
        new_commit = Commit()
        new_commit.dev_branch_id = branch.id
        new_commit.attribute_id_in = row.attribute_id_in
        new_commit.attribute_id_out = row.attribute_id_out
        new_commit.branch_id = 1
        if row.prev_commit_id == 1:
            new_commit.prev_commit_id = 1
        else:
            prev_commit = session.query(Commit).filter(Commit.dev_branch_id == branch.id).order_by(Commit.id.desc()).first()
            new_commit.prev_commit_id = prev_commit.id
        session.add(new_commit)
        session.flush()
    session.commit()
    logger.debug("ok_branch")
    return branch


def get_branch(id: int) -> Branch:
    """Return branch by id or name"""
    logger.debug("get_branch")
    try:
        return session.query(Branch).filter(Branch.id == id).one()
    except sqlalchemy.exc.NoResultFound:
        logging.error(sqlalchemy.exc.NoResultFound, exc_info=True)
