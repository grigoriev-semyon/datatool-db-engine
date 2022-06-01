from typing import List
import pytest

from dbengine.methods import *
from dbengine.exceptions import BranchError
from dbengine.models import BranchTypes
from dbengine.models.branch import Commit

from . import Session


def test_main():
    session = Session()
    branch = get_branch(1, session=session)
    assert branch.name == "MAIN BRANCH"
    assert branch.type == BranchTypes.MAIN
    with pytest.raises(BranchError):
        create_main_branch(session=session)


def test_branch_ok():
    session = Session()
    branch = create_branch("Test 1", session=session)
    assert branch.type == BranchTypes.WIP
    create_table(branch, "Test Branch 1", session=session)

    request_merge_branch(branch, session=session)
    assert branch.type == BranchTypes.MR
    with pytest.raises(BranchError):
        create_table(branch, "Test Branch 1", session=session)

    ok_branch(branch, session=session)
    with pytest.raises(BranchError):
        create_table(branch, "Test Branch 1", session=session)

    assert branch.type == BranchTypes.MERGED
    assert branch.name == "Test 1"


def test_branch_mr():
    session = Session()
    branch = create_branch("Test 2", session=session)
    assert branch.type == BranchTypes.WIP
    create_table(branch, "Test Branch 2", session=session)

    request_merge_branch(branch, session=session)
    with pytest.raises(BranchError):
        create_table(branch, "Test Branch 2", session=session)
    assert branch.type == BranchTypes.MR

    unrequest_merge_branch(branch, session=session)
    assert branch.type == BranchTypes.WIP
    create_table(branch, "Test Branch 2", session=session)


def test_commit_iteration():
    session = Session()
    branch = create_branch("Test 3", session=session)
    _, _, commit1 = create_table(branch, "Test 3 Table 1", session=session)
    _, _, commit2 = create_table(branch, "Test 3 Table 2", session=session)
    session.flush()

    assert commit2.prev_commit == commit1
    assert commit2 in commit1.next_commit
    assert commit1.branch == commit2.branch == branch

    # First commit in branch is empty commit
    assert commit1.prev_commit.prev_commit.branch.type == BranchTypes.MAIN

    log: List[Commit] = list(branch.commits)
    # At least 1 commit from main, then empty commit and 2 commits from branch
    assert len(log) >= 4
    assert log[0] == commit2
    assert log[1] == commit1
    assert log[3].branch.type == BranchTypes.MAIN
