import pytest

from dbengine import *
from dbengine.exceptions import BranchError
from dbengine.models import BranchTypes

from . import Session


def test_main():
    session = Session()
    branch = get_branch(1, session=session)
    session.commit()
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
    session.commit()

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

    session.commit()
