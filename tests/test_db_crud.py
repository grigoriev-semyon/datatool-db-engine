import logging
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dbengine import *
from dbengine.settings import Settings
from dbengine.exceptions import BranchError
from dbengine.models import Base, BranchTypes


settings = Settings()
logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def session():
    engine = create_engine(settings.DB_DSN)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(engine)
    session = Session()
    create_main_branch(session=session)
    session.commit()
    return Session()


def test_main_branch(session):
    branch = get_branch(1, session=session)
    assert False
    session.commit()
    assert False
    assert branch.name == "Main"
    assert branch.type == BranchTypes.MAIN
    with pytest.raises(BranchError):
        create_main_branch(session=session)


def test_branch(session):
    branch = create_branch("Test 1", session=session)
    assert branch.type == BranchTypes.WIP
    request_merge_branch(branch, session=session)
    assert branch.type == BranchTypes.MR
    ok_branch(branch, session=session)
    session.commit()
    assert branch.type == BranchTypes.MERGED
    assert branch.name == "Test 1"
