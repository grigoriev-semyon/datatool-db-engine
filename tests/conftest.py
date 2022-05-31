import pytest

from dbengine import create_main_branch
from dbengine.exceptions import BranchError
from dbengine.models import Base

from . import engine, Session, SessionType


@pytest.fixture(scope="module", autouse=True)
def ddl():
    Base.metadata.create_all(engine)
    session: SessionType = Session()
    try:
        create_main_branch(session=session)
    except BranchError:
        pass
    finally:
        session.close()
