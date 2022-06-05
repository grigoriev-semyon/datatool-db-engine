import uvicorn
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dbengine import app
from dbengine.db_connector.db_connector import PostgreConnector
from dbengine.methods.branch import get_branch
from fastapi_sqlalchemy import db
from dbengine.models.base import Base
from dbengine.models.branch import *
from dbengine.models.entity import *
from dbengine.settings import Settings
from dbengine.methods.branch import create_main_branch

if __name__ == '__main__':
    settings = Settings()
    engine = create_engine(settings.DB_DSN)
    Session = sessionmaker(engine, autocommit=True, autoflush=False)
    session =  Session()
    conn = PostgreConnector(session)
    conn.execute_test(get_branch(2, session=session))
