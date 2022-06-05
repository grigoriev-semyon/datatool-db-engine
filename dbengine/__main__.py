import uvicorn
from dbengine import app
from dbengine.db_connector.db_connector import PostgreConnector
from dbengine.methods.branch import get_branch
from fastapi_sqlalchemy import db

if __name__ == '__main__':
    con = PostgreConnector()
    con.connect()
    session = con.get_session()

    con.execute(get_branch(1, session=session))
