from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session as SessionType

from dbengine.settings import Settings
from dbengine.db_connector.db_connector import PostgreConnector

__all__ = ["SessionType", "Session", "engine", "settings"]


settings = Settings()
engine = create_engine(settings.DB_DSN)
Session = sessionmaker(engine, autocommit=True, autoflush=False)
test_connector = PostgreConnector(settings.DWH_CONNECTION_TEST)
prod_connector = PostgreConnector(settings.DWH_CONNECTION_PROD)
test_connector.connect()
prod_connector.connect()
