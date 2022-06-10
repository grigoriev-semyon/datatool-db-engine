from dbengine.routes import app
from settings import Settings
from dbengine.db_connector.db_connector import PostgreConnector
from fastapi_sqlalchemy import db

__all__ = ["app"]
settings = Settings()
test_connector = None
prod_connector = None
if settings.DWH_CONNECTION_TEST.scheme == "postgres":
    test_connector = PostgreConnector(db.session)
elif settings.DWH_CONNECTION_PROD.scheme == "postgres":
    prod_connector = PostgreConnector(db.session)