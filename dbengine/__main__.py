import uvicorn

from dbengine import app
from dbengine.db_connector.db_connector import PostgreConnector, Settings


if __name__ == '__main__':
    uvicorn.run(app)
    settings = Settings()
