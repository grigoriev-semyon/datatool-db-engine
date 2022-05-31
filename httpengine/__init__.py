from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session as SessionType

from dbengine.settings import Settings

settings = Settings()
engine = create_engine(settings.DB_DSN)
Session = sessionmaker(engine, autocommit=True, autoflush=False)
session = Session()
