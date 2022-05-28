from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session as SessionType

from dbengine.settings import Settings

__all__ = ["SessionType", "Session", "engine", "settings"]


settings = Settings()
engine = create_engine(settings.DB_DSN)
Session = sessionmaker(engine)
