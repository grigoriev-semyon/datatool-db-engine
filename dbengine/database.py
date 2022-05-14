import re

from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy import create_engine, Integer, String, Column, DateTime, ForeignKey
from datetime import datetime
from dbengine.settings import Settings

settings = Settings()

engine = create_engine(settings.DB_DSN)
engine.connect()


@as_declarative()
class Base:
    """Base class for all database entities"""

    @declared_attr
    def __tablename__(cls) -> str:  # pylint: disable=no-self-argument
        """Generate database table name automatically.
        Convert CamelCase class name to snake_case db table name.
        """
        return re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__).lower()


class DbEntity(Base):
    id = Column(Integer, primary_key=True)
    type = Column(String)
    create_ts = Column(DateTime, nullable=False, default=datetime.utcnow)

    __mapper_args__ = {"polymorphic_identity": type}


class DbTable(DbEntity):
    entity_id = Column(Integer, ForeignKey("db_entity.id"), primary_key=True)
    __mapper_args__ = {"polymorphic_identity": "TABLE"}


class DbColumn(DbEntity):
    entity_id = Column(Integer, ForeignKey("db_entity.id"), primary_key=True)
    table_id = Column(Integer)

    __mapper_args__ = {"polymorphic_identity": "COLUMN"}


class DbAttributes(Base):
    id = Column(Integer, primary_key=True)
    type = Column(String)
    create_ts = Column(DateTime, nullable=False, default=datetime.utcnow)

    __mapper_args__ = {"polymorphic_identity": type}


class DbTableAttributes(DbAttributes):
    entity_id = Column(Integer, ForeignKey("db_attributes.id"), primary_key=True)
    path = Column(String)

    __mapper_args__ = {"polymorphic_identity": "TABLE"}


class DbColumnAttributes(DbAttributes):
    entity_id = Column(Integer, ForeignKey("db_attributes.id"), primary_key=True)
    name = Column(String)
    datatype = Column(String)

    __mapper_args__ = {"polymorphic_identity": "COLUMN"}


def create_tables():
    Base.metadata.create_all(engine)
