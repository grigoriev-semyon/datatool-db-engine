import re

from sqlalchemy.ext.declarative import as_declarative, declared_attr, declarative_base
from sqlalchemy import create_engine, Integer, String, Column, DateTime, ForeignKey
from datetime import datetime
from settings import Settings
from sqlalchemy.orm import sessionmaker

settings = Settings()

engine = create_engine(settings.DB_DSN)
engine.connect()
session = sessionmaker(bind=engine)


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
    id = Column(Integer, ForeignKey("db_entity.id"), primary_key=True)
    __mapper_args__ = {"polymorphic_identity": "TABLE"}


class DbColumn(DbEntity):
    id = Column(Integer, ForeignKey("db_entity.id"), primary_key=True)
    table_id = Column(Integer, ForeignKey("db_table.id"))

    __mapper_args__ = {"polymorphic_identity": "COLUMN"}


class DbAttributes(Base):
    id = Column(Integer, primary_key=True)
    type = Column(String)
    create_ts = Column(DateTime, nullable=False, default=datetime.utcnow)

    __mapper_args__ = {"polymorphic_identity": type}


class DbTableAttributes(DbAttributes):
    id = Column(Integer, ForeignKey("db_attributes.id"), primary_key=True)
    table_id = Column(Integer, ForeignKey("db_table.id"))
    path = Column(String)

    __mapper_args__ = {"polymorphic_identity": "TABLE"}


class DbColumnAttributes(DbAttributes):
    id = Column(Integer, ForeignKey("db_attributes.id"), primary_key=True)
    column_id = Column(Integer, ForeignKey("db_column.id"))
    name = Column(String)
    datatype = Column(String)

    __mapper_args__ = {"polymorphic_identity": "COLUMN"}


class Branch(Base):
    id = Column(Integer, primary_key=True)
    type = Column(String, default="MAIN")
    name = Column(String)
    create_ts = Column(DateTime, default=datetime.utcnow(), nullable=False)
    __mapper_args__ = {"polymorphic_identity": type}


class Commit(Base):
    id = Column(Integer, primary_key=True)
    prev_commit_id = Column(Integer, default=None)
    dev_branch_id = Column(Integer, default=None)
    branch_id = Column(Integer, ForeignKey("branch.id"))
    attribute_id_in = Column(Integer)
    attribute_id_out = Column(Integer)
    create_ts = Column(DateTime, default=datetime.utcnow(), nullable=False)


def create_tables():
    Base.metadata.create_all(engine)


def delete_table(id: int, new_name: str):
    pass