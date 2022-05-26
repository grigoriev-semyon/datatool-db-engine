import re
from datetime import datetime
from enum import Enum

from sqlalchemy import Column, DateTime
from sqlalchemy import Enum as EnumDb
from sqlalchemy import ForeignKey, Integer, String, create_engine
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.orm import sessionmaker

from .settings import Settings


settings = Settings()

engine = create_engine(settings.DB_DSN, echo=True)
engine.connect()
Session = sessionmaker(bind=engine)
session = Session()


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
    name = Column(String)

    __mapper_args__ = {"polymorphic_identity": "TABLE"}


class DbColumnAttributes(DbAttributes):
    id = Column(Integer, ForeignKey("db_attributes.id"), primary_key=True)
    column_id = Column(Integer, ForeignKey("db_column.id"))
    name = Column(String)
    datatype = Column(String)

    __mapper_args__ = {"polymorphic_identity": "COLUMN"}


class BranchTypes(str, Enum):
    MAIN = "MAIN"
    WIP = "WORK IN PROGRESS"
    MR = "MERGE REQUEST"
    MERGED = "MERGED"


class AttributeTypes(str, Enum):
    TABLE = "TABLE"
    COLUMN = "COLUMN"


class Branch(Base):
    id = Column(Integer, primary_key=True)
    type = Column(EnumDb(BranchTypes, native_enum=False), default=BranchTypes.WIP)
    name = Column(String)
    create_ts = Column(DateTime, default=datetime.utcnow, nullable=False)


class Commit(Base):
    id = Column(Integer, primary_key=True)
    prev_commit_id = Column(Integer, ForeignKey("commit.id"), default=1)
    dev_branch_id = Column(Integer, ForeignKey("branch.id"), default=None)
    branch_id = Column(Integer)
    attribute_id_in = Column(Integer, ForeignKey("db_attributes.id"))
    attribute_id_out = Column(Integer, ForeignKey("db_attributes.id"))
    create_ts = Column(DateTime, default=datetime.utcnow, nullable=False)


def create_tables():
    Base.metadata.create_all(engine)
