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
    name = Column(String)

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
    create_ts = Column(DateTime, default=datetime.utcnow, nullable=False)


class Commit(Base):
    id = Column(Integer, primary_key=True)
    prev_commit_id = Column(Integer, ForeignKey("commit.id"), default=None)
    dev_branch_id = Column(Integer, ForeignKey("branch.id"), default=None)
    branch_id = Column(Integer)
    attribute_id_in = Column(Integer)
    attribute_id_out = Column(Integer)
    create_ts = Column(DateTime, default=datetime.utcnow, nullable=False)


def create_tables():
    Base.metadata.create_all(engine)


def delete_table(id: int):
    commit = Commit()
    commit.attribute_id_in = id
    commit.attribute_id_out = None
    session().add(commit)
    session().commit()


def alter_table(id_func: int, new_name: str):
    session().query(DbColumnAttributes).filter(id == id_func).update({"name": new_name}, synchronize_session="fetch")
    session().commit()


def get_column(id: int):
    s = session().query(DbColumnAttributes).filter(id == id)
    return s.commit().all()


def get_table(id: int):
    s = session().query(DbTableAttributes).filter(id == id)
    result = s.commit().all()
    return {"name": result["name"], "columns": dict(**result, **get_column(id))}


def alter_column(id: int, new_name: str, new_datatype: str):
    s = session().query(DbColumnAttributes).filter(id == id).update({"name": new_name, "datatype": new_datatype})
    s.commit()


def get_column_id(table_id: int, name: str):
    if table_id:
        s = session().query(DbTableAttributes).filter(table_id == table_id)
    elif name:
        s = session().query(DbTableAttributes).filter(name == name)
    return s.commit().all()["id"]


def delete_column(id):
    pass


