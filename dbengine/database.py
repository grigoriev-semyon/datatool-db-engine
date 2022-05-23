import re

from sqlalchemy.ext.declarative import as_declarative, declared_attr, declarative_base
from sqlalchemy import create_engine, Integer, String, Column, DateTime, ForeignKey
from datetime import datetime
from settings import Settings
from sqlalchemy.orm import sessionmaker
from exceptions import ProhibitedActionInBranch

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
    attribute_id_in = Column(Integer, ForeignKey("db_attributes.id"))
    attribute_id_out = Column(Integer, ForeignKey("db_attributes.id"))
    create_ts = Column(DateTime, default=datetime.utcnow, nullable=False)


def create_tables():
    Base.metadata.create_all(engine)


def delete_table(branch: Branch, attribute_id: int):
    if branch.id != 1:
        s = session.query(Commit).filter(Commit.branch_id == attribute_id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.attribute_id_in = attribute_id
        if s:
            new_commit.prev_commit_id = s
        new_commit.attribute_id_out = None
        session.add(new_commit)
        session.flush()
        session.commit()
    else:
        raise ProhibitedActionInBranch("Deleting table", branch.name)


def alter_table(id: int, new_name: str):
    session.query(DbColumnAttributes).filter(DbColumnAttributes.id == id).update({"name": new_name},
                                                                                 synchronize_session="fetch")
    session.commit()


def get_column(id: int):
    s = session.query(DbColumnAttributes).filter(DbColumnAttributes.id == id)
    return s


def get_table(id: int):
    s = session.query(DbTableAttributes).filter(DbTableAttributes.id == id).one()
    return {"name": s.name, "columns": dict(**s, **get_column(id))}


def alter_column(id: int, new_name: str, new_datatype: str):
    s = session.query(DbColumnAttributes).filter(DbColumnAttributes.id == id).update(
        {"name": new_name, "datatype": new_datatype})
    s.commit()


def get_column_id(table_id: int, name: str):
    if table_id:
        s = session.query(DbTableAttributes).filter(DbTableAttributes.table_id == table_id)
    elif name:
        s = session.query(DbTableAttributes).filter(DbTableAttributes.name == name)
    return s.id


def create_table(name: str):
    new_table = DbTable()
    new_table.type = "TABLE"
    session.add(new_table)
    session.flush()
    session.commit()
    new_table_attribute = DbTableAttributes()
    new_table_attribute.type = "TABLE"
    new_table_attribute.id = new_table.id
    new_table_attribute.name = name
    session.add(new_table_attribute)
    session.flush()
    session.commit()
    return new_table.id


def create_column(name: str, datatype: str):
    new_column = DbColumn()
    new_column.type = "COLUMN"
    new_column.table_id = new_column.id
    session.add(new_column)
    session.flush()
    session.commit()
    new_column_attribute = DbColumnAttributes()
    new_column_attribute.type = "COLUMN"
    new_column_attribute.id = new_column.id
    new_column_attribute.datatype = datatype
    new_column_attribute.name = name
    session.add(new_column_attribute)
    session.flush()
    session.commit()
    return new_column.id


def create_new_branch(name: str):
    s = session.query(Branch).all()
    if not s:
        new_branch = Branch()
        new_branch.name = name
        session.add(new_branch)
        session.flush()
        session.commit()
        new_commit = Commit()
        new_commit.branch_id = new_branch.id
        session.add(new_commit)
        session.flush()
        session.commit()
    else:
        new_branch = Branch()
        new_branch.name = name
        new_branch.type = "IN_WORK"
        session.add(new_branch)
        session.flush()
        session.commit()
        new_commit = Commit()
        new_commit.branch_id = new_branch.id
        session.add(new_commit)
        session.flush()
        session.commit()
    return new_branch.id


def ok_branch_creator_column(branch: Branch, name: str):
    if branch.id != 1:
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s
        new_commit.attribute_id_in = None
        new_attrubute_id = create_column(name=name, datatype="COLUMN")
        new_commit.attribute_id_out = new_attrubute_id
        session.add(new_commit)
        session.flush()
        session.commit()
        return new_commit.id
    else:
        raise ProhibitedActionInBranch("Column creating", branch.name)


def ok_branch_changer_column(branch: Branch, name: str, attribute_id: int):
    if branch.id != 1:
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s
        new_commit.attribute_id_in = attribute_id
        new_column = create_column(name, "COLUMN")
        new_commit.attribute_id_out = new_column
        session.add(new_commit)
        session.flush()
        session.commit()
        return new_column
    else:
        raise ProhibitedActionInBranch("Column altering", branch.name)


def ok_branch_deleter_column(branch: Branch, attribute_id: int):
    if branch.id != 1:
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s
        new_commit.attribute_id_in = attribute_id
        new_commit.attribute_id_out = None
        session.add(new_commit)
        session.flush()
        session.commit()
        return True
    else:
        raise ProhibitedActionInBranch("Column deleting", branch.name)


def get_branch(id: int):
    return session.query(Branch).filter(Branch.id == id).one()


def get_branch_id(name: str):
    return session.query(Branch).filter(Branch.name == name).one().id


def ok_branch_creator_table(branch: Branch, name: str):
    if branch.id != 1:
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s
        new_commit.attribute_id_in = None
        new_table_id = create_table(name)
        new_commit.attribute_id_out = new_table_id
        session.add(new_commit)
        session.flush()
        session.commit()
        return new_commit.id
    else:
        raise ProhibitedActionInBranch("Table creating", branch.name)


def ok_branch_alter_table(branch: Branch, name: str, attribute_id: int):
    if branch.id != 1:
        s = session.query(Commit).filter(Commit.branch_id == branch.id).order_by(Commit.id.desc()).first()
        new_commit = Commit()
        new_commit.branch_id = branch.id
        if s:
            new_commit.prev_commit_id = s
        new_commit.attribute_id_in = attribute_id
        new_table = create_table(name)
        new_commit.attribute_id_out = new_table
        session.add(new_commit)
        session.flush()
        session.commit()
        return new_table
    else:
        raise ProhibitedActionInBranch("Table altering", branch.name)


def merge(branch: Branch, new_type: str):
    s = session.query(Branch).filter(Branch.id == branch.id).update(
        {"type": new_type})
    s.commit()
