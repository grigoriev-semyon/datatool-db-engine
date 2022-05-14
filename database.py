import enum

import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, DateTime, ForeignKey, Numeric
from datetime import datetime
from settings import Settings

##settings = Settings()

engine = create_engine("")
engine.connect()

Base = declarative_base()


def create_table(name: str) -> int:
    class DBEntity(Base):
        __tablename__ = name
        id = Column(Integer, primary_key=True, unique=True)
        type = Column(String)
        create_ts: Column(DateTime, nullable=False, default=datetime.utcnow())
        __mapper_args__ = {
            'polymorphic_identity': name
        }

    class DBTable(DBEntity):
        __tablename__ = f"table{name}"
        entity_id = sqlalchemy.orm.column_property(
            Column(Integer, ForeignKey(f'{name}.id'), primary_key=True, unique=True),
            DBEntity.id)
        __mapper_args__ = {
            'polymorphic_identity': __tablename__
        }

    class DBAttributes(DBEntity):
        __tablename__ = f"attributes{name}"
        id = Column(Integer)
        entity_id = sqlalchemy.orm.column_property(
            Column(Integer, ForeignKey(f'{name}.id'), primary_key=True, unique=True, nullable=False),
            DBEntity.id)
        type = Column(String)
        create_ts: Column(DateTime, nullable=False, default=datetime.utcnow())
        __mapper_args__ = {
            'polymorphic_identity': __tablename__
        }

    class DBTableAttributes(DBAttributes):
        __tablename__ = f"tableattributes{name}"
        attribute_id: Column(Integer, ForeignKey(f"attributes{name}.id"))
        entity_id = sqlalchemy.orm.column_property(
            Column(Integer, ForeignKey(f'attributes{name}.entity_id'), primary_key=True, unique=True),
            DBAttributes.entity_id)
        path = Column(String)
        __mapper_args__ = {
            'polymorphic_identity': __tablename__
        }

    class DBColumn(DBEntity):
        __tablename__ = f"column{name}"
        entity_id = sqlalchemy.orm.column_property(Column(Integer, ForeignKey(f'{name}.id'), primary_key=True, unique=True),
                                                   DBEntity.id)
        table_id: Column(Integer)
        __mapper_args__ = {
            'polymorphic_identity': __tablename__
        }

    class DBColumnAttributes(DBAttributes):
        __tablename__ = f"columnattributes{name}"
        attribute_id = sqlalchemy.orm.column_property(Column(Integer, ForeignKey(f"attributes{name}.id")),
                                                      DBAttributes.id)
        entity_id = sqlalchemy.orm.column_property(
            Column(Integer, ForeignKey(f'attributes{name}.entity_id'), primary_key=True, unique=True),
            DBAttributes.entity_id)
        name1: Column(String)
        datatype: Column(String)
        __mapper_args__ = {
            'polymorphic_identity': __tablename__,
            'inherit_condition': (entity_id == DBAttributes.entity_id)
        }

    Base.metadata.create_all(engine)
    return DBEntity.id


create_table('test')
