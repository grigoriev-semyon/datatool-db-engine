import enum

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, DateTime, ForeignKey, Numeric
from datetime import datetime
from settings import Settings

engine = create_engine()

Base = declarative_base()
settings = Settings()


def create_table(name: str) -> int:
    class DBEntity(Base):
        __tablename__ = name
        id = Column(Integer, primary_key=True)
        type = Table
        create_ts: datetime = datetime.now()

    class DBTable(DBEntity):
        entity_id = Column(Integer, ForeignKey('DBEntity.id'), primary_key=True)

    class DBAttributes(Base):
        id = Column(Integer)
        type = Table
        create_ts: datetime = datetime.now()

    class DBTableAttributes(DBTable, DBAttributes):
        attribute_id: Column(Integer)
        entity_id = Column(Integer, ForeignKey('DBEntity.id'), primary_key=True)
        path = name

    class DBColumn(DBTable, DBEntity):
        entity_id = Column(Integer, ForeignKey('DBEntity.id'), primary_key=True)
        table_id: Column(Integer)

    class DBColumnAttributes(DBColumn, DBAttributes):
        attribute_id = Column(Integer, ForeignKey('DBAttributes.if'))
        entity_id = Column(Integer, ForeignKey('DBEntity.id'), primary_key=True)
        name: Column(String)
        datatype: Column(String)

    Base.metadata.create_all(engine)
    return DBEntity.id
