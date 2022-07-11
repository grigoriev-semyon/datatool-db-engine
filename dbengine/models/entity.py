from datetime import datetime
from enum import Enum

from sqlalchemy import Column, DateTime
from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from .base import Base


class AttributeTypes(str, Enum):
    TABLE = "TABLE"
    COLUMN = "COLUMN"


class DbEntity(Base):
    id = Column(Integer, primary_key=True)
    type = Column(String)
    create_ts = Column(DateTime, nullable=False, default=datetime.utcnow)

    __mapper_args__ = {'polymorphic_identity': 'DbAttributes', 'polymorphic_on': type, 'with_polymorphic': '*'}


class DbTable(DbEntity):
    id = Column(Integer, ForeignKey("db_entity.id"), primary_key=True)

    __mapper_args__ = {"polymorphic_identity": AttributeTypes.TABLE, 'polymorphic_load': 'inline'}
    columns = relationship('DbColumn', back_populates='table', foreign_keys='DbColumn.table_id')

    def __repr__(self) -> str:
        return f'<Table id={self.id}>'


class DbColumn(DbEntity):
    id = Column(Integer, ForeignKey("db_entity.id"), primary_key=True)
    table_id = Column(Integer, ForeignKey("db_table.id"))

    __mapper_args__ = {"polymorphic_identity": AttributeTypes.COLUMN, 'polymorphic_load': 'inline'}
    table = relationship('DbTable', back_populates='columns', foreign_keys='DbColumn.table_id')

    def __repr__(self) -> str:
        return f'<Column id={self.id}>'


class DbAttributes(Base):
    id = Column(Integer, primary_key=True)
    type = Column(String)
    create_ts = Column(DateTime, nullable=False, default=datetime.utcnow)

    __mapper_args__ = {'polymorphic_identity': 'DbAttributes', 'polymorphic_on': type, 'with_polymorphic': '*'}


class DbTableAttributes(DbAttributes):
    id = Column(Integer, ForeignKey("db_attributes.id"), primary_key=True)
    table_id = Column(Integer, ForeignKey("db_table.id"))
    name = Column(String)

    __mapper_args__ = {"polymorphic_identity": AttributeTypes.TABLE, 'polymorphic_load': 'inline'}
    table: DbTable = relationship('DbTable', foreign_keys='DbTableAttributes.table_id')


class DbColumnAttributes(DbAttributes):
    id = Column(Integer, ForeignKey("db_attributes.id"), primary_key=True)
    column_id = Column(Integer, ForeignKey("db_column.id"))
    table_id = Column(Integer, ForeignKey("db_table.id"))
    name = Column(String, nullable=False)
    datatype = Column(String, nullable=False)

    __mapper_args__ = {"polymorphic_identity": AttributeTypes.COLUMN, 'polymorphic_load': 'inline'}
    column: DbColumn = relationship('DbColumn', foreign_keys='DbColumnAttributes.column_id')
    table: DbTable = relationship('DbTable', foreign_keys='DbColumnAttributes.table_id')
