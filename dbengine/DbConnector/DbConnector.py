import logging
from abc import ABCMeta, abstractmethod
from typing import Tuple

from sqlalchemy.engine import Engine, create_engine
from pydantic import AnyUrl
from sqlalchemy.orm import Session

from dbengine.methods import get_table, get_column
from dbengine.settings import Settings
from sqlalchemy.exc import SQLAlchemyError
from dbengine.models.branch import Branch, Commit, CommitActionTypes
from dbengine.models.entity import AttributeTypes
from dbengine.methods.branch import get_action_of_commit, get_type_of_commit_object, get_names_table_in_commit, \
    get_names_column_in_commit


class IDbConnector(metaclass=ABCMeta):
    _settings: Settings = Settings()
    _connection: Engine = None
    _session: Session

    def _connect(self):
        """Connect to DB and create self._connection Engine`"""
        try:
            self._connection = create_engine(self._settings.DWH_CONNECTION_TEST, echo=True)
            self._connection.connect()
        except SQLAlchemyError:
            logging.error(SQLAlchemyError, exc_info=True)

    @staticmethod
    @abstractmethod
    def _create_table(tablename: str):
        pass

    @staticmethod
    @abstractmethod
    def _create_column(tablename: str, columnname: str, columntype: str):
        pass

    @staticmethod
    @abstractmethod
    def _delete_column(tablename: str, columnname: str):
        pass

    @staticmethod
    @abstractmethod
    def _delete_table(tablename: str):
        pass

    @staticmethod
    @abstractmethod
    def _alter_table(tablename: str, new_tablename: str):
        pass

    @staticmethod
    @abstractmethod
    def _alter_column(tablename: str, columnname: str, new_name: str, datatype: str, new_datatype: str):
        pass

    @staticmethod
    @abstractmethod
    def _alter_datatype(tablename: str, columnname: str, new_type: str):
        pass

    def generate_migration(self, branch: Branch):
        """
        Generates SQL Code for migration any DataBase
        """
        code = []
        s = self._session.query(Commit).filter(Commit.branch_id == branch.id).all()
        for row in s:
            object_type = get_type_of_commit_object(row, session=self._session)
            action_type = get_action_of_commit(row)
            name1 = None
            name2 = None
            datatype1 = None
            datatype2 = None
            tablename = None
            if object_type == AttributeTypes.TABLE:
                name1, name2 = get_names_table_in_commit(row, session=self._session)
            elif object_type == AttributeTypes.COLUMN:
                tablename, name1, datatype1, name2, datatype2 = get_names_column_in_commit(row, session=self._session)
            if object_type == AttributeTypes.TABLE:
                if action_type == CommitActionTypes.CREATE and name1 is None and name2 is not None:
                    self._create_table(name2)
                elif action_type == CommitActionTypes.ALTER and name1 is not None and name2 is not None:
                    self._alter_table(name1, name2)
                elif action_type == CommitActionTypes.DROP and name1 is not None and name2 is None:
                    self._delete_table(name1)
            elif object_type == AttributeTypes.COLUMN:
                if action_type == CommitActionTypes.CREATE and name1 is None and datatype1 is None and name2 is not None and datatype2 is not None and tablename is not None:
                    self._create_column(tablename, name2, datatype2)
                if action_type == CommitActionTypes.ALTER and name1 is not None and datatype1 is not None and name2 is not None and datatype2 is not None and tablename is not None:
                    self._alter_column(tablename, name1, name2, datatype1, datatype2)
                if action_type == CommitActionTypes.DROP and name1 is not None and name2 is None and datatype1 is not None and datatype2 is None and tablename is not None:
                    self._delete_column(tablename, name1)

        return code

    @abstractmethod
    def execute(self, sql: str):
        """Execute Sql code"""


class PostgreConnector(IDbConnector):
    _connection: Engine = super()._connection
    _settings: Settings = super()._settings

    @staticmethod
    def _create_table(tablename: str):
        return f"CREATE TABLE {tablename};"

    @staticmethod
    def _create_column(tablename: str, columnname: str, columntype: str):
        return f'{"ALTER TABLE"} {tablename} ADD COLUMN {columnname} {columntype};'

    @staticmethod
    def _delete_column(tablename: str, columnname: str):
        return f"{'ALTER TABLE'} {tablename} DROP COLUMN {columnname};"

    @staticmethod
    def _delete_table(tablename: str):
        return f"{'DROP TABLE'} {tablename};"

    @staticmethod
    def _alter_table(tablename: str, new_tablename: str):
        return f"ALTER TABLE {tablename} RENAME TO {new_tablename};"

    @staticmethod
    def _alter_column(tablename: str, columnname: str, new_name: str, datatype: str, new_datatype: str):
        pass

    @staticmethod
    def _alter_datatype(tablename: str, columnname: str, new_type: str):
        pass

    def execute(self, sql: str):
        pass
