import logging
from abc import ABCMeta, abstractmethod
from typing import Tuple

from sqlalchemy.engine import Engine, create_engine
from pydantic import AnyUrl
from sqlalchemy.orm import Session

from dbengine.settings import Settings
from sqlalchemy.exc import SQLAlchemyError
from dbengine.models.branch import Branch, Commit, CommitActionTypes
from dbengine.models.entity import AttributeTypes
from dbengine.methods.branch import get_action_of_commit, get_type_of_commit_object


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
    def _alter_table(tablename: str):
        pass

    @staticmethod
    @abstractmethod
    def _alter_column(tablename: str, columnname: str, new_name: str):
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
            if object_type == AttributeTypes.TABLE:
                if action_type == CommitActionTypes.CREATE:
                    pass
                elif action_type == CommitActionTypes.ALTER:
                    pass
                elif action_type == CommitActionTypes.DROP:
                    pass
            elif object_type == AttributeTypes.COLUMN:
                if action_type == CommitActionTypes.CREATE:
                    pass
                if action_type == CommitActionTypes.ALTER:
                    pass
                if action_type == CommitActionTypes.DROP:
                    pass

        return code

    @abstractmethod
    def execute(self, sql: str):
        """Execute Sql code"""


class PostgreConnector(IDbConnector):
    _connection: Engine = super()._connection
    _settings: Settings = super()._settings

    def generate_migration(self, branch: Branch) -> Tuple[str, str]:
        pass

    def execute(self, sql: str):
        pass


class MySqlConnector(IDbConnector):
    _connection: Engine = super()._connection
    _settings: Settings = super()._settings

    def generate_migration(self, branch: Branch) -> Tuple[str, str]:
        pass

    def execute(self, sql: str):
        pass
