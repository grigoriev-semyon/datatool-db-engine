import logging
from abc import ABCMeta, abstractmethod
from typing import Tuple

from sqlalchemy.engine import Engine, create_engine
from pydantic import AnyUrl
from dbengine.settings import Settings
from sqlalchemy.exc import SQLAlchemyError
from dbengine.models.branch import Branch


class IDbConnector(metaclass=ABCMeta):
    _settings: Settings = Settings()
    _connection: Engine = None

    @abstractmethod
    def _connect(self, url: AnyUrl):
        """Connect to DB and create self._connection Engine`"""
        try:
            self._connection = create_engine(self._settings.DWH_CONNECTION_TEST, echo = True)
            self._connection.connect()
        except SQLAlchemyError:
            logging.error(SQLAlchemyError, exc_info=True)

    @abstractmethod
    def generate_migration(self, branch: Branch) -> Tuple[str, str]:
        """
        Generates SQL Code for migration any DataBase
        """

    @abstractmethod
    def execute(self, sql: str):
        """Execute Sql code"""


class PostgreConnector(IDbConnector):
    _connection: Engine = super()._connection
    _settings: Settings = super()._settings

    def _connect(self, url: AnyUrl):
        super()._connect(url)

    def generate_migration(self, branch: Branch) -> Tuple[str, str]:
        pass

    def execute(self, sql: str):
        pass


class MySqlConnector(IDbConnector):
    _connection: Engine = super()._connection
    _settings: Settings = super()._settings

    def _connect(self, url: AnyUrl):
        super()._connect(url)

    def generate_migration(self, branch: Branch) -> Tuple[str, str]:
        pass

    def execute(self, sql: str):
        pass