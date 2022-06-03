from abc import ABCMeta, abstractmethod
from typing import Tuple

from sqlalchemy.engine import Engine, create_engine
from pydantic import AnyUrl
from dbengine.settings import Settings


class IDbConnector(metaclass=ABCMeta):
    _settings: Settings = Settings()
    _connection: Engine = None

    @abstractmethod
    def _connect(self, url: AnyUrl):
        """Connect to DB and create self._connection Engine`"""

    @abstractmethod
    def generate_migration(self, attribute_in, attribute_out) -> Tuple[str, str]:
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
        if self._settings.DWH_CONNECTION_TEST.scheme == 'postgresql':
            self._connection = create_engine(self._settings.DWH_CONNECTION_TEST, echo=True)

    def generate_migration(self, attribute_in, attribute_out) -> Tuple[str, str]:
        pass

    def execute(self, sql: str):
        pass


class MySqlConnector(IDbConnector):
    _connection: Engine = super()._connection
    _settings: Settings = super()._settings

    def _connect(self, url: AnyUrl):
        pass

    def generate_migration(self, attribute_in, attribute_out) -> Tuple[str, str]:
        pass

    def execute(self, sql: str):
        pass