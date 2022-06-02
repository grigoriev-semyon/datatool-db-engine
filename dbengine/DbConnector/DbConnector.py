from abc import ABCMeta, abstractmethod
from typing import Tuple

from sqlalchemy.engine import Engine
from pydantic import AnyUrl


class IDbConnector(metaclass=ABCMeta):
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
