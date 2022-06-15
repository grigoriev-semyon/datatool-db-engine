import logging
from abc import ABCMeta, abstractmethod

from fastapi_sqlalchemy import db
from pydantic import AnyUrl
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import Connection

from dbengine.methods.branch import get_action_of_commit, get_type_of_commit_object, get_names_table_in_commit, \
    get_names_column_in_commit
from dbengine.models.branch import Branch, CommitActionTypes
from dbengine.models.entity import AttributeTypes
from dbengine.settings import Settings


class IDbConnector(metaclass=ABCMeta):
    __settings: Settings = Settings()
    __engine_db_dsn: Engine = None
    __engine: Engine = None
    __connection: Connection = None
    __connection_url: AnyUrl = None
    __session = db.session

    def connect(self):
        """Connect to DB and create self._connection Engine`"""
        try:
            self.__engine = create_engine(self.__connection_url, echo=True)
            self.__connection = self.__engine.connect()
        except SQLAlchemyError:
            logging.error(SQLAlchemyError, exc_info=True)

    def __init__(self, connection_url: AnyUrl):
        self.__connection_url = connection_url

    @staticmethod
    @abstractmethod
    def _create_table(tablename: str) -> str:
        """
        Generate query code to create table
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def _create_column(tablename: str, columnname: str, columntype: str) -> str:
        """
        Generate query code to create column
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def _delete_column(tablename: str, columnname: str) -> str:
        """
        Generate query code to delete column
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def _delete_table(tablename: str) -> str:
        """
        Generate query code to delete table
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def _alter_table(tablename: str, new_tablename: str) -> str:
        """
        Generate query code to alter table
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def _alter_column(tablename: str, columnname: str, new_name: str, datatype: str, new_datatype: str) -> str:
        """
        Generate query code to alter column
        """
        raise NotImplementedError

    def generate_migration(self, branch: Branch):
        """
        Generates SQL Code for migration any DataBase
        """
        s = branch.commits
        for row in s:
            object_type = get_type_of_commit_object(row, session=self.__session)
            action_type = get_action_of_commit(row)
            if object_type == AttributeTypes.TABLE:
                name1, name2 = get_names_table_in_commit(row, session=self.__session)
            elif object_type == AttributeTypes.COLUMN:
                tablename, name1, datatype1, name2, datatype2 = get_names_column_in_commit(row, session=self.__session)
            if object_type == AttributeTypes.TABLE:
                if action_type == CommitActionTypes.CREATE and name1 is None and name2 is not None:
                    row.sql_up = self._create_table(name2)
                    row.sql_down = self._delete_table(name2)
                    self.__session.flush()
                elif action_type == CommitActionTypes.ALTER and name1 is not None and name2 is not None:
                    row.sql_up = self._alter_table(name1, name2)
                    row.sql_down = self._alter_table(name2, name1)
                    self.__session.flush()
                elif action_type == CommitActionTypes.DROP and name1 is not None and name2 is None:
                    row.sql_up = self._delete_table(name1)
                    row.sql_down = self._create_table(name1)
                    self.__session.flush()
            elif object_type == AttributeTypes.COLUMN:
                if action_type == CommitActionTypes.CREATE and name1 is None and datatype1 is None and name2 is not None and datatype2 is not None and tablename is not None:
                    row.sql_up = self._create_column(tablename, name2, datatype2)
                    row.sql_down = self._delete_column(tablename, name2)
                    self.__session.flush()
                if action_type == CommitActionTypes.ALTER and name1 is not None and datatype1 is not None and name2 is not None and datatype2 is not None and tablename is not None:
                    row.sql_up = self._alter_column(tablename, name1, name2, datatype1, datatype2)
                    row.sql_down = self._alter_column(tablename, name2, name1, datatype2, datatype1)
                    self.__session.flush()
                if action_type == CommitActionTypes.DROP and name1 is not None and name2 is None and datatype1 is not None and datatype2 is None and tablename is not None:
                    row.sql_up = self._delete_column(tablename, name1)
                    row.sql_down = self._create_column(tablename, name1, datatype1)
                    self.__session.flush()

    def execute(self, str: str):
        """Execute Sql code"""
        self.__connection.execute(str)


class PostgreConnector(IDbConnector):
    @staticmethod
    def _create_table(tablename: str):
        return f"CREATE TABLE {tablename} ();"

    @staticmethod
    def _create_column(tablename: str, columnname: str, columntype: str):
        return f'ALTER TABLE {tablename} ADD COLUMN {columnname} {columntype};'

    @staticmethod
    def _delete_column(tablename: str, columnname: str):
        return f"ALTER TABLE {tablename} DROP COLUMN {columnname};"

    @staticmethod
    def _delete_table(tablename: str):
        return f"DROP TABLE {tablename};"

    @staticmethod
    def _alter_table(tablename: str, new_tablename: str):
        return f"ALTER TABLE {tablename} RENAME TO {new_tablename};"

    @staticmethod
    def _alter_column(tablename: str, columnname: str, new_name: str, datatype: str, new_datatype: str):
        tmp_columnname = f"tmp_{columnname}"
        n1 = " \n"
        first_query = f"ALTER TABLE {tablename} RENAME COLUMN {new_name} TO {tmp_columnname}".join(n1).lstrip()
        second_query = f"ALTER TABLE {tablename} ADD {new_name} AS ({tmp_columnname} as {new_datatype})".join(
            n1).lstrip()
        third_query = f"ALTER TABLE {tablename} DROP COLUMN {tmp_columnname};"
        return f"{first_query}{second_query}{third_query}"
