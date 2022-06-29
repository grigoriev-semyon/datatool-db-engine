import logging
from abc import ABCMeta, abstractmethod

from pydantic import AnyUrl
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import Connection

from dbengine.methods.branch import get_action_of_commit, get_type_of_commit_object, get_names_table_in_commit, \
    get_names_column_in_commit
from dbengine.models.branch import Branch, CommitActionTypes
from dbengine.models.entity import AttributeTypes
from dbengine.settings import Settings


class IDbConnector(metaclass=ABCMeta):
    """
    Fields:
    __coordinated_connection: Connection
        Connection to the coordinated database
    __coordinated_connection_url: AnyURL
        URL coordinated database
    __settings: Settings
        env fields
    """
    __settings: Settings = Settings()
    __coordinated_connection: Connection = None
    __coordinated_connection_url: AnyUrl = None

    def connect(self):
        """
        Connect to coordinated database
        """
        try:
            engine = create_engine(self.__coordinated_connection_url, echo=True)
            self.__coordinated_connection = engine.connect()
        except SQLAlchemyError:
            logging.error(SQLAlchemyError, exc_info=True)

    def __init__(self, connection_url: AnyUrl):
        self.__coordinated_connection_url = connection_url

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
            object_type = get_type_of_commit_object(row)
            action_type = get_action_of_commit(row)
            if object_type == AttributeTypes.TABLE:
                name1, name2 = get_names_table_in_commit(row)
            elif object_type == AttributeTypes.COLUMN:
                tablename, name1, datatype1, name2, datatype2 = get_names_column_in_commit(row)
            if object_type == AttributeTypes.TABLE:
                if action_type == CommitActionTypes.CREATE and name1 is None and name2 is not None:
                    row.sql_up = self._create_table(name2)
                    row.sql_down = self._delete_table(name2)
                elif action_type == CommitActionTypes.ALTER and name1 is not None and name2 is not None:
                    row.sql_up = self._alter_table(name1, name2)
                    row.sql_down = self._alter_table(name2, name1)
                elif action_type == CommitActionTypes.DROP and name1 is not None and name2 is None:
                    row.sql_up = self._delete_table(name1)
                    row.sql_down = self._create_table(name1)
            elif object_type == AttributeTypes.COLUMN:
                if action_type == CommitActionTypes.CREATE and name1 is None and datatype1 is None and name2 is not None and datatype2 is not None and tablename is not None:
                    row.sql_up = self._create_column(tablename, name2, datatype2)
                    row.sql_down = self._delete_column(tablename, name2)
                if action_type == CommitActionTypes.ALTER and name1 is not None and datatype1 is not None and name2 is not None and datatype2 is not None and tablename is not None:
                    row.sql_up = self._alter_column(tablename, name1, name2, datatype1, datatype2)
                    row.sql_down = self._alter_column(tablename, name2, name1, datatype2, datatype1)
                if action_type == CommitActionTypes.DROP and name1 is not None and name2 is None and datatype1 is not None and datatype2 is None and tablename is not None:
                    row.sql_up = self._delete_column(tablename, name1)
                    row.sql_down = self._create_column(tablename, name1, datatype1)

    def execute(self, str_up: str):
        """
        Execute Sql code
        """
        self.__coordinated_connection.execute(str_up)


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
        n1 = " \n"
        first_query = f"ALTER TABLE {tablename} RENAME COLUMN {columnname} TO {new_name};".join(n1).lstrip()
        second_query = f"ALTER TABLE {tablename} ALTER COLUMN {new_name} TYPE {new_datatype} USING {new_name}::{new_datatype};"
        return f"{first_query}{second_query}"
