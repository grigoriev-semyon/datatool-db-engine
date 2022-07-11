import logging
from abc import ABCMeta, abstractmethod
from typing import Final, Tuple, Optional, List

from pydantic import AnyUrl
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import SQLAlchemyError, DBAPIError
from sqlalchemy.future import Connection

from dbengine.exceptions import MigrationError
from dbengine.methods import get_table
from dbengine.models.branch import Branch, CommitActionTypes, Commit
from dbengine.models.entity import AttributeTypes


class IDbConnector(metaclass=ABCMeta):
    """
    Fields:
    __coordinated_connection: Connection
        Connection to the coordinated database
    __coordinated_connection_url: AnyURL
        URL coordinated database
    """
    __coordinated_connection: Connection = None
    __coordinated_connection_url: AnyUrl = None

    def connect(self):
        """
        Connect to coordinated database
        """
        try:
            engine = create_engine(self.__coordinated_connection_url, pool_pre_ping=True)
            self.__coordinated_connection = engine.connect()
        except SQLAlchemyError:
            logging.error(SQLAlchemyError, exc_info=True)

    def __init__(self, connection_url: AnyUrl):
        self.__coordinated_connection_url = connection_url

    @staticmethod
    def __get_action_of_commit(commit: Commit) -> str:
        """
        Get action if commit: Alter, Drop or Create
        """
        attr_in, attr_out = commit.attribute_id_in, commit.attribute_id_out
        if attr_in is not None and attr_out is not None:
            return CommitActionTypes.ALTER
        elif attr_in is not None and attr_out is None:
            return CommitActionTypes.DROP
        elif attr_in is None and attr_out is not None:
            return CommitActionTypes.CREATE

    @staticmethod
    def __get_names_table_in_commit(commit: Commit) -> Tuple:
        """
        Get old and new tablename in commit
        """
        attr_in, attr_out = commit.attribute_in, commit.attribute_out
        name1, name2 = None, None
        if IDbConnector.__get_type_of_commit_object(commit) == AttributeTypes.TABLE:
            if attr_in is not None:
                name1 = attr_in.name
            if attr_out is not None:
                name2 = attr_out.name
        return name1, name2

    @staticmethod
    def __get_names_column_in_commit(commit: Commit) -> Tuple:
        """
        Get old and new columnname in commit
        """
        attr_in, attr_out = commit.attribute_in, commit.attribute_out
        tablename, name1, name2, datatype1, datatype2 = None, None, None, None, None
        if IDbConnector.__get_type_of_commit_object(commit) == AttributeTypes.COLUMN:
            if attr_in is not None:
                name1, datatype1 = attr_in.name, attr_in.datatype
                tablename = get_table(commit.branch, attr_in.column.table_id, commit)[1].name
            if attr_out is not None:
                name2, datatype2 = attr_out.name, attr_out.datatype
                if not tablename:
                    tablename = get_table(commit.branch, attr_out.column.table_id, commit)[1].name
        return tablename, name1, datatype1, name2, datatype2

    @staticmethod
    def __get_type_of_commit_object(commit: Commit) -> Optional[str]:
        """
        Get type of changing object: Table or Column
        """
        anyattr = commit.attribute_in or commit.attribute_out
        if not anyattr:
            return None
        return anyattr.type

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
            object_type = IDbConnector.__get_type_of_commit_object(row)
            action_type = IDbConnector.__get_action_of_commit(row)
            if object_type == AttributeTypes.TABLE:
                name1, name2 = IDbConnector.__get_names_table_in_commit(row)
            elif object_type == AttributeTypes.COLUMN:
                tablename, name1, datatype1, name2, datatype2 = IDbConnector.__get_names_column_in_commit(row)
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
                elif action_type == CommitActionTypes.ALTER and name1 is not None and datatype1 is not None and name2 is not None and datatype2 is not None and tablename is not None:
                    row.sql_up = self._alter_column(tablename, name1, name2, datatype1, datatype2)
                    row.sql_down = self._alter_column(tablename, name2, name1, datatype2, datatype1)
                elif action_type == CommitActionTypes.DROP and name1 is not None and name2 is None and datatype1 is not None and datatype2 is None and tablename is not None:
                    row.sql_up = self._delete_column(tablename, name1)
                    row.sql_down = self._create_column(tablename, name1, datatype1)

    def upgrade(self, commits: List[Commit]) -> Optional[List[str]]:
        rollback, commits_up, commits_down = [], [], []
        for row in commits:
            if row.sql_up is not None and row.sql_down is not None:
                for line in reversed(row.sql_up.splitlines()):
                    commits_up.append(line)
                for line in row.sql_down.splitlines():
                    commits_down.append(line)
            if row.prev_commit.branch_id == 1:
                break
        i = 0
        commits_down.reverse()
        for row in commits_up.__reversed__():
            try:
                self.__coordinated_connection.execute(row)
                rollback.append(commits_down[i])
                i += 1
            except DBAPIError:
                raise MigrationError
        return rollback

    def downgrade(self, rollback: List[str]) -> None:
        for row in rollback.__reversed__():
            try:
                self.__coordinated_connection.execute(row)
            except DBAPIError:
                raise MigrationError


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


CONNECTOR_DICT: Final = {"postgresql": PostgreConnector}
