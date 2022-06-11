import logging
from abc import ABCMeta, abstractmethod

from pydantic import AnyUrl
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import Connection
from sqlalchemy.orm import Session

from dbengine.methods.branch import get_action_of_commit, get_type_of_commit_object, get_names_table_in_commit, \
    get_names_column_in_commit
from dbengine.models.branch import Branch, CommitActionTypes
from dbengine.models.entity import AttributeTypes
from dbengine.settings import Settings


class IDbConnector(metaclass=ABCMeta):
    _settings: Settings = Settings()
    _engine_db_dsn: Engine = None
    _engine: Engine = None
    _connection: Connection = None
    _connection_url: AnyUrl = None
    _session = None
    _Session = None

    def _connect(self):
        """Connect to DB and create self._connection Engine`"""
        try:
            self._engine = create_engine(self._connection_url, echo=True)
            self._engine_db_dsn = create_engine(self._settings.DB_DSN, echo=True)
            self._connection = self._engine.connect()
        except SQLAlchemyError:
            logging.error(SQLAlchemyError, exc_info=True)

    def get_session(self):
        return self._session

    def __init__(self, session: Session, connection_url: AnyUrl):
        self._session = session
        self._connection_url = connection_url
        self._connect()

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

    def generate_migration(self, branch: Branch):
        """
        Generates SQL Code for migration any DataBase
        """
        s = branch.commits
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
                    row.sql_up = self._create_table(name2)
                    row.sql_down = self._delete_table(name2)
                    self._session.flush()
                elif action_type == CommitActionTypes.ALTER and name1 is not None and name2 is not None:
                    row.sql_up = self._alter_table(name1, name2)
                    row.sql_down = self._alter_table(name2, name1)
                    self._session.flush()
                elif action_type == CommitActionTypes.DROP and name1 is not None and name2 is None:
                    row.sql_up = self._delete_table(name1)
                    row.sql_down = self._create_table(name1)
                    self._session.flush()
            elif object_type == AttributeTypes.COLUMN:
                if action_type == CommitActionTypes.CREATE and name1 is None and datatype1 is None and name2 is not None and datatype2 is not None and tablename is not None:
                    row.sql_up = self._create_column(tablename, name2, datatype2)
                    row.sql_down = self._delete_column(tablename, name2)
                    self._session.flush()
                if action_type == CommitActionTypes.ALTER and name1 is not None and datatype1 is not None and name2 is not None and datatype2 is not None and tablename is not None:
                    row.sql_up = self._alter_column(tablename, name1, name2, datatype1, datatype2)
                    row.sql_down = self._alter_column(tablename, name2, name1, datatype2, datatype1)
                    self._session.flush()
                if action_type == CommitActionTypes.DROP and name1 is not None and name2 is None and datatype1 is not None and datatype2 is None and tablename is not None:
                    row.sql_up = self._delete_column(tablename, name1)
                    row.sql_down = self._create_column(tablename, name1, datatype1)
                    self._session.flush()

    def execute(self, str: str):
        """Execute Sql code"""
        self._connection.execute(str)


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
