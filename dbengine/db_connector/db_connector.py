import logging
from abc import ABCMeta, abstractmethod

from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.exc import SQLAlchemyError, DBAPIError
from sqlalchemy.future import Connection
from sqlalchemy.orm import Session, sessionmaker

from dbengine.methods.branch import get_action_of_commit, get_type_of_commit_object, get_names_table_in_commit, \
    get_names_column_in_commit
from dbengine.models.branch import Branch, Commit, CommitActionTypes
from dbengine.models.entity import AttributeTypes
from dbengine.settings import Settings


class IDbConnector(metaclass=ABCMeta):
    _settings: Settings = Settings()
    _engine_db_dsn: Engine = None
    _engine_test: Engine = None
    _engine_prod: Engine = None
    _connection_test: Connection = None
    _connection_prod: Connection = None
    _session = None
    _Session = None

    def _connect(self):
        """Connect to DB and create self._connection Engine`"""
        try:
            self._engine_test = create_engine(self._settings.DWH_CONNECTION_TEST, echo=True)
            self._engine_prod = create_engine(self._settings.DWH_CONNECTION_PROD, echo=True)
            self._engine_db_dsn = create_engine(self._settings.DB_DSN, echo=True)
            self._connection_test = self._engine_test.connect()
            self._connection_prod = self._engine_prod.connect()
            self._Session = sessionmaker(self._engine_db_dsn)
            self._session = self._Session()
        except SQLAlchemyError:
            logging.error(SQLAlchemyError, exc_info=True)

    def get_session(self):
        return self._session

    def __init__(self):
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

    def _generate_migration(self, branch: Branch):
        """
        Generates SQL Code for migration any DataBase
        """
        code = []
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
                    code.append(self._create_table(name2))
                elif action_type == CommitActionTypes.ALTER and name1 is not None and name2 is not None:
                    code.append(self._alter_table(name1, name2))
                elif action_type == CommitActionTypes.DROP and name1 is not None and name2 is None:
                    code.append(self._delete_table(name1))
            elif object_type == AttributeTypes.COLUMN:
                if action_type == CommitActionTypes.CREATE and name1 is None and datatype1 is None and name2 is not None and datatype2 is not None and tablename is not None:
                    code.append(self._create_column(tablename, name2, datatype2))
                if action_type == CommitActionTypes.ALTER and name1 is not None and datatype1 is not None and name2 is not None and datatype2 is not None and tablename is not None:
                    code.append(self._alter_column(tablename, name1, name2, datatype1, datatype2))
                if action_type == CommitActionTypes.DROP and name1 is not None and name2 is None and datatype1 is not None and datatype2 is None and tablename is not None:
                    code.append(self._delete_column(tablename, name1))

        return code.__reversed__()

    def execute(self, branch: Branch):
        """Execute Sql code"""
        code = self._generate_migration(branch)

        for row in code:
            self._connection_test.execute(row)

            ##откат
        # try:
        #     for row in code:
        #         self._connection_prod.execute(row)
        # except DBAPIError:
        #     pass
        #     ##откат


class PostgreConnector(IDbConnector):
    @staticmethod
    def _create_table(tablename: str):
        return f"CREATE TABLE {tablename} ();"

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
        return f"{'ALTER TABLE'} {tablename} RENAME TO {new_tablename};"

    @staticmethod
    def _alter_column(tablename: str, columnname: str, new_name: str, datatype: str, new_datatype: str):
        tmp_columnname = f"tmp_{columnname}"
        return f"{'ALTER TABLE'}' {tablename} RENAME COLUMN {new_name} TO {tmp_columnname}" \
               f"ALTER TABLE {tablename} ADD {new_name} AS ({tmp_columnname} as {new_datatype})" \
               f"ALTER TABLE {tablename} DROP COLUMN {tmp_columnname};"
