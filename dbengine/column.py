import logging
from typing import Optional, Tuple, Union

from .models import Branch, Commit, DbColumn, DbColumnAttributes, DbTable

logger = logging.getLogger(__name__)


def create_column(
    branch: Branch, table: DbTable, *, name: str, datatype: str
) -> Tuple[DbColumn, DbColumnAttributes, Commit]:
    """Create column in table in branch"""
    logging.debug('create_column')


def get_column(
    branch: Branch, table: DbTable, id_or_name: Union[str, int]
) -> Tuple[DbColumn, DbColumnAttributes]:
    """Get last version of column in table in branch"""
    logging.debug('get_column')


def update_column(
    branch: Branch,
    table: DbTable,
    *,
    name: Optional[str] = None,
    datatype: Optional[str] = None
) -> Tuple[DbColumn, DbColumnAttributes, Commit]:
    """Update one or more attributes

    То есть создать новые атрибуты и указать начало и конец
    """
    logging.debug('update_column')


def delete_column(branch: Branch, table: DbTable):
    """Delete column from table from branch

    То есть надо удалить у колонки атрибуты, сам объект колонки останется
    То есть создать коммит с пустым концом
    """
    logging.debug('delete_column')
