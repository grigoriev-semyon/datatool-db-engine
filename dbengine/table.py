import logging
from typing import List, Optional, Tuple, Union

from .models import Branch, Commit, DbColumn, DbTable, DbTableAttributes

logger = logging.getLogger(__name__)


def create_table(
    branch: Branch, name: str, *, columns: Optional[List[DbColumn]] = None
) -> Tuple[DbTable, DbTableAttributes, Commit]:
    """Create table in branch with optional columns"""
    logging.debug("create_table")


def get_table(
    branch: Branch, id_or_name: Union[str, int]
) -> Tuple[DbTable, DbTableAttributes]:
    """Return table and last attributes in branch by id or name"""
    logging.debug("get_table")


def update_table(
    branch: Branch, table: DbTable, name: str
) -> Tuple[DbTable, DbTableAttributes, Commit]:
    """Change name of table and commit to branch"""
    logging.debug("update_table")


def delete_table(branch: Branch, table: DbTable) -> Commit:
    """Delete table in branch"""
    logging.debug("delete_table")
