import logging
from typing import Union

from .models import Branch

logger = logging.getLogger(__name__)


def init_main() -> Branch:
    """Инициализировать ветку main если веток еще не существует.

    Тип ветки MAIN, название ветки Main
    """
    logger.debug("init_main")


def create_branch(name) -> Branch:
    """Создать новую ветку из головы main ветки.

    Тип ветки WIP, название ветки `name`
    """
    logger.debug("create_branch")


def request_merge_branch(branch: Branch) -> Branch:
    """Поменять тип ветки на MR

    Только если сейчас WIP
    """
    logger.debug("request_merge_branch")


def unrequest_merge_branch(branch: Branch) -> Branch:
    """Поменять тип ветки на WIP

    Только если сейчас MR
    """
    logger.debug("unrequest_merge_branch")


def ok_branch(branch: Branch) -> Branch:
    """Поменять тип ветки на MERGED

    Только если сейчас MR
    """
    logger.debug("ok_branch")


def get_branch(name_or_id: Union[str, int]) -> Branch:
    """Return branch by id or name"""
    logger.debug("get_branch")
