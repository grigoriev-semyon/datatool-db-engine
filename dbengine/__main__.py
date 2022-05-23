import logging
logging.basicConfig(level=logging.DEBUG, format="\x1b[33;20m%(asctime)s %(levelname)s %(module)s:%(funcName)s %(message)s\x1b[0m")

from .models import create_tables
from .branch import create_branch

create_tables()
create_branch('b1')