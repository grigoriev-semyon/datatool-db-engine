import logging

logging.basicConfig(level=logging.DEBUG,
                    format="\x1b[33;20m%(asctime)s %(levelname)s %(module)s:%(funcName)s %(message)s\x1b[0m")

from .models import create_tables
from .branch import *
from .table import *
from .column import *

# create_tables()
# create_main_branch()
# create_branch('test1')
# create_table(get_branch(2), 'testtable1')
# create_table(get_branch(2), 'testtable2')

# update_table(get_branch(2), get_table(get_branch(2), 1)[0], "test_update2")
# delete_table(get_branch(2), get_table(get_branch(2), 1)[0])

# create_branch('test2')
# create_table(get_branch(3), 'testtable3')
# create_table(get_branch(3), 'testtable4')
# create_column(get_branch(3), get_table(get_branch(3), 5)[0], name="testcolumn", datatype="testtype")
# update_column(get_branch(3), get_table(get_branch(3), 6)[0],
#               get_column(get_branch(3), get_table(get_branch(3), 5)[0], 7)[0], name="testupdate",
#               datatype="testupdatetype")
# create_column(get_branch(3), get_table(get_branch(3), 5)[0], name="here")
# create_table(get_branch(4), "thistable")
# create_column(get_branch(4), get_table(get_branch(4), 8)[0], name="thiscol", datatype="thistype")
# update_column(get_branch(4), get_table(get_branch(4), 8)[0],
#               get_column(get_branch(4), get_table(get_branch(4), 8)[0], 9)[0], name="thisupdate!",
#               datatype="thistypeupdate")

update_column(get_branch(5), get_table(get_branch(5), 10)[0],
              get_column(get_branch(5), get_table(get_branch(5), 10)[0], 11)[0], name="aaaaaa", datatype="((")
