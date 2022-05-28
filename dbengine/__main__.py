import logging

logging.basicConfig(level=logging.DEBUG,
                    format="\x1b[33;20m%(asctime)s %(levelname)s %(module)s:%(funcName)s %(message)s\x1b[0m")

from .models import create_tables
from .branch import *
from .table import *
from .column import *

#
# create_tables()
# init_main()

# create_branch('test1')
# create_table(get_branch(2), 'testtable')
#
##get_table(get_branch(2), 1)
# update_table(get_branch(2), get_table(get_branch(2), 1), "test_up22d222ate2")
# delete_table(get_branch(2), get_table(get_branch(2), 1))
create_column(get_branch(2), get_table(get_branch(2), 1))
# create_branch('test2')
# create_table(get_branch(3), 'testtable2')
# create_column(get_branch(3), get_table(get_branch(3), 2)[0], name="testcolumn", datatype="testtype")
# update_column(get_branch(3), get_column(get_branch(3), 3), name="hehupdlololololeted", datatype="updated")
# create_column(get_branch(3), get_table(get_branch(3), 5)[0], name="here")
# delete_column(get_branch(3), get_column(get_branch(3), 3))
# create_table(get_branch(4), "thistable")
# create_column(get_branch(4), get_table(get_branch(4), 8)[0], name="thiscol", datatype="thistype")
# update_column(get_branch(4), get_table(get_branch(4), 8)[0],
#               get_column(get_branch(4), get_table(get_branch(4), 8)[0], 9)[0], name="thisupdate!",
#               datatype="thistypeupdate")

# update_column(get_branch(5), get_table(get_branch(5), 10)[0],
#              get_column(get_branch(5), get_table(get_branch(5), 10)[0], 11)[0], name="aaaaaa", datatype="((")
# delete_column(get_branch(3), get_column(get_branch(3), 7)[0])
ok_branch(get_branch(3))
