import database

# database.create_table('testtable')
# database.create_column('testcolumn', 'COLUMN')

# database.create_new_branch("test1")
#database.create_new_branch("test5")
#database.create_new_branch("test6")
# database.create_new_branch("test4")
# print(database.get_branch(2))
# database.ok_branch_creator_column(database.get_branch(2), "changed!")
database.ok_branch_deleter_column(database.get_branch(4), 2)
