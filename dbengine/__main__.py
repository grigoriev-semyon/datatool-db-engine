# import database

# database.create_tables()

# br1 = database.get_branch(database.create_new_branch("Second branch"))
# print(br1)
# database.ok_branch_creator_table(br1, "table1")
# database.ok_branch_creator_column(br1, "col1")
# database.ok_branch_creator_column(br1, "col2")
# database.ok_branch_creator_column(br1, "col3")
# database.ok_branch_creator_column(br1, "col4")

# database.ok_branch_creator_table(br1, "table2")


# database.create_column('testcolumn', 'COLUMN')
# database.create_tables()
# database.create_new_branch("test5")
# database.create_new_branch("test6")
# database.create_new_branch("test4")
# print(database.get_branch(2))
# database.ok_branch_creator_column(database.get_branch(2), "creator works")
# database.ok_branch_deleter_column(database.get_branch(4), 2)
# #database.ok_branch_changer_column(database.get_branch(3), "DOES CHANGER WORKS??/", 2)
# database.ok_branch_creator_table(database.get_branch(5), "hehh")
# database.ok_branch_alter_table(database.get_branch(5), "chander table works", 5)
# database.delete_table(database.get_branch(6), 5)
# database.merge(database.get_branch(5), "MERGED")
