from dbengine import *
from . import Session


def test_create():
    session = Session()
    branch = create_branch("Test Table 1", session=session)
    table, tab_attr, commit = create_table(branch, "test_table_1", session=session)
    col_1, col_attr_1, commit_1 = create_column(branch, table, name="id", datatype="string", session=session)

    assert tab_attr.name == "test_table_1"
    assert tab_attr.table_id == table.id
    assert commit.attribute_id_in is None
    assert commit.attribute_id_out == tab_attr.id

    assert col_attr_1.name == "id"
    assert col_attr_1.datatype == "string"
    assert col_attr_1.column_id == col_1.id
    assert col_1.table_id == table.id
    assert commit_1.attribute_id_in is None
    assert commit_1.attribute_id_out == col_attr_1.id


def test_column_update():
    session = Session()
    branch = create_branch("Test Table 1", session=session)
    table, _, _ = create_table(branch, "test_table_1", session=session)
    col_1, col_attr_1, _ = create_column(branch, table, name="id", datatype="string", session=session)
    ucol_1, ucol_attr_1, ucommit_1 = update_column(branch, table, name="uid", datatype="integer", session=session)

    assert col_1.id == ucol_1.id
    assert ucommit_1.attribute_id_in == col_attr_1.id
    assert ucommit_1.attribute_id_out == ucol_attr_1.id

    tcol, tcol_attr = get_column(branch, col_1.id, session=session)
    assert tcol.id == ucol_1.id
    assert tcol_attr.name == "uid"
    assert tcol_attr.datatype == "integer"



def test_table_update():
    session = Session()
    branch = create_branch("Test Table 1", session=session)
    table, table_attr, _ = create_table(branch, "test_table_1", session=session)
    create_column(branch, table, name="id", datatype="string", session=session)
    utable, utable_attr, utable_commit = update_table(branch, table, name="upd_test_table_1", session=session)

    assert table.id == utable.id
    assert utable_commit.attribute_id_in == table_attr.id
    assert utable_commit.attribute_id_out == utable_attr.id
