import pytest
from sqlalchemy import column
from dbengine import *
from dbengine.exceptions import TableDoesntExists, ColumnDoesntExists, ColumnDeleted, TableDeleted
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


def test_table_get():
    session = Session()
    branch = create_branch("Test Table 1", session=session)
    branch2 = create_branch("Test Table 3", session=session)
    table, attrs, _ = create_table(branch, "test_table_1", session=session)
    create_column(branch, table, name="id", datatype="string", session=session)
    request_merge_branch(branch, session=session)
    ok_branch(branch, session=session)
    table_id = table.id
    attrs_id = attrs.id
    attrs_table_id = attrs.table_id
    attrs_name = attrs.name
    session.commit()

    # Тут должно падать потому, что ветка была сделана в момент до того, как колонка была создана
    with pytest.raises(TableDoesntExists):
        get_table(branch2, table_id, session=session)

    usession = Session()
    ubranch = create_branch("Test Table 2", session=usession)
    # FIXME: В ветке не существует таблицы. Но эта таблица существует в main ветке и которая бранчуется позже
    utable, uattrs = get_table(ubranch, table_id, session=usession)
    usession.commit()

    assert table_id == utable.id
    assert attrs_id != uattrs.id, "Commit in branch and in main are different"
    assert attrs_table_id == uattrs.table_id
    assert attrs_name == uattrs.name


def test_column_get():
    session = Session()
    branch = create_branch("Test Table 1", session=session)
    branch2 = create_branch("Test Table 3", session=session)
    table, _, _ = create_table(branch, "test_table_1", session=session)
    col, attrs, _ = create_column(branch, table, name="id", datatype="string", session=session)
    request_merge_branch(branch, session=session)
    ok_branch(branch, session=session)
    col_id = col.id
    attrs_id = attrs.id
    attrs_column_id = attrs.column_id
    attrs_name = attrs.name

    # Тут должно падать потому, что ветка была сделана в момент до того, как колонка была создана
    with pytest.raises(ColumnDoesntExists):
        get_column(branch2, col_id, session=session)

    # Тут колонка есть, потому что в мэйн уже изменения завезли
    ubranch = create_branch("Test Table 2", session=session)
    # FIXME: В ветке не существует колонки. Но эта колонке существует в main ветке и которая бранчуется позже
    ucol, uattrs = get_column(ubranch, col_id, session=session)

    assert col_id == ucol.id
    assert attrs_id != uattrs.id, "Commit in branch and in main are different"
    assert attrs_column_id == uattrs.column_id
    assert attrs_name == uattrs.name


def test_table_update():
    session = Session()
    branch = create_branch("Test Table 1", session=session)
    table, table_attr, _ = create_table(branch, "test_table_1", session=session)
    create_column(branch, table, name="id", datatype="string", session=session)
    utable, utable_attr, utable_commit = update_table(branch, table, name="upd_test_table_1", session=session)

    assert table.id == utable.id
    assert utable_commit.attribute_id_in == table_attr.id
    assert utable_commit.attribute_id_out == utable_attr.id


def test_column_update():
    session = Session()
    branch = create_branch("Test Table 1", session=session)
    table, _, _ = create_table(branch, "test_table_1", session=session)
    col_1, col_attr_1, _ = create_column(branch, table, name="id", datatype="string", session=session)
    ucol_1, ucol_attr_1, ucommit_1 = update_column(branch, col_1, name="uid", datatype="integer", session=session)

    assert col_1.id == ucol_1.id
    assert ucommit_1.attribute_id_in == col_attr_1.id
    assert ucommit_1.attribute_id_out == ucol_attr_1.id

    tcol, tcol_attr = get_column(branch, col_1.id, session=session)
    assert tcol.id == ucol_1.id
    assert tcol_attr.name == "uid"
    assert tcol_attr.datatype == "integer"


def test_table_delete():
    session = Session()
    branch = create_branch("Test Table 1", session=session)
    table, attrs, _ = create_table(branch, "test_table_1", session=session)
    col_1, _, _ = create_column(branch, table, name="id", datatype="string", session=session)
    ucommit = delete_table(branch, table, session=session)

    assert ucommit.attribute_id_in == attrs.id
    assert ucommit.attribute_id_out is None

    with pytest.raises(TableDeleted):
        get_table(branch, table.id, session=session)

    with pytest.raises(ColumnDeleted):
        get_column(branch, col_1.id, session=session)


def test_column_delete():
    session = Session()
    branch = create_branch("Test Table 1", session=session)
    table, _, _ = create_table(branch, "test_table_1", session=session)
    col, attrs, _ = create_column(branch, table, name="id", datatype="string", session=session)
    ucommit = delete_column(branch, col, session=session)

    with pytest.raises(ColumnDeleted):
        get_column(branch, col.id, session=session)

    assert ucommit.attribute_id_in == attrs.id
    assert ucommit.attribute_id_out is None
