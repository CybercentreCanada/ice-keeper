import datetime

import pytest

from ice_keeper import TimeProvider
from ice_keeper.config import Config
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL, Scope
from ice_keeper.table import MaintenanceScheduleRecord, StorageInventoryReport
from ice_keeper.table.schedule import MaintenanceSchedule
from ice_keeper.table.schedule_entry import MaintenanceScheduleEntry
from tests.test_common import (
    ONE_EXPECTED,
    TEST_CATALOG_NAME,
    TEST_FULL_NAME,
    TEST_SCHEMA_NAME,
    TEST_TABLE_NAME,
    TWO_EXPECTED,
    ZERO_EXPECTED,
    load_test_table,
)
from tests.utils import create_test_table


@pytest.mark.integration
def test_delete_empty_dir_recent(executor: TaskExecutor) -> None:
    TimeProvider.set(datetime.datetime(2025, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc))
    create_test_table(executor)
    table_location = load_test_table().location().replace("file:///", "")
    loaded_at = "date '2025-01-10'"
    last_modified = "timestamp '2025-01-06'"
    sql = f"""
        select *
        from (
        values
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_a', 0)
        )
        as t(loaded_at, last_modified, storage_account, container, file_path, file_size_in_bytes)
    """

    df = STL.sql(sql)
    df.createOrReplaceGlobalTempView("inventory_report")
    Config.instance().storage_inventory_report_table_name = "global_temp.inventory_report"

    mnt_props = MaintenanceSchedule(Scope()).get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should find our table"
    inventory = StorageInventoryReport(mnt_props)
    age_of_inventory_report = 2
    n_days = age_of_inventory_report + mnt_props.retention_days_orphan_files
    expected_n_days = 7
    assert n_days == expected_n_days, "Default orphan retention is 5, +2 day old retention report"
    older_than = TimeProvider.current_date() - datetime.timedelta(days=n_days)
    select_empty_folders_sql = inventory.select_empty_folders_from_inventory_stmt(older_than=older_than)
    empty_folders = [row.file_path for row in STL.sql(select_empty_folders_sql).collect()]
    assert len(empty_folders) == ZERO_EXPECTED, "Should not delete recent folders"


@pytest.mark.integration
def test_delete_empty_dir_no_partition(executor: TaskExecutor) -> None:
    TimeProvider.set(datetime.datetime(2025, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc))
    create_test_table(executor)
    table_location = load_test_table().location().replace("file:///", "")
    loaded_at = "date '2025-01-01'"
    last_modified = "timestamp '2025-01-01'"
    sql = f"""
        select *
        from (
        values
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/a.parquet', 1),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/b.parquet', 1),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/c.parquet', 1)
        )
        as t(loaded_at, last_modified, storage_account, container, file_path, file_size_in_bytes)
    """
    df = STL.sql(sql)
    df.createOrReplaceGlobalTempView("inventory_report")
    Config.instance().storage_inventory_report_table_name = "global_temp.inventory_report"

    mnt_props = MaintenanceSchedule(Scope()).get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should find our table"
    inventory = StorageInventoryReport(mnt_props)
    select_empty_folders_sql = inventory.select_empty_folders_from_inventory_stmt(older_than=TimeProvider.current_date())
    empty_folders = [row.file_path for row in STL.sql(select_empty_folders_sql).collect()]
    assert len(empty_folders) == ZERO_EXPECTED, "Table does not have any empty folders"


@pytest.mark.integration
def test_delete_empty_dir_no_partition_some_empty_dirs(executor: TaskExecutor) -> None:
    TimeProvider.set(datetime.datetime(2025, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc))
    create_test_table(executor)
    table_location = load_test_table().location().replace("file:///", "")
    loaded_at = "date '2025-01-01'"
    last_modified = "timestamp '2025-01-01'"
    sql = f"""
        select *
        from (
        values
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/a.parquet', 1),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/b.parquet', 1),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/c.parquet', 1),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/an_empty_dir', 0)
        )
        as t(loaded_at, last_modified, storage_account, container, file_path, file_size_in_bytes)
    """
    STL.sql(sql).show(truncate=False)
    df = STL.sql(sql)
    df.createOrReplaceGlobalTempView("inventory_report")
    Config.instance().storage_inventory_report_table_name = "global_temp.inventory_report"
    mnt_props = MaintenanceSchedule(Scope()).get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should find our table"
    inventory = StorageInventoryReport(mnt_props)
    select_empty_folders_sql = inventory.select_empty_folders_from_inventory_stmt(older_than=TimeProvider.current_date())
    empty_folders = [row.file_path for row in STL.sql_and_log(select_empty_folders_sql).collect()]
    assert len(empty_folders) == ONE_EXPECTED, "There is only one folder to delete"
    assert empty_folders[0].endswith("test/data/an_empty_dir"), "Should delete the single empty folder"


@pytest.mark.integration
def test_delete_empty_dir_no_partition_and_a_dir_with_zero_bytes_parquet(executor: TaskExecutor) -> None:
    TimeProvider.set(datetime.datetime(2025, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc))
    create_test_table(executor)
    table_location = load_test_table().location().replace("file:///", "")
    loaded_at = "date '2025-01-01'"
    last_modified = "timestamp '2025-01-01'"
    sql = f"""
        select *
        from (
        values
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_dir', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_dir/zero.parquet', 0)
        )
        as t(loaded_at, last_modified, storage_account, container, file_path, file_size_in_bytes)
    """
    df = STL.sql(sql)
    df.createOrReplaceGlobalTempView("inventory_report")
    Config.instance().storage_inventory_report_table_name = "global_temp.inventory_report"

    mnt_props = MaintenanceSchedule(Scope()).get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should find our table"
    inventory = StorageInventoryReport(mnt_props)
    select_empty_folders_sql = inventory.select_empty_folders_from_inventory_stmt(older_than=TimeProvider.current_date())
    empty_folders = [row.file_path for row in STL.sql(select_empty_folders_sql).collect()]
    assert len(empty_folders) == ZERO_EXPECTED, "Folders with parquet files of size zero are considered as none empty."


@pytest.mark.integration
def test_delete_empty_dir_no_partition_no_data_files(executor: TaskExecutor) -> None:
    TimeProvider.set(datetime.datetime(2025, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc))
    create_test_table(executor)
    table_location = load_test_table().location().replace("file:///", "")
    loaded_at = "date '2025-01-01'"
    last_modified = "timestamp '2025-01-01'"
    sql = f"""
        select *
        from (
        values
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data', 0)
        )
        as t(loaded_at, last_modified, storage_account, container, file_path, file_size_in_bytes)
    """
    df = STL.sql(sql)
    df.createOrReplaceGlobalTempView("inventory_report")
    Config.instance().storage_inventory_report_table_name = "global_temp.inventory_report"

    mnt_props = MaintenanceSchedule(Scope()).get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should find our table"
    inventory = StorageInventoryReport(mnt_props)
    select_empty_folders_sql = inventory.select_empty_folders_from_inventory_stmt(older_than=TimeProvider.current_date())
    empty_folders = [row.file_path for row in STL.sql(select_empty_folders_sql).collect()]
    assert len(empty_folders) == ZERO_EXPECTED, "Table with no data files at all, should have no folders that are empty."


@pytest.mark.integration
def test_delete_empty_dir_empty_sub_dir(executor: TaskExecutor) -> None:
    TimeProvider.set(datetime.datetime(2025, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc))
    create_test_table(executor)
    table_location = load_test_table().location().replace("file:///", "")
    loaded_at = "date '2025-01-01'"
    last_modified = "timestamp '2025-01-01'"
    sql = f"""
        select *
        from (
        values
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_a', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_a/sub_1_a', 0)
        )
        as t(loaded_at, last_modified, storage_account, container, file_path, file_size_in_bytes)
    """
    df = STL.sql(sql)
    df.createOrReplaceGlobalTempView("inventory_report")
    Config.instance().storage_inventory_report_table_name = "global_temp.inventory_report"

    mnt_props = MaintenanceSchedule(Scope()).get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should find our table"
    inventory = StorageInventoryReport(mnt_props)
    select_empty_folders_sql = inventory.select_empty_folders_from_inventory_stmt(older_than=TimeProvider.current_date())
    empty_folders = [row.file_path for row in STL.sql(select_empty_folders_sql).collect()]
    assert len(empty_folders) == TWO_EXPECTED, "Delete sub-folder and it's parent"
    assert empty_folders[0].endswith("test/data/sub_0_a/sub_1_a"), "Sub-folder is empty, should be deleted."
    assert empty_folders[1].endswith("test/data/sub_0_a"), "It's parent should also be deleted."


@pytest.mark.integration
def test_delete_empty_dir_empty_sub_dir_and_sub_dir(executor: TaskExecutor) -> None:
    TimeProvider.set(datetime.datetime(2025, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc))
    create_test_table(executor)
    table_location = load_test_table().location().replace("file:///", "")
    loaded_at = "date '2025-01-01'"
    last_modified = "timestamp '2025-01-01'"
    sql = f"""
        select *
        from (
        values
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_a', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_a/sub_1', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_a/sub_2', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_a/sub_1/a.parquet', 1)
        )
        as t(loaded_at, last_modified, storage_account, container, file_path, file_size_in_bytes)
    """
    df = STL.sql(sql)
    df.createOrReplaceGlobalTempView("inventory_report")
    Config.instance().storage_inventory_report_table_name = "global_temp.inventory_report"

    mnt_props = MaintenanceSchedule(Scope()).get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should find our table"
    inventory = StorageInventoryReport(mnt_props)
    select_empty_folders_sql = inventory.select_empty_folders_from_inventory_stmt(older_than=TimeProvider.current_date())
    empty_folders = [row.file_path for row in STL.sql(select_empty_folders_sql).collect()]
    assert len(empty_folders) == ONE_EXPECTED, "Only the sub_2 folder should be deleted"
    assert empty_folders[0].endswith("test/data/sub_0_a/sub_2"), "sub_2 folder should be deleted."


@pytest.mark.integration
def test_delete_empty_dir_with_empty_branch(executor: TaskExecutor) -> None:
    TimeProvider.set(datetime.datetime(2025, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc))
    create_test_table(executor)
    table_location = load_test_table().location().replace("file:///", "")
    loaded_at = "date '2025-01-01'"
    last_modified = "timestamp '2025-01-01'"
    sql = f"""
        select *
        from (
        values
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_a', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_a/sub_1_a', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_a/sub_1_a/x.parquet', 1),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_b', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_b/sub_1_b', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_b/sub_1_b/x.parquet', 1),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_c', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_c/sub_1_c', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_d', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_d/sub_1_d', 0),
            ({loaded_at}, {last_modified}, "", "", '{table_location}/data/sub_0_d/sub_1_d/x.parquet', 0)
        )
        as t(loaded_at, last_modified, storage_account, container, file_path, file_size_in_bytes)
    """
    df = STL.sql(sql)
    df.createOrReplaceGlobalTempView("inventory_report")
    Config.instance().storage_inventory_report_table_name = "global_temp.inventory_report"

    mnt_props = MaintenanceSchedule(Scope()).get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should find our table"
    inventory = StorageInventoryReport(mnt_props)
    select_empty_folders_sql = inventory.select_empty_folders_from_inventory_stmt(older_than=TimeProvider.current_date())
    empty_folders = [row.file_path for row in STL.sql(select_empty_folders_sql).collect()]
    assert len(empty_folders) == TWO_EXPECTED, "There are two folders to delete"
    assert empty_folders[0].endswith("test/data/sub_0_c/sub_1_c"), "sub_1_c"
    assert empty_folders[1].endswith("test/data/sub_0_c"), "and it's parent sub_0_c"


@pytest.mark.integration
def test_select_files_and_empty_folders_from_inventory_stmt_abfss() -> None:
    TimeProvider.set(datetime.datetime(2025, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc))
    table_location = "abfss://warehouse@storage_account_name.dfs.core.windows.net/iceberg/test/test"
    loaded_at = "date '2025-01-01'"
    last_modified = "timestamp '2025-01-01'"
    sql = f"""
        select *
        from (
        values
            ({loaded_at}, {last_modified}, 'storage_account_name', 'warehouse', 'warehouse/iceberg/test/test/data/sub_1', 0)
        )
        as t(loaded_at, last_modified, storage_account, container, file_path, file_size_in_bytes)
    """
    df = STL.sql(sql)
    df.createOrReplaceGlobalTempView("inventory_report")
    Config.instance().storage_inventory_report_table_name = "global_temp.inventory_report"

    record = MaintenanceScheduleRecord(
        full_name=TEST_FULL_NAME,
        catalog=TEST_CATALOG_NAME,
        schema=TEST_SCHEMA_NAME,
        table_name=TEST_TABLE_NAME,
        table_location=table_location,
    )
    mnt_props = MaintenanceScheduleEntry(record)
    inventory = StorageInventoryReport(mnt_props)
    sql = inventory.select_files_and_empty_folders_from_inventory_stmt(older_than=TimeProvider.current_date())
    empty_folders = [row.file_path for row in STL.sql(sql).collect()]
    assert len(empty_folders) == ONE_EXPECTED, "There is one folder to delete"
    assert empty_folders[0] == "abfss://warehouse@storage_account_name.dfs.core.windows.net/iceberg/test/test/data/sub_1"


@pytest.mark.integration
def test_select_files_and_empty_folders_from_inventory_stmt_file() -> None:
    TimeProvider.set(datetime.datetime(2025, 1, 12, 0, 0, 0, tzinfo=datetime.timezone.utc))
    table_location = "file:///iceberg/test/test"
    loaded_at = "date '2025-01-01'"
    last_modified = "timestamp '2025-01-01'"
    sql = f"""
        select *
        from (
        values
            ({loaded_at}, {last_modified}, '', '', 'iceberg/test/test/data/sub_1', 0)
        )
        as t(loaded_at, last_modified, storage_account, container, file_path, file_size_in_bytes)
    """
    df = STL.sql(sql)
    df.createOrReplaceGlobalTempView("inventory_report")
    Config.instance().storage_inventory_report_table_name = "global_temp.inventory_report"

    record = MaintenanceScheduleRecord(
        full_name=TEST_FULL_NAME,
        catalog=TEST_CATALOG_NAME,
        schema=TEST_SCHEMA_NAME,
        table_name=TEST_TABLE_NAME,
        table_location=table_location,
    )
    mnt_props = MaintenanceScheduleEntry(record)
    inventory = StorageInventoryReport(mnt_props)
    sql = inventory.select_files_and_empty_folders_from_inventory_stmt(older_than=TimeProvider.current_date())
    empty_folders = [row.file_path for row in STL.sql(sql).collect()]
    assert len(empty_folders) == ONE_EXPECTED, "There is one folder to delete"
    assert empty_folders[0] == "file:///iceberg/test/test/data/sub_1"
