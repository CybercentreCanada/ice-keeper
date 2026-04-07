import pytest
from pyiceberg.table import Table
from pyspark.sql import Row

from ice_keeper import ActionFailed
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL, Scope
from ice_keeper.table.schedule import MaintenanceSchedule
from ice_keeper.table.schedule_entry import MaintenanceScheduleRecord
from tests.test_common import (
    SCOPE_SCHEMA,
    SCOPE_WHERE_FULL_NAME,
    TEST_FULL_NAME,
    TEST_TABLE_NAME,
    load_test_table,
)
from tests.utils import discover_tables, get_updated_mnt_props


@pytest.fixture
def test_table(executor: TaskExecutor) -> Table:
    STL.sql_and_log(
        f"""
            create table {TEST_FULL_NAME}
            (id int)
            using iceberg
        """,
    )
    discover_tables(executor, SCOPE_SCHEMA)
    return load_test_table()


def add_maintenance_entry(other_table_location: str) -> None:
    row = Row(catalog="x", schema="y", table_name="y", full_name="x.y.z", table_location=other_table_location)
    record = MaintenanceScheduleRecord.from_row(row)
    schedule = MaintenanceSchedule(Scope())
    schedule.merge_an_entry(record.to_entry())


def assert_table_location_not_unique() -> None:
    mnt_props = get_updated_mnt_props()
    with pytest.raises(ActionFailed):
        MaintenanceSchedule(SCOPE_WHERE_FULL_NAME).check_table_location_is_unique(mnt_props.full_name, mnt_props.table_location)


@pytest.mark.integration
def test_other_table_with_exact_same_location(test_table: Table) -> None:
    table_location = test_table.location()
    other_table_location = table_location
    # Other table with same location
    add_maintenance_entry(other_table_location)
    assert_table_location_not_unique()


@pytest.mark.integration
def test_other_table_with_same_sub_location(test_table: Table) -> None:
    table_location = test_table.location()
    other_table_location = table_location + "/sub_dir"
    # Other table location below ours
    add_maintenance_entry(other_table_location)
    assert_table_location_not_unique()


@pytest.mark.integration
def test_other_table_with_same_parent_location(test_table: Table) -> None:
    table_location = test_table.location()
    other_table_location = table_location[: -(len(TEST_TABLE_NAME) + 1)]
    # Other table with location under ours
    add_maintenance_entry(other_table_location)
    assert_table_location_not_unique()
