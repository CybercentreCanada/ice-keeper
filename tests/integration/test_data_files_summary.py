import datetime
from datetime import timezone

import pyspark.sql.functions as F  # noqa:N812
import pytest
from pyspark.sql import Row

from ice_keeper import Action
from ice_keeper.ice_keeper import MaintenanceScheduleRecord
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL, Scope
from ice_keeper.table import MaintenanceSchedule, MaintenanceScheduleEntry
from ice_keeper.table.schedule_entry import IceKeeperTblProperty
from ice_keeper.task import ActionTaskFactory
from ice_keeper.task.action.optimization.datafile_summary import DataFilesSummary
from tests.test_common import (
    ONE_EXPECTED,
    SCOPE_WHERE_FULL_NAME,
    SEVEN_EXPECTED,
    TEST_CATALOG_NAME,
    TEST_FULL_NAME,
    TEST_SCHEMA_NAME,
)
from tests.utils import (
    create_empty_test_table,
    discover_tables,
    insert_data,
)


def set_mnt_props_age(mnt_props: MaintenanceScheduleEntry, min_age: int, max_age: int) -> MaintenanceScheduleEntry:
    record = mnt_props.record
    # Make a copy of the record before mutating
    record_copy = record.model_copy()
    record_copy.min_age_to_optimize = min_age
    record_copy.max_age_to_optimize = max_age
    row = Row(**record_copy.model_dump(by_alias=True))
    return MaintenanceScheduleRecord.from_row(row).to_entry()


def set_mnt_partition_to_optimize(mnt_props: MaintenanceScheduleEntry, min_p: str, max_p: str) -> MaintenanceScheduleEntry:
    record = mnt_props.record
    # Make a copy of the record before mutating
    record_copy = record.model_copy()
    record_copy.min_partition_to_optimize = min_p
    record_copy.max_partition_to_optimize = max_p
    row = Row(**record_copy.model_dump(by_alias=True))
    return MaintenanceScheduleRecord.from_row(row).to_entry()


def get_partition_time_from_summary(mnt_props: MaintenanceScheduleEntry) -> Row | None:
    spec_id = 0
    spec = mnt_props.partition_specs[spec_id]
    widening_rule = None
    datafiles_summary = DataFilesSummary(mnt_props, spec, spec_id, widening_rule)
    sql = datafiles_summary.create_summary_stmt()
    df = STL.sql_and_log(sql, "Retrieve rows from partition summary")
    rows = df.select(F.expr("min(partition_time)").alias("oldest"), F.expr("max(partition_time)").alias("most_recent")).take(1)
    if len(rows) == 0:
        return None
    return rows[0]


dt_first_utc = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
dt_second_utc = datetime.datetime(2025, 12, 2, 0, 0, 0, tzinfo=timezone.utc)
dt_third_utc = datetime.datetime(2025, 12, 3, 0, 0, 0, tzinfo=timezone.utc)

dt_first_18h_utc = datetime.datetime(2025, 12, 1, 18, 0, 0, tzinfo=timezone.utc)
dt_first_19h_utc = datetime.datetime(2025, 12, 1, 19, 0, 0, tzinfo=timezone.utc)
dt_first_20h_utc = datetime.datetime(2025, 12, 1, 20, 0, 0, tzinfo=timezone.utc)


d_first = datetime.date(2025, 12, 1)
d_second = datetime.date(2025, 12, 2)
d_third = datetime.date(2025, 12, 3)

dt_first = datetime.datetime(2025, 12, 1, 0, 0, 0)  # noqa: DTZ001
dt_second = datetime.datetime(2025, 12, 2, 0, 0, 0)  # noqa: DTZ001
dt_third = datetime.datetime(2025, 12, 3, 0, 0, 0)  # noqa: DTZ001

dt_first_18h = datetime.datetime(2025, 12, 1, 18, 0, 0)  # noqa: DTZ001
dt_first_19h = datetime.datetime(2025, 12, 1, 19, 0, 0)  # noqa: DTZ001
dt_first_20h = datetime.datetime(2025, 12, 1, 20, 0, 0)  # noqa: DTZ001


@pytest.mark.integration
def test_summary_age_day(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "binpack"
    properties: dict[str, str] = {}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)

    insert_data(event_time=dt_first_utc, num_inserts=1)
    insert_data(event_time=dt_second_utc, num_inserts=1)
    insert_data(event_time=dt_third_utc, num_inserts=1)

    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    mnt_props = maintenance_schedule.get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should have maintenance properties for the table"

    # Consider all partitions (age 1 is current partition)
    mnt_props = set_mnt_props_age(mnt_props, 1, 2000)
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_third

    # Do not process most recent partition
    mnt_props = set_mnt_props_age(mnt_props, 2, 2000)
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_second

    # Only partition age 2 to 5
    mnt_props = set_mnt_props_age(mnt_props, 2, 2)
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_second
    assert row.most_recent == d_second


@pytest.mark.integration
def test_summary_identity_day(executor: TaskExecutor) -> None:
    partitioned_by = "ts"
    optimization_strategy = "binpack"
    properties: dict[str, str] = {}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)

    insert_data(event_time=dt_first_utc, num_inserts=1)
    insert_data(event_time=dt_second_utc, num_inserts=1)
    insert_data(event_time=dt_third_utc, num_inserts=1)

    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    mnt_props = maintenance_schedule.get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should have maintenance properties for the table"

    # Consider all partitions
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0d", "2000d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first
    assert row.most_recent == dt_third

    # Do not process most recent partition
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "2000d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first
    assert row.most_recent == dt_second

    # skip current partition and up to 2d
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "2d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first
    assert row.most_recent == dt_second

    # skip current partition and up to 1d
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "1d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_second
    assert row.most_recent == dt_second

    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0d", "0d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_third
    assert row.most_recent == dt_third

    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0M", "0M")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first
    assert row.most_recent == dt_third


@pytest.mark.integration
def test_summary_partition_day(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "binpack"
    properties: dict[str, str] = {}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)

    insert_data(event_time=dt_first_utc, num_inserts=1)
    insert_data(event_time=dt_second_utc, num_inserts=1)
    insert_data(event_time=dt_third_utc, num_inserts=1)

    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    mnt_props = maintenance_schedule.get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should have maintenance properties for the table"

    # Consider all partitions
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0d", "2000d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_third

    # Do not process most recent partition
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "2000d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_second

    # skip current partition and up to 2d
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "2d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_second

    # skip current partition and up to 1d
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "1d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_second
    assert row.most_recent == d_second

    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0d", "0d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_third
    assert row.most_recent == d_third

    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0M", "0M")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_third


@pytest.mark.integration
def test_summary_partition_hour(executor: TaskExecutor) -> None:  # noqa: PLR0915
    partitioned_by = "hours(ts)"
    optimization_strategy = "binpack"
    properties: dict[str, str] = {}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)

    insert_data(event_time=dt_first_18h_utc, num_inserts=1)
    insert_data(event_time=dt_first_19h_utc, num_inserts=1)
    insert_data(event_time=dt_first_20h_utc, num_inserts=1)

    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    mnt_props = maintenance_schedule.get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should have maintenance properties for the table"

    # Consider all partitions
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0h", "2000h")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first_18h
    assert row.most_recent == dt_first_20h

    # Do not process most recent partition
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1h", "2000h")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first_18h
    assert row.most_recent == dt_first_19h

    # skip current partition and up to 2h
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1h", "2h")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first_18h
    assert row.most_recent == dt_first_19h

    # skip current partition and up to 1h
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1h", "1h")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first_19h
    assert row.most_recent == dt_first_19h

    # Assert grabbing all partitions
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0d", "2d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first_18h
    assert row.most_recent == dt_first_20h

    # Assert grabbing all partitions
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0d", "0d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first_18h
    assert row.most_recent == dt_first_20h

    # Assert no partitions to optimize 1 day ago
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "1d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest is None
    assert row.most_recent is None

    # Assert no partitions to optimize 1 day ago
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "10d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest is None
    assert row.most_recent is None


@pytest.mark.integration
def test_optimize_binpack_correct_hour(executor: TaskExecutor) -> None:
    partitioned_by = "hours(ts)"
    optimization_strategy = "binpack"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "2", IceKeeperTblProperty.MAX_AGE_TO_OPTIMIZE: "200"}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    # 6 files in hour 2025-12-01 00:00:00
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=3)
    dt = datetime.datetime(2025, 12, 1, 0, 59, 59, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=3)

    # 7 files in hour 2025-12-01 23:00:00
    dt = datetime.datetime(2025, 12, 1, 23, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=3)
    dt = datetime.datetime(2025, 12, 1, 23, 59, 59, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=4)

    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)

    sql = f"""select * from {TEST_FULL_NAME}.data_files where partition.ts_hour = 490175 """
    num_files_age_1 = STL.sql_and_log(sql).count()
    assert num_files_age_1 == SEVEN_EXPECTED, "Most recent hour should not be optimized, should have 7 files."
    sql = f"""select * from {TEST_FULL_NAME}.data_files where partition.ts_hour = 490152 """
    num_files_age_2 = STL.sql_and_log(sql).count()
    assert num_files_age_2 == ONE_EXPECTED, "Older hour should be optimized into one file."


@pytest.mark.integration
def test_optimize_binpack_correct_day(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "binpack"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "2", IceKeeperTblProperty.MAX_AGE_TO_OPTIMIZE: "200"}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=6)
    dt = datetime.datetime(2025, 12, 2, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=7)
    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    sql = f"""select * from {TEST_FULL_NAME}.data_files where partition.ts_day = '2025-12-02' """
    num_files_age_1 = STL.sql_and_log(sql).count()
    assert num_files_age_1 == SEVEN_EXPECTED, "Most recent day should not be optimized, should have 7 files."
    sql = f"""select * from {TEST_FULL_NAME}.data_files where partition.ts_day = '2025-12-01' """
    num_files_age_2 = STL.sql_and_log(sql).count()
    assert num_files_age_2 == ONE_EXPECTED, "Older day should be optimized into one file."


@pytest.mark.integration
def test_optimize_binpack_correct_month(executor: TaskExecutor) -> None:
    partitioned_by = "month(ts)"
    optimization_strategy = "binpack"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "2", IceKeeperTblProperty.MAX_AGE_TO_OPTIMIZE: "200"}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    # 6 files in month 2025-12-01
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=3)
    dt = datetime.datetime(2025, 12, 31, 0, 59, 59, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=3)

    # 7 files in month 2026-01-01
    dt = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=3)
    dt = datetime.datetime(2026, 1, 1, 23, 59, 59, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=4)

    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)

    sql = f"""select * from {TEST_FULL_NAME}.data_files where partition.ts_month = 672 """
    num_files_age_1 = STL.sql_and_log(sql).count()
    assert num_files_age_1 == SEVEN_EXPECTED, "Most recent month should not be optimized, should have 7 files."
    sql = f"""select * from {TEST_FULL_NAME}.data_files where partition.ts_month = 671 """
    num_files_age_2 = STL.sql_and_log(sql).count()
    assert num_files_age_2 == ONE_EXPECTED, "Older month should be optimized into one file."


@pytest.mark.integration
def test_optimize_binpack_correct_year(executor: TaskExecutor) -> None:
    partitioned_by = "year(ts)"
    optimization_strategy = "binpack"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "2", IceKeeperTblProperty.MAX_AGE_TO_OPTIMIZE: "200"}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    # 6 files in year 2024
    dt = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=3)
    dt = datetime.datetime(2024, 12, 31, 0, 59, 59, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=3)

    # 7 files in year 2025
    dt = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=3)
    dt = datetime.datetime(2025, 1, 1, 23, 59, 59, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=4)

    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)

    sql = f"""select * from {TEST_FULL_NAME}.data_files where partition.ts_year = 55 """
    num_files_age_1 = STL.sql_and_log(sql).count()
    assert num_files_age_1 == SEVEN_EXPECTED, "Most recent year should not be optimized, should have 7 files."
    sql = f"""select * from {TEST_FULL_NAME}.data_files where partition.ts_year = 54 """
    num_files_age_2 = STL.sql_and_log(sql).count()
    assert num_files_age_2 == ONE_EXPECTED, "Older year should be optimized into one file."
