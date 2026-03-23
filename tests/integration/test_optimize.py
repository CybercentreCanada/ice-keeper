import datetime
from dataclasses import dataclass
from datetime import timezone

import humanfriendly
import pytest
from click.testing import CliRunner

from ice_keeper import Action, Status
from ice_keeper.ice_keeper import diagnose
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL, Scope
from ice_keeper.table import MaintenanceSchedule, PartitionHealth
from ice_keeper.table.journal import Journal
from ice_keeper.table.schedule_entry import IceKeeperTblProperty
from ice_keeper.task import ActionTaskFactory
from tests.test_common import (
    FIVE_EXPECTED,
    ONE_EXPECTED,
    SCOPE_SCHEMA,
    SCOPE_WHERE_FULL_NAME,
    SEVEN_EXPECTED,
    TEST_CATALOG_NAME,
    TEST_FULL_NAME,
    TEST_SCHEMA_NAME,
    TEST_TABLE_NAME,
    TWO_EXPECTED,
    ZERO_EXPECTED,
)
from tests.utils import (
    compare_multiline_strings,
    create_empty_test_table,
    create_test_table_with_data,
    discover_tables,
    insert_data,
)

default_sort_rewrite_data_files_options = f"""options => map(
                'max-concurrent-file-group-rewrites', '100',
                'partial-progress.enabled', 'true',
                'delete-file-threshold', '1',
                'remove-dangling-deletes', 'true',
                'max-file-group-size-bytes', '{humanfriendly.parse_size("200 GB", binary=True)!s}',
                'target-file-size-bytes', '536870912',
                'output-spec-id', '0',
                'rewrite-all', 'true',
                'shuffle-partitions-per-file', '8')"""

default_binpack_rewrite_data_files_options = f"""options => map(
                'max-concurrent-file-group-rewrites', '100',
                'partial-progress.enabled', 'true',
                'delete-file-threshold', '1',
                'remove-dangling-deletes', 'true',
                'max-file-group-size-bytes', '{humanfriendly.parse_size("200 GB", binary=True)!s}',
                'target-file-size-bytes', '536870912',
                'output-spec-id', '0',
                'rewrite-all', 'false')"""


@dataclass
class OptimizeTestCase:
    test_name: str
    partitioned_by: str
    optimization_strategy: str
    expected_output: str


# The schema of the test table is
# (ts timestamp, id int, name string, category string, submission struct<ts timestamp>)

optimize_test_scenarios = [
    OptimizeTestCase("not_partitioned_not_binpack", "", "", ""),
    OptimizeTestCase("hours_ts__not_sorted", "hours(ts)", "", ""),
    OptimizeTestCase("hours_submission_ts__not_sorted", "hours(submission.ts)", "", ""),
    # ---------- binpack --------------
    OptimizeTestCase(
        "not_partitioned_binpack",
        "",
        "binpack",
        f"""
        CALL local.system.rewrite_data_files(
        table => 'test.test'
        , {default_binpack_rewrite_data_files_options}
        , strategy => 'binpack'
        , where => " (1 = 1) "
        )
        """,
    ),
    OptimizeTestCase(
        "hours_ts__binpack",
        "hours(ts)",
        "binpack",
        f"""CALL local.system.rewrite_data_files(
        table => 'test.test'
        , {default_binpack_rewrite_data_files_options}
        , strategy => 'binpack'
        , where => " ( ts >= timestamp('2025-03-03 18:00:00') and ts < timestamp('2025-03-03 18:00:00') + interval 1 hour ) "
        )
        """,
    ),
    OptimizeTestCase(
        "hours_submission_ts__binpack",
        "hours(submission.ts)",
        "binpack",
        f""" CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_binpack_rewrite_data_files_options}
            , strategy => 'binpack'
            , where => " ( submission.ts >= timestamp('2025-03-03 18:00:00') and submission.ts < timestamp('2025-03-03 18:00:00') + interval 1 hour )  "
            )
        """,
    ),
    OptimizeTestCase(
        "days_ts___bucket_3__id__binpack",
        "days(ts), bucket(3, id)",
        "binpack",
        f"""
        CALL local.system.rewrite_data_files(
                table => 'test.test'
                , {default_binpack_rewrite_data_files_options}
                , strategy => 'binpack'
                , where => " ( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) "
                 )
            """,
    ),
    # Truncate category_int to one bin.
    OptimizeTestCase(
        "truncate_category_int",
        "truncate(100000, category_int)",
        "binpack",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_binpack_rewrite_data_files_options}
            , strategy => 'binpack'
            , where => " ( local.system.truncate(100000, category_int) = 0 ) "
            )
        """,
    ),
    OptimizeTestCase(
        "months_ts___truncate_3__category__binpack",
        "months(ts), truncate(3, category)",
        "binpack",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_binpack_rewrite_data_files_options}
            , strategy => 'binpack'
            , where => " ( ts >= date('2025-03-01') and ts < date('2025-03-01') + interval 1 month )  "
            )
        """,
    ),
    # ---------- Sort --------------
    OptimizeTestCase(
        "year_sort_by_id",
        "years(ts)",
        "id",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-01-01') and ts < date('2025-01-01') + interval 1 year ) "
            , sort_order => 'id'
            )
        """,
    ),
    OptimizeTestCase(
        "not_partitioned_sort_by_id",
        "",
        "id",
        f"""
        CALL local.system.rewrite_data_files(
        table => 'test.test'
        , {default_sort_rewrite_data_files_options}
        , strategy => 'sort'
        , where => " (1 = 1) "
        , sort_order => 'id'
        )
        """,
    ),
    OptimizeTestCase(
        "hours_ts__id",
        "hours(ts)",
        "id",
        f"""CALL local.system.rewrite_data_files(
        table => 'test.test'
        , {default_sort_rewrite_data_files_options}
        , strategy => 'sort'
        , where => " ( ts >= timestamp('2025-03-03 18:00:00') and ts < timestamp('2025-03-03 18:00:00') + interval 1 hour ) "
        , sort_order => 'id'
        )
         """,
    ),
    OptimizeTestCase(
        "days_ts___bucket_4__id__id",
        "days(ts), bucket(4, id)",
        "id",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) "
            , sort_order => 'id'
            )
        """,
    ),
    OptimizeTestCase(
        "months_ts___truncate_1__category__name",
        "months(ts), truncate(1, category)",
        "name",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-01') and ts < date('2025-03-01') + interval 1 month ) "
            , sort_order => 'name'
            )
        """,
    ),
    OptimizeTestCase(
        "months_ts___truncate_2__category__name",
        "months(ts), truncate(2, category)",
        "name",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-01') and ts < date('2025-03-01') + interval 1 month ) "
            , sort_order => 'name'
            )
        """,
    ),
    OptimizeTestCase(
        "months_ts___truncate_4__category__id",
        "months(ts), truncate(4, category)",
        "id",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-01') and ts < date('2025-03-01') + interval 1 month ) "
            , sort_order => 'id'
            )
         """,
    ),
    OptimizeTestCase(
        "hours_submission_ts__id",
        "hours(submission.ts)",
        "id",
        f""" CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( submission.ts >= timestamp('2025-03-03 18:00:00') and submission.ts < timestamp('2025-03-03 18:00:00') + interval 1 hour ) "
            , sort_order => 'id'
            )
        """,
    ),
    OptimizeTestCase(
        "hours_submission_ts__id_DESC__submission_ts_nulls_first",
        "hours(submission.ts)",
        "id DESC, submission.ts nulls first",
        f""" CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( submission.ts >= timestamp('2025-03-03 18:00:00') and submission.ts < timestamp('2025-03-03 18:00:00') + interval 1 hour ) "
            , sort_order => 'id DESC, submission.ts nulls first'
             )
        """,
    ),
    OptimizeTestCase(
        "days_ts___bucket_2__id__id_ASC_NULLS_LAST",
        "days(ts), bucket(2, id)",
        "id ASC NULLS LAST",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) "
            , sort_order => 'id ASC NULLS LAST'
            )
        """,
    ),
    # ------- zorder --------
    OptimizeTestCase(
        "not_partitioned_zorder_id__name_",
        "",
        "zorder(id, name)",
        f"""
        CALL local.system.rewrite_data_files(
        table => 'test.test'
        , {default_sort_rewrite_data_files_options}
        , strategy => 'sort'
        , where => " (1 = 1) "
        , sort_order => 'zorder(id, name)'
        )
        """,
    ),
    OptimizeTestCase(
        "hours_ts__zorder_id__name_",
        "hours(ts)",
        "zorder(id, name)",
        f""" CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= timestamp('2025-03-03 18:00:00') and ts < timestamp('2025-03-03 18:00:00') + interval 1 hour ) "
            , sort_order => 'zorder(id, name)'
            )
        """,
    ),
    OptimizeTestCase(
        "hours_submission_ts__zorder_id__name_",
        "hours(submission.ts)",
        "zorder(id, name)",
        f""" CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( submission.ts >= timestamp('2025-03-03 18:00:00') and submission.ts < timestamp('2025-03-03 18:00:00') + interval 1 hour ) "
            , sort_order => 'zorder(id, name)'
             )
        """,
    ),
    OptimizeTestCase(
        "days_ts___bucket_5__id___bucket_2__category__zorder_id__name_",
        "days(ts), bucket(5, id), bucket(2, category)",
        "zorder(id, name)",
        f"""
         CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) "
            , sort_order => 'zorder(id, name)'
            )
        """,
    ),
    OptimizeTestCase(
        "days_ts___bucket_5__id___truncate_2__category__zorder_id__name_",
        "days(ts), bucket(5, id), truncate(2, category)",
        "zorder(id, name)",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) "
            , sort_order => 'zorder(id, name)'
            )
        """,
    ),
    OptimizeTestCase(
        "days_ts___bucket_5__id__zorder_id__name_",
        "days(ts), bucket(5, id)",
        "zorder(id, name)",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) "
            , sort_order => 'zorder(id, name)'
            )
          """,
    ),
    OptimizeTestCase(
        "months_ts___truncate_5__category__zorder_id__name_",
        "months(ts), truncate(5, category)",
        "zorder(id, name)",
        f"""
        CALL local.system.rewrite_data_files(
            table => 'test.test'
            , {default_sort_rewrite_data_files_options}
            , strategy => 'sort'
            , where => " ( ts >= date('2025-03-01') and ts < date('2025-03-01') + interval 1 month ) "
            , sort_order => 'zorder(id, name)'
            )
        """,
    ),
]


@pytest.mark.integration
@pytest.mark.parametrize(
    argnames=("test_name", "partitioned_by", "optimization_strategy", "expected_output"),
    argvalues=[
        (test.test_name, test.partitioned_by, test.optimization_strategy, test.expected_output)
        for test in optimize_test_scenarios
    ],
    ids=[test.test_name for test in optimize_test_scenarios],
)
def test_optimize(
    test_name: str, partitioned_by: str, optimization_strategy: str, expected_output: str, executor: TaskExecutor
) -> None:
    # Change default min age for testing.
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "1"}
    create_test_table_with_data(
        executor, partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties
    )

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    mnt_props = maintenance_schedule.entries()[0]
    assert mnt_props

    if mnt_props.optimization_spec and mnt_props.optimization_spec.is_zordered:
        dt = datetime.datetime(2025, 3, 3, 18, 33, 59, tzinfo=datetime.timezone.utc)
        insert_data(TEST_TABLE_NAME, event_time=dt, num_inserts=25)

    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    health_df = PartitionHealth().read(SCOPE_WHERE_FULL_NAME)
    health_df.show(truncate=False)
    for row in health_df.collect():
        before = row.before.n_records
        after = row.after.n_records
        if before != after:
            msg = f"In each partition the number of rows should be the same. before: {before}, after: {after}"
            raise Exception(msg)
        before = row.before.n_files
        after = row.after.n_files
        if not (before >= after):
            msg = f"In each partition the number of files should be smaller or equals. before: {before}, after: {after}"
            raise Exception(msg)
        before = row.before.corr
        after = row.after.corr
        if not (before <= after):
            msg = f"In each partition the rows should be better sorted or the same. before: {before}, after: {after}"
            raise Exception(msg)

    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    if expected_output != "":
        assert len(rows) == ONE_EXPECTED, "Should have a single log"
        if rows[0].status != Status.SUCCESS.value:
            assert rows[0].status_details == "", "Should succeed, why this error..."
        actual_output = rows[0].sql_stm
        diff, details = compare_multiline_strings(expected_output, actual_output)
        if diff:
            msg = f"Test {test_name} failed. The actual output was {actual_output}.\nDifferences are {details}"
            raise Exception(msg)
    else:
        assert len(rows) == ZERO_EXPECTED, "Should have no logs"


@pytest.mark.integration
def test_optimize_two_partitions(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "id asc"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "1"}
    create_test_table_with_data(executor, partitioned_by, optimization_strategy, properties)
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt)

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."

    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)

    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == TWO_EXPECTED, "Should have a two log"
    status0 = rows[0].status
    status_details0 = rows[0].status_details
    status1 = rows[1].status
    status_details1 = rows[1].status_details
    assert status0 == Status.SUCCESS.value
    assert status1 == Status.SUCCESS.value
    assert status_details0 == ""
    assert status_details1 == ""
    actual_output0 = rows[0].sql_stm
    actual_output1 = rows[1].sql_stm
    for actual_output in [actual_output0, actual_output1]:
        # normalize outputs
        if "2025-03-03" in actual_output:
            normalized_output = actual_output.replace("2025-03-03", "2020-01-01")
        elif "2025-12-01" in actual_output:
            normalized_output = actual_output.replace("2025-12-01", "2020-01-01")
        else:
            pytest.fail(reason="Day to optimize should one of 2025-12-01 or 2025-03-03")

        expected_output = """
            CALL local.system.rewrite_data_files(
                table => 'test.test'
                , options => map(
                    'max-concurrent-file-group-rewrites', '100',
                    'partial-progress.enabled', 'true',
                    'delete-file-threshold', '1',
                    'remove-dangling-deletes', 'true',
                    'max-file-group-size-bytes', '214748364800',
                    'target-file-size-bytes', '536870912',
                    'output-spec-id', '0',
                    'rewrite-all', 'true',
                    'shuffle-partitions-per-file', '8')
                , strategy => 'sort'
                , where => " ( ts >= date('2020-01-01') and ts < date('2020-01-01') + interval 1 day ) "
                , sort_order => 'id asc'
            )
        """
        diff, details = compare_multiline_strings(expected_output, normalized_output)
        if diff:
            msg = (
                f"Test test_optimize_two_partitions failed. The actual output was {normalized_output}.\nDifferences are {details}"
            )
            raise Exception(msg)


@pytest.mark.integration
def test_optimize_binpack_correct_hour(executor: TaskExecutor) -> None:
    partitioned_by = "hours(ts)"
    optimization_strategy = "binpack"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "2"}
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
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "2"}
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
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "2"}
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
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "2"}
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


@pytest.mark.integration
def test_optimize_invalid_column(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "invalid_column_name asc"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "1"}
    create_test_table_with_data(executor, partitioned_by, optimization_strategy, properties)
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt)

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."

    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == ONE_EXPECTED, "Should have a one log"
    status = rows[0].status
    status_details = rows[0].status_details
    assert status == Status.FAILED.value
    assert "is configured to sort columns [invalid_column_name] but these columns do not exist." in status_details


@pytest.mark.integration
def test_optimize_binpack_min_num_files(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "binpack"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "1"}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=5)
    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == ZERO_EXPECTED, "Should not binpack a single file"

    Journal.reset()
    insert_data(event_time=dt, num_inserts=1)
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == ONE_EXPECTED, "Should binpack since more than 5 files"


@pytest.mark.integration
def test_optimize_partitioned_by_category_sorted_by_id_max_age_2(executor: TaskExecutor) -> None:
    partitioned_by = "category"
    optimization_strategy = "id ASC"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "2"}
    create_test_table_with_data(executor, partitioned_by, optimization_strategy, properties)
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=5)
    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == FIVE_EXPECTED, (
        "The category column is not temporal, max age should not affect it. All 5 categories should be sorted."
    )


@pytest.mark.integration
def test_optimize_partitioned_by_category_and_by_day_ts(executor: TaskExecutor) -> None:
    partitioned_by = "category, days(ts)"
    optimization_strategy = "id ASC"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "1"}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=5)
    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == ONE_EXPECTED, "Should fail, so 1 journal row."
    assert rows[0].status == Status.FAILED.value
    assert "Unable to optimize table because the temporal partition is not the first one" in rows[0].status_details


@pytest.mark.integration
def test_optimize_special_partition_column(executor: TaskExecutor) -> None:
    special_column_name = "col+plus|pipe"
    sql = f"""
            create table {TEST_FULL_NAME}
            (`{special_column_name}` string, id int)
            using iceberg
            partitioned by ( truncate(8, `{special_column_name}`) )
            tblproperties (
            '{IceKeeperTblProperty.SHOULD_OPTIMIZE}'='true',
            '{IceKeeperTblProperty.OPTIMIZATION_STRATEGY}'='binpack',
            '{IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE}'='1'
            )
        """
    STL.sql_and_log(sql)

    for _i in range(6):
        STL.sql(
            f"""
                insert into {TEST_FULL_NAME}
                    select
                        'samestring' as `{special_column_name}`,
                        id as id
                    from
                        range(1, 1000)
                """,
        )

    discover_tables(executor, SCOPE_SCHEMA)
    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."

    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == ONE_EXPECTED, "Should have a one log"
    status = rows[0].status
    assert status == Status.SUCCESS.value
    assert "( local.system.truncate(8, `col+plus|pipe`) = 'samestri' )" in rows[0].sql_stm


@pytest.mark.integration
def test_optimize_special_partition_struct(executor: TaskExecutor) -> None:
    catalog = TEST_CATALOG_NAME
    schema = TEST_SCHEMA_NAME
    table_name = "test_optimize_special_partition_struct"
    full_name = f"{catalog}.{schema}.{table_name}"
    special_column_name = "col+plus|pipe.dot"
    sql = f"""
            create table {full_name}
            (base_struct struct<`{special_column_name}` string>, id int)
            using iceberg
            partitioned by ( truncate(8, base_struct.`{special_column_name}`) )
            tblproperties (
            '{IceKeeperTblProperty.SHOULD_OPTIMIZE}'='true',
            '{IceKeeperTblProperty.OPTIMIZATION_STRATEGY}'='binpack',
            '{IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE}'='1'
            )
        """
    STL.sql_and_log(sql)

    for _i in range(6):
        STL.sql(
            f"""
                insert into {full_name}
                    select
                        named_struct('{special_column_name}', 'samestring') as base_struct,
                        id as id
                    from
                        range(1, 1000)
                """,
        )

    discover_tables(executor, Scope(catalog, schema))
    maintenance_schedule = MaintenanceSchedule(Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME, table_name))
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."

    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == ONE_EXPECTED, "Should have a one log"
    status = rows[0].status
    assert status == Status.SUCCESS.value
    assert "( local.system.truncate(8, base_struct.`col+plus|pipe.dot`) = 'samestri' )" in rows[0].sql_stm


@pytest.mark.integration
def test_optimize_null_category(executor: TaskExecutor) -> None:
    # This demonstrates the limitation of ice-keeper in handling null
    # See https://github.chimera.cyber.gc.ca/CCCS/ice-keeper/issues/103
    partitioned_by = "category"
    optimization_strategy = "id ASC"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "1"}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    event_time = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)

    num_datafiles_per_category = 6
    num_category_including_null = 5
    for _i in range(num_datafiles_per_category):
        STL.sql(
            f"""
                insert into {TEST_FULL_NAME}
                    select
                        timestamp '{event_time}' as ts,
                        CAST((rand() * 4294967296) - 2147483648 AS INT) as id,
                        uuid() as name,
                        case when (id % {num_category_including_null} = 0) then NULL
                        else 'category_' || (id % {num_category_including_null})
                        end as category,
                        null as category_int,
                        named_struct('ts', timestamp '{event_time}') as submission
                    from
                        range(1, 10000)
                    """,
        )

    df = STL.sql_and_log(f"select partition, count(*) as num_files from {TEST_FULL_NAME}.data_files group by partition")
    rows = df.collect()
    print(rows)

    # Partitions to optimize in local.test.test_optimize_null_category
    # ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
    # ┃ partition_age ┃ category   ┃
    # ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
    # │ 1             │ category_4 │
    # │ 2             │ category_3 │
    # │ 3             │ category_2 │
    # │ 4             │ category_1 │
    # │ 5             │ None       │
    # └───────────────┴────────────┘

    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))
    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    results = executor.get_journal().flush().read_journal_entries(Scope())
    number_of_partition_rewritten = len(results)
    assert number_of_partition_rewritten == num_category_including_null

    warnings = []
    for result in results:
        assert result.status in [Status.SUCCESS.value, Status.WARNING.value]
        if result.status == Status.WARNING.value:
            warnings.append(result)
        else:
            # Verify category_1, category_2 partitions were optimized.
            assert result.rewritten_data_files_count == num_datafiles_per_category
            assert result.added_data_files_count == 1
    assert len(warnings) == ONE_EXPECTED

    result = warnings[0]
    assert "partition field category cannot be NULL" in result.status_details
    # This shows that if we could handle optimize_partition_depth=0
    # We would be able to optimize the data in the NULL partition field.
    #
    # df = STL.sql_and_log(f"select partition, count(*) as num_files from {full_name}.data_files group by partition")
    # rows = df.collect()
    # print(rows)
    # sql = """
    #         CALL local.system.rewrite_data_files(
    #                         table => 'test.test_optimize_null_category',
    #         options => map('max-concurrent-file-group-rewrites', '100',
    #         'partial-progress.enabled', 'true',
    #         'delete-file-threshold', '1',
    #         'remove-dangling-deletes', 'true',
    #         'max-file-group-size-bytes', '214748364800',
    #         'target-file-size-bytes', '536870912',
    #         'output-spec-id', '0',
    #         'rewrite-all', 'true',
    #         'shuffle-partitions-per-file', '8'),
    #         , strategy => 'sort'
    #         sort_order => 'id ASC'
    #                     )
    # """
    # rows = STL.sql_and_log(sql).collect()
    # print(rows)

    # df = STL.sql_and_log(f"select partition, count(*) as num_files from {full_name}.data_files group by partition")
    # rows = df.collect()
    # print(rows)


@pytest.mark.integration
def test_diagnose(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "id ASC"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "1"}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=5)
    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))
    full_name = TEST_FULL_NAME

    # Use CliRunner to invoke the diagnose command
    runner = CliRunner()
    result = runner.invoke(
        diagnose,
        [
            "--full_name",
            full_name,
            "--min_age_to_diagnose",
            "1",
            "--max_age_to_diagnose",
            "14",
            "--optimization_strategy",
            "id ASC",
        ],
    )

    # Assert the command executed successfully
    assert result.exit_code == 0, f"CliRunner failed: {result.output}"
