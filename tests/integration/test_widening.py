import datetime
from datetime import timezone

import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, HourTransform, MonthTransform, YearTransform
from pyiceberg.types import NestedField, TimestamptzType
from pyspark.sql.types import Row

from ice_keeper import Action, Status
from ice_keeper.pool import TaskExecutor
from ice_keeper.spec import WideningRule
from ice_keeper.spec.partition_spec import PartitionSpecification
from ice_keeper.stm import STL, Scope
from ice_keeper.table import MaintenanceSchedule
from ice_keeper.table.journal import Journal
from ice_keeper.table.schedule_entry import IceKeeperTblProperty, MaintenanceScheduleEntry
from ice_keeper.task.action.factory import ActionTaskFactory
from tests.test_common import (
    ONE_EXPECTED,
    SCOPE_SCHEMA,
    SCOPE_WHERE_FULL_NAME,
    TEN_EXPECTED,
    TEST_FULL_NAME,
    THREE_EXPECTED,
    TWO_EXPECTED,
    ZERO_EXPECTED,
    set_tblproperties,
    set_tblproperty,
)
from tests.utils import compare_multiline_strings, discover_tables


def create_widening_table(executor: TaskExecutor, properties: dict[str, str] = {}) -> None:  # noqa: B006
    defaults = {
        IceKeeperTblProperty.SHOULD_OPTIMIZE: "true",
        IceKeeperTblProperty.OPTIMIZATION_STRATEGY: "id asc",
        IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH: "1",
        IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "1",  # Change default min age for testing
    }

    total_props = defaults | properties  # the properties dict takes preceedence.

    tblproperties_stm = ""
    if total_props:
        props_stm = ", ".join(f'"{key}" = "{value}"' for key, value in total_props.items())
        tblproperties_stm = f"tblproperties ({props_stm})"
    STL.sql(
        f"""
            create table {TEST_FULL_NAME}
            (ts timestamp, id int, _lag string)
            using iceberg
            partitioned by (days(ts))
            {tblproperties_stm}
        """,
    )

    discover_tables(executor, SCOPE_SCHEMA)


def get_refreshed_partition_mnt_props() -> MaintenanceScheduleEntry:
    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    mnt_props = maintenance_schedule.get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should be able to retrieve it"
    return maintenance_schedule.update_maintenance_schedule_entry(mnt_props, force=True)


def optimize(executor: TaskExecutor) -> list[Row]:
    Journal.reset()
    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    mnt_props = maintenance_schedule.get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should be able to retrieve it"
    executor.submit_tasks_and_wait(tasks)
    df = executor.get_journal().flush().read(Scope())
    return df.orderBy("start_time", ascending=True).collect()


def insert_rows(
    event_time: datetime.datetime,
    num_rows: int = 10000,
) -> None:
    STL.sql(
        f"""
        insert into {TEST_FULL_NAME}
            select
                timestamp '{event_time}' as ts,
                int(1000000000 * rand()) as id,
                case when id % 2 = 0 then 'leading'
                else 'lagging' end as _lag
            from
                range(0, {num_rows})
        """,
    )


def insert_rows_with_null_lag(
    event_time: datetime.datetime,
    num_rows: int = 10000,
) -> None:
    STL.sql(
        f"""
        insert into {TEST_FULL_NAME}
            select
                timestamp '{event_time}' as ts,
                int(1000000000 * rand()) as id,
                case when id % 2 = 0 then 'leading'
                when id % 3 = 0 then 'lagging'
                when id % 4 = 0 then NULL
                else NULL end as _lag
            from
                range(0, {num_rows})
        """,
    )


@pytest.mark.integration
def test_widening_config_defaults(executor: TaskExecutor) -> None:
    create_widening_table(executor)
    mnt_props = get_refreshed_partition_mnt_props()
    assert mnt_props.widening_rule_select_criteria == ""
    assert mnt_props.widening_rule_required_partition_columns == ""
    assert mnt_props.widening_rule_src_partition == ""
    assert mnt_props.widening_rule_dst_partition == ""
    assert mnt_props.widening_rule_min_age_to_widen == 1


@pytest.mark.integration
def test_widening_config_works(executor: TaskExecutor) -> None:
    create_widening_table(
        executor,
        {
            IceKeeperTblProperty.WIDENING_RULE_SELECT_CRITERIA: "x",
            IceKeeperTblProperty.WIDENING_RULE_REQUIRED_PARTITION_COLUMNS: "partition.col1, partition.col2, partition.`sub.col3`",
            IceKeeperTblProperty.WIDENING_RULE_SRC_PARTITION: "partition.y",
            IceKeeperTblProperty.WIDENING_RULE_DST_PARTITION: "partition.z",
            IceKeeperTblProperty.WIDENING_RULE_MIN_AGE_TO_WIDEN: "3",
        },
    )
    mnt_props = get_refreshed_partition_mnt_props()
    assert mnt_props.widening_rule_select_criteria == "x"
    columns = mnt_props.get_widening_rule_required_partition_columns()
    assert "partition.col1" in columns
    assert "partition.col2" in columns
    assert "partition.`sub.col3`" in columns
    assert mnt_props.widening_rule_src_partition == "partition.y"
    assert mnt_props.widening_rule_dst_partition == "partition.z"
    assert mnt_props.widening_rule_min_age_to_widen == THREE_EXPECTED


@pytest.mark.integration
def test_widening_config_missing_required_partition(executor: TaskExecutor) -> None:
    # No spec has a partitioned of that name
    create_widening_table(
        executor,
        {
            IceKeeperTblProperty.WIDENING_RULE_SELECT_CRITERIA: "partition._bogus in ('leading', 'lagging')",
            IceKeeperTblProperty.WIDENING_RULE_REQUIRED_PARTITION_COLUMNS: "partition.bogus",
            IceKeeperTblProperty.WIDENING_RULE_SRC_PARTITION: "partition.wrong_column_name_day",
            IceKeeperTblProperty.WIDENING_RULE_DST_PARTITION: "partition.wrong_column_name_month",
            IceKeeperTblProperty.WIDENING_RULE_MIN_AGE_TO_WIDEN: "1",
        },
    )
    mnt_props = get_refreshed_partition_mnt_props()
    with pytest.raises(
        ValueError, match=r"Could not find any partition specification with a partition named \[partition.wrong_column_name_day\]"
    ):
        WideningRule(
            mnt_props.partition_specs,
            mnt_props.widening_rule_src_partition,
            mnt_props.widening_rule_dst_partition,
            mnt_props.get_widening_rule_required_partition_columns(),
            mnt_props.widening_rule_select_criteria,
            mnt_props.widening_rule_min_age_to_widen,
        )

    # Fix src, but still invalid required column name
    set_tblproperty(IceKeeperTblProperty.WIDENING_RULE_SRC_PARTITION, "partition.ts_day")
    mnt_props = get_refreshed_partition_mnt_props()
    with pytest.raises(
        ValueError,
        match=r"Found partition specification with partition named \[partition.ts_day\]. But none of them had the required partitions \[partition.bogus\]",
    ):
        WideningRule(
            mnt_props.partition_specs,
            mnt_props.widening_rule_src_partition,
            mnt_props.widening_rule_dst_partition,
            mnt_props.get_widening_rule_required_partition_columns(),
            mnt_props.widening_rule_select_criteria,
            mnt_props.widening_rule_min_age_to_widen,
        )

    # Correct required name, but not partitioned by _lag
    set_tblproperty(IceKeeperTblProperty.WIDENING_RULE_REQUIRED_PARTITION_COLUMNS, "partition._lag")
    mnt_props = get_refreshed_partition_mnt_props()
    with pytest.raises(
        ValueError,
        match=r"Found partition specification with partition named \[partition.ts_day\]. But none of them had the required partitions \[partition._lag\]",
    ):
        WideningRule(
            mnt_props.partition_specs,
            mnt_props.widening_rule_src_partition,
            mnt_props.widening_rule_dst_partition,
            mnt_props.get_widening_rule_required_partition_columns(),
            mnt_props.widening_rule_select_criteria,
            mnt_props.widening_rule_min_age_to_widen,
        )


@pytest.mark.integration
def test_widening_config_missing_target_partition(executor: TaskExecutor) -> None:
    # No spec has a partitioned of that name
    create_widening_table(
        executor,
        {
            IceKeeperTblProperty.WIDENING_RULE_SELECT_CRITERIA: "partition._bogus in ('leading', 'lagging')",
            IceKeeperTblProperty.WIDENING_RULE_REQUIRED_PARTITION_COLUMNS: "partition._lag",
            IceKeeperTblProperty.WIDENING_RULE_SRC_PARTITION: "partition.ts_day",
            IceKeeperTblProperty.WIDENING_RULE_DST_PARTITION: "partition.wrong_column_name_month",
            IceKeeperTblProperty.WIDENING_RULE_MIN_AGE_TO_WIDEN: "1",
        },
    )
    # Add _lag partition
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")
    mnt_props = get_refreshed_partition_mnt_props()
    assert len(mnt_props.partition_specs.get_specifications()) == TWO_EXPECTED, "Should now have two partition specification"

    # Now that the source spec can be found it fails on dest.
    mnt_props = get_refreshed_partition_mnt_props()
    with pytest.raises(
        ValueError,
        match=r"Could not find any partition specification with a partition named \[partition.wrong_column_name_month\]",
    ):
        WideningRule(
            mnt_props.partition_specs,
            mnt_props.widening_rule_src_partition,
            mnt_props.widening_rule_dst_partition,
            mnt_props.get_widening_rule_required_partition_columns(),
            mnt_props.widening_rule_select_criteria,
            mnt_props.widening_rule_min_age_to_widen,
        )
    # Right name but spec does not exists
    set_tblproperty(IceKeeperTblProperty.WIDENING_RULE_DST_PARTITION, "partition.ts_month")
    mnt_props = get_refreshed_partition_mnt_props()
    with pytest.raises(
        ValueError, match=r"Could not find any partition specification with a partition named \[partition.ts_month\]"
    ):
        WideningRule(
            mnt_props.partition_specs,
            mnt_props.widening_rule_src_partition,
            mnt_props.widening_rule_dst_partition,
            mnt_props.get_widening_rule_required_partition_columns(),
            mnt_props.widening_rule_select_criteria,
            mnt_props.widening_rule_min_age_to_widen,
        )

    # Add spec month(ts) only spec
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field _lag")
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field days(ts)")
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field month(ts)")
    # Still missing, required _lag
    mnt_props = get_refreshed_partition_mnt_props()
    assert len(mnt_props.partition_specs.get_specifications()) > ONE_EXPECTED, "Should have more possible specs now."
    with pytest.raises(
        ValueError,
        match=r"Found partition specification with partition named \[partition.ts_month\]. But none of them had the required partitions \[partition._lag\]",
    ):
        WideningRule(
            mnt_props.partition_specs,
            mnt_props.widening_rule_src_partition,
            mnt_props.widening_rule_dst_partition,
            mnt_props.get_widening_rule_required_partition_columns(),
            mnt_props.widening_rule_select_criteria,
            mnt_props.widening_rule_min_age_to_widen,
        )

    # add a (month(ts) and _lag) partition spec
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")

    # This should work now
    mnt_props = get_refreshed_partition_mnt_props()
    rule = WideningRule(
        mnt_props.partition_specs,
        mnt_props.widening_rule_src_partition,
        mnt_props.widening_rule_dst_partition,
        mnt_props.get_widening_rule_required_partition_columns(),
        mnt_props.widening_rule_select_criteria,
        mnt_props.widening_rule_min_age_to_widen,
    )

    assert (
        rule.make_diagnosis_widening_expr_stmt()
        == "floor(months_between(date_trunc('month', partition.ts_day), date('1970-01-01')))"
    )


@pytest.mark.integration
def test_widening_wrong_depth(executor: TaskExecutor) -> None:
    create_widening_table(
        executor,
        {
            IceKeeperTblProperty.WIDENING_RULE_SELECT_CRITERIA: "partition._bogus in ('leading', 'lagging')",
            IceKeeperTblProperty.WIDENING_RULE_REQUIRED_PARTITION_COLUMNS: "partition._lag",
            IceKeeperTblProperty.WIDENING_RULE_SRC_PARTITION: "partition.ts_day",
            IceKeeperTblProperty.WIDENING_RULE_DST_PARTITION: "partition.ts_month",
            IceKeeperTblProperty.WIDENING_RULE_MIN_AGE_TO_WIDEN: "1",
        },
    )
    # Add _lag partition
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")

    # Remove existing specs
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field _lag")
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field days(ts)")
    # Add a month(ts) and a _lag
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field month(ts)")
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")

    event_time = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    for _ in range(10):
        insert_rows_with_null_lag(event_time)

    rows = optimize(executor)
    assert len(rows) == ONE_EXPECTED, "Should have a one log"
    status = rows[0].status
    status_details = rows[0].status_details
    assert status == Status.FAILED.value
    assert (
        "'ice-keeper.optimize-partition-depth' of 1, which is insufficient for the widening rule. The rule requires a minimum 'optimize_partition_depth' of 2."
        in status_details
    )

    # We have the wrong optimization depth, correct it.
    set_tblproperty(IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH, "2")
    rows = optimize(executor)
    assert len(rows) == ONE_EXPECTED, "Should have a one log"
    status = rows[0].status
    status_details = rows[0].status_details
    assert status == Status.FAILED.value
    assert "No such struct field `_bogus`" in status_details

    # The widening criteria uses a column that does not exists. Fix it.
    set_tblproperty(IceKeeperTblProperty.WIDENING_RULE_SELECT_CRITERIA, "partition._lag in ('leading', 'lagging')")
    rows = optimize(executor)
    assert len(rows) == THREE_EXPECTED, "Should have 3 logs"
    expected = "Some required columns have null values, which prevents a safe widening operation"
    # There are 3 values for _lag which are 'leading', 'lagging', and NULL.
    # None of the rewrites should execute because it detects a _lag=NULL which is a required partition column.
    for row in rows:
        status = row.status
        status_details = row.status_details
        assert status == Status.WARNING.value
        assert expected in status_details


@pytest.mark.integration
def test_widening_invalid_column_in_filtering_expr(executor: TaskExecutor) -> None:
    create_widening_table(
        executor,
        {
            IceKeeperTblProperty.SHOULD_OPTIMIZE: "true",
            IceKeeperTblProperty.OPTIMIZATION_STRATEGY: "id asc",
            IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH: "2",
        },
    )
    # Add _lag partition
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")

    # Add spec
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field _lag")
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field days(ts)")
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field month(ts)")
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")

    set_tblproperties(
        {
            IceKeeperTblProperty.WIDENING_RULE_SELECT_CRITERIA: "partition._bogus in ('leading', 'lagging')",
            IceKeeperTblProperty.WIDENING_RULE_REQUIRED_PARTITION_COLUMNS: "partition._lag",
            IceKeeperTblProperty.WIDENING_RULE_SRC_PARTITION: "partition.ts_day",
            IceKeeperTblProperty.WIDENING_RULE_DST_PARTITION: "partition.ts_month",
            IceKeeperTblProperty.WIDENING_RULE_MIN_AGE_TO_WIDEN: "1",
        },
    )
    event_time = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_rows_with_null_lag(event_time)

    rows = optimize(executor)
    # If we have an expected procedure call.
    assert len(rows) == ONE_EXPECTED, "Should have a one log"
    status = rows[0].status
    status_details = rows[0].status_details
    assert status == Status.FAILED.value
    assert "No such struct field `_bogus`" in status_details


@pytest.mark.integration
def test_widening_bad_filtering_expr(executor: TaskExecutor) -> None:
    create_widening_table(
        executor,
        {
            IceKeeperTblProperty.SHOULD_OPTIMIZE: "true",
            IceKeeperTblProperty.OPTIMIZATION_STRATEGY: "id asc",
            IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH: "2",
        },
    )
    # Add _lag partition
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")

    # Add spec
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field _lag")
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field days(ts)")
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field month(ts)")
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    mnt_props = maintenance_schedule.get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Should be able to retrieve it"

    set_tblproperties(
        {
            IceKeeperTblProperty.WIDENING_RULE_SELECT_CRITERIA: "partition._lag not valid sql here... in ('leading', 'lagging')",
            IceKeeperTblProperty.WIDENING_RULE_REQUIRED_PARTITION_COLUMNS: "partition._lag",
            IceKeeperTblProperty.WIDENING_RULE_SRC_PARTITION: "partition.ts_day",
            IceKeeperTblProperty.WIDENING_RULE_DST_PARTITION: "partition.ts_month",
            IceKeeperTblProperty.WIDENING_RULE_MIN_AGE_TO_WIDEN: "1",
        },
    )
    event_time = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_rows_with_null_lag(event_time)

    rows = optimize(executor)
    # If we have an expected procedure call.
    assert len(rows) == ONE_EXPECTED, "Should have a one log"
    status = rows[0].status
    status_details = rows[0].status_details
    assert status == Status.FAILED.value
    assert "PARSE_SYNTAX_ERROR" in status_details


@pytest.mark.integration
def test_widening_no_data_in_src_partition(executor: TaskExecutor) -> None:
    create_widening_table(executor)
    # Add _lag partition
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")
    set_tblproperties(
        {
            IceKeeperTblProperty.SHOULD_OPTIMIZE: "true",
            IceKeeperTblProperty.OPTIMIZATION_STRATEGY: "id asc",
            IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH: "2",
        },
    )

    # Add spec
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field _lag")
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field days(ts)")
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field month(ts)")
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")

    set_tblproperties(
        {
            IceKeeperTblProperty.WIDENING_RULE_SELECT_CRITERIA: "partition._lag in ('leading', 'lagging')",
            IceKeeperTblProperty.WIDENING_RULE_REQUIRED_PARTITION_COLUMNS: "partition._lag",
            IceKeeperTblProperty.WIDENING_RULE_SRC_PARTITION: "partition.ts_day",
            IceKeeperTblProperty.WIDENING_RULE_DST_PARTITION: "partition.ts_month",
            IceKeeperTblProperty.WIDENING_RULE_MIN_AGE_TO_WIDEN: "1",
        },
    )

    event_time = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_rows_with_null_lag(event_time)

    rows = optimize(executor)
    # If we have an expected procedure call.
    assert len(rows) == ZERO_EXPECTED, "Should skip no data"


@pytest.mark.integration
def test_widening_success(executor: TaskExecutor) -> None:
    create_widening_table(
        executor,
        {
            IceKeeperTblProperty.SHOULD_OPTIMIZE: "true",
            IceKeeperTblProperty.OPTIMIZATION_STRATEGY: "id asc",
            IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH: "2",
            IceKeeperTblProperty.WRITE_TARGET_FILE_SIZE_BYTES: "500000",  # write small files
        },
    )
    # Add _lag partition
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")

    # Add month spec
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field _lag")
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field days(ts)")
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field month(ts)")
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")

    # default back to day spec
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field _lag")
    STL.sql(f"alter table {TEST_FULL_NAME} drop partition field month(ts)")
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field days(ts)")
    STL.sql(f"alter table {TEST_FULL_NAME} add partition field _lag")

    set_tblproperties(
        {
            IceKeeperTblProperty.WIDENING_RULE_SELECT_CRITERIA: "partition._lag in ('leading', 'lagging')",
            IceKeeperTblProperty.WIDENING_RULE_REQUIRED_PARTITION_COLUMNS: "partition._lag",
            IceKeeperTblProperty.WIDENING_RULE_SRC_PARTITION: "partition.ts_day",
            IceKeeperTblProperty.WIDENING_RULE_DST_PARTITION: "partition.ts_month",
            IceKeeperTblProperty.WIDENING_RULE_MIN_AGE_TO_WIDEN: "1",
        },
    )
    # insert data into many days.
    for day in range(1, 10):
        event_time = datetime.datetime(2025, 10, day, 0, 0, 0, tzinfo=timezone.utc)
        for _ in range(5):
            insert_rows(event_time, num_rows=100000)

    # insert data into more recent month.
    event_time = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    for _ in range(1):
        insert_rows(event_time, num_rows=100000)

    df = STL.sql(f"""
        select *
        from {TEST_FULL_NAME}.data_files
        where partition.ts_day = '2025-10-01' and partition._lag = 'leading'
        """)
    num_files = df.count()
    assert num_files == TEN_EXPECTED, "10 inserts, 10 files"

    rows = optimize(executor)
    # If we have an expected procedure call.
    assert len(rows) > ZERO_EXPECTED, (
        "It should have made multiple rewrite calls for daily partitions and for the monthly widening."
    )
    actual_leading_stmt = ""
    expected_leading_pattern = (
        "( ts >= date('2025-10-01') and ts < date('2025-10-01') + interval 1 month ) and ( _lag = 'leading' )"
    )

    for row in rows:
        print(row.sql_stm)
        if expected_leading_pattern in row.sql_stm:
            actual_leading_stmt = row.sql_stm
            break
    found_optimize_10th_month = actual_leading_stmt != ""
    assert found_optimize_10th_month, "Check we found the montly widening."
    assert "'output-spec-id', '4'" in actual_leading_stmt, "The winderning rule should apply to montly partition, spec id 4"

    expected_leading_stmt = """
        CALL local.system.rewrite_data_files(
            table => 'test.test'
           , options => map(
           'max-concurrent-file-group-rewrites', '100',
           'partial-progress.enabled', 'false',
           'delete-file-threshold', '1',
           'remove-dangling-deletes', 'true',
           'max-file-group-size-bytes', '214748364800',
           'target-file-size-bytes', '500000',
           'output-spec-id', '4',
           'rewrite-all', 'true',
           'min-input-files', '1',
           'shuffle-partitions-per-file', '1')
           , strategy => 'sort'
           , where => " ( ts >= date('2025-10-01') and ts < date('2025-10-01') + interval 1 month ) and ( _lag = 'leading' ) "
           , sort_order => 'id asc'
        )
    """
    diff, details = compare_multiline_strings(expected_leading_stmt, actual_leading_stmt)
    if diff:
        msg = f"Test test_optimize_two_partitions failed. The actual output was {actual_leading_stmt}.\nDifferences are {details}"
        raise Exception(msg)

    # Verify that none of the day partitions have more than 5 data files.
    rows = STL.sql(f"""
        select *
        from (
            select partition, count(*) as num_files
            from {TEST_FULL_NAME}.data_files
            group by partition
        )
        where
        partition.ts_day is not null
        """).collect()
    # print(rows)
    assert len(rows) == ZERO_EXPECTED, "Verify that none of the day partitions have files."

    # Verify that we have data in the 10th month
    rows = STL.sql(f"""
        select *
        from (
            select partition, count(*) as num_files
            from {TEST_FULL_NAME}.data_files
            group by partition
        )
        where
        partition.ts_month is not null and partition._lag='leading'
        """).collect()
    assert len(rows) > ZERO_EXPECTED
    for row in rows:
        print(row)
        assert row.num_files >= ONE_EXPECTED, "Verify that the 10th month now has data."


@pytest.mark.integration
def hour_partition() -> PartitionSpecification:
    field_name = "ts"
    partition_name = "ts_hour"
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=TimestamptzType(), required=False),
        schema_id=1,
    )
    iceberg_partition_spec = PartitionSpec(PartitionField(1, 1001, HourTransform(), partition_name))
    catalog = "test"
    return PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)


@pytest.mark.integration
def day_partition() -> PartitionSpecification:
    field_name = "ts"
    partition_name = "ts_day"
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=TimestamptzType(), required=False),
        schema_id=1,
    )
    iceberg_partition_spec = PartitionSpec(PartitionField(1, 1001, DayTransform(), partition_name))
    catalog = "test"
    return PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)


@pytest.mark.integration
def month_partition() -> PartitionSpecification:
    field_name = "ts"
    partition_name = "ts_month"
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=TimestamptzType(), required=False),
        schema_id=1,
    )
    iceberg_partition_spec = PartitionSpec(PartitionField(1, 1001, MonthTransform(), partition_name))
    catalog = "test"
    return PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)


@pytest.mark.integration
def year_partition() -> PartitionSpecification:
    field_name = "ts"
    partition_name = "ts_year"
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=TimestamptzType(), required=False),
        schema_id=1,
    )
    iceberg_partition_spec = PartitionSpec(PartitionField(1, 1001, YearTransform(), partition_name))
    catalog = "test"
    return PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)


@pytest.mark.integration
def test_widening_hour_to_day() -> None:
    src_specification = hour_partition()
    dst_specification = day_partition()
    partition_stmt = "(select named_struct('ts_hour', 491914) as partition)"

    src_trans = src_specification.get_base_partition().transformation
    dst_trans = dst_specification.get_base_partition().transformation
    stmt = dst_trans.make_diagnosis_widening_expr_stmt(src_trans)
    assert stmt == "date(timestamp(from_unixtime(3600 * partition.ts_hour)))"
    result = STL.sql(f"select {stmt} as result from {partition_stmt}").take(10)[0].result
    day_expected = datetime.date(2026, 2, 12)
    assert result == day_expected


@pytest.mark.integration
def test_widening_hour_to_month() -> None:
    src_specification = hour_partition()
    dst_specification = month_partition()
    partition_stmt = "(select named_struct('ts_hour', 491914) as partition)"

    src_trans = src_specification.get_base_partition().transformation
    dst_trans = dst_specification.get_base_partition().transformation
    stmt = dst_trans.make_diagnosis_widening_expr_stmt(src_trans)
    assert (
        stmt
        == "floor(months_between(date_trunc('month', timestamp(from_unixtime(3600 * partition.ts_hour))), date('1970-01-01')))"
    )
    result = STL.sql(f"select {stmt} as result from {partition_stmt}").take(10)[0].result
    months_expected = 673
    assert result == months_expected


@pytest.mark.integration
def test_widening_hour_to_year() -> None:
    src_specification = hour_partition()
    dst_specification = year_partition()
    partition_stmt = "(select named_struct('ts_hour', 491914) as partition)"

    src_trans = src_specification.get_base_partition().transformation
    dst_trans = dst_specification.get_base_partition().transformation
    stmt = dst_trans.make_diagnosis_widening_expr_stmt(src_trans)
    assert stmt == "(year(from_unixtime(partition.ts_hour * 3600)) - year('1970-01-01'))"
    result = STL.sql(f"select {stmt} as result from {partition_stmt}").take(10)[0].result
    years_expected = 56
    assert result == years_expected


@pytest.mark.integration
def test_widening_day_to_month() -> None:
    src_specification = day_partition()
    dst_specification = month_partition()
    partition_stmt = "(select named_struct('ts_day', '2026-02-12') as partition)"

    src_trans = src_specification.get_base_partition().transformation
    dst_trans = dst_specification.get_base_partition().transformation
    stmt = dst_trans.make_diagnosis_widening_expr_stmt(src_trans)
    assert stmt == "floor(months_between(date_trunc('month', partition.ts_day), date('1970-01-01')))"
    result = STL.sql(f"select {stmt} as result from {partition_stmt}").take(10)[0].result
    months_expected = 673
    assert result == months_expected


@pytest.mark.integration
def test_widening_day_to_year() -> None:
    src_specification = day_partition()
    dst_specification = year_partition()
    partition_stmt = "(select named_struct('ts_day', '2026-02-12') as partition)"

    src_trans = src_specification.get_base_partition().transformation
    dst_trans = dst_specification.get_base_partition().transformation
    stmt = dst_trans.make_diagnosis_widening_expr_stmt(src_trans)
    assert stmt == "(year(partition.ts_day) - year('1970-01-01'))"
    result = STL.sql(f"select {stmt} as result from {partition_stmt}").take(10)[0].result
    years_expected = 56
    assert result == years_expected


@pytest.mark.integration
def test_widening_month_to_year() -> None:
    src_specification = month_partition()
    dst_specification = year_partition()
    partition_stmt = "(select named_struct('ts_month', 673) as partition)"

    src_trans = src_specification.get_base_partition().transformation
    dst_trans = dst_specification.get_base_partition().transformation
    stmt = dst_trans.make_diagnosis_widening_expr_stmt(src_trans)
    assert stmt == "((cast((partition.ts_month / 12) as int) + 1970) - year('1970-01-01'))"
    result = STL.sql(f"select {stmt} as result from {partition_stmt}").take(10)[0].result
    years_expected = 56
    assert result == years_expected
