import datetime

import pytest

from ice_keeper import Action, IceKeeperTblProperty, Status
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL
from tests.test_common import TEST_FULL_NAME
from tests.utils import create_generic_test_table, run_action_and_collect_journal

# create_generic_test_table inserts 9999 rows per partition
ROWS_PER_PARTITION = 9999


def create_lifecycle_test_table(executor: TaskExecutor, properties: dict[str, str] = {}) -> None:  # noqa: B006
    now = datetime.datetime.now(datetime.timezone.utc)
    partitions = [now - datetime.timedelta(days=i) for i in range(4, 0, -1)]
    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=partitions,
        partitioned_by="days(ts)",
        properties=properties,
    )


@pytest.mark.integration
def test_lifecycle_default_behaviour(executor: TaskExecutor) -> None:
    create_lifecycle_test_table(executor)
    # Test that default behaviour is to not run lifecycle
    rows = run_action_and_collect_journal(executor, Action.LIFECYCLE_DATA)
    # The default is should lifecycle = False
    assert len(rows) == 0


@pytest.mark.integration
def test_lifecycle_disabled_explicitly(executor: TaskExecutor) -> None:
    create_lifecycle_test_table(executor, {IceKeeperTblProperty.SHOULD_APPLY_LIFECYCLE: "false"})
    # Test that default behaviour is to not run lifecycle
    rows = run_action_and_collect_journal(executor, Action.LIFECYCLE_DATA)
    # Explicitly should lifecycle = False
    assert len(rows) == 0


@pytest.mark.integration
def test_lifecycle_column_not_specified(executor: TaskExecutor) -> None:
    create_lifecycle_test_table(executor, {IceKeeperTblProperty.SHOULD_APPLY_LIFECYCLE: "true"})
    rows = run_action_and_collect_journal(executor, Action.LIFECYCLE_DATA)
    assert len(rows) == 1
    assert rows[0].status == Status.FAILED.value
    assert "[] but this column does not exist." in rows[0].status_details


@pytest.mark.integration
def test_lifecycle_column_does_not_exist(executor: TaskExecutor) -> None:
    create_lifecycle_test_table(
        executor,
        {
            IceKeeperTblProperty.SHOULD_APPLY_LIFECYCLE: "true",
            IceKeeperTblProperty.LIFECYCLE_INGESTION_TIME_COLUMN: "column_xyz",
        },
    )

    rows = run_action_and_collect_journal(executor, Action.LIFECYCLE_DATA)
    assert len(rows) == 1
    assert rows[0].status == Status.FAILED.value
    assert "[column_xyz] but this column does not exist." in rows[0].status_details


@pytest.mark.integration
def test_lifecycle_default_num_days(executor: TaskExecutor) -> None:
    create_lifecycle_test_table(
        executor,
        {IceKeeperTblProperty.SHOULD_APPLY_LIFECYCLE: "true", IceKeeperTblProperty.LIFECYCLE_INGESTION_TIME_COLUMN: "ts"},
    )
    row = STL.sql(f"select count(*) as num_files, sum(record_count) as num_rows from {TEST_FULL_NAME}.data_files").take(1)[0]
    num_data_files_before = row.num_files
    num_rows_before = row.num_rows
    num_data_files_before = STL.sql(f"select * from {TEST_FULL_NAME}.data_files").count()

    rows = run_action_and_collect_journal(executor, Action.LIFECYCLE_DATA)
    assert len(rows) == 1
    assert rows[0].status == Status.SUCCESS.value
    assert "where ts < current_date() - interval '330' days" in rows[0].sql_stm
    assert rows[0].lifecycle_deleted_data_files == 0
    assert rows[0].lifecycle_deleted_records == 0
    assert rows[0].lifecycle_changed_partition_count == 0
    row = STL.sql(f"select count(*) as num_files, sum(record_count) as num_rows from {TEST_FULL_NAME}.data_files").take(1)[0]
    num_data_files_after = row.num_files
    num_rows_after = row.num_rows
    assert num_rows_before == num_rows_after
    assert num_data_files_before == num_data_files_after


@pytest.mark.integration
def test_lifecycle_delete_past_one_day(executor: TaskExecutor) -> None:
    create_lifecycle_test_table(
        executor,
        {
            IceKeeperTblProperty.SHOULD_APPLY_LIFECYCLE: "true",
            IceKeeperTblProperty.LIFECYCLE_INGESTION_TIME_COLUMN: "ts",
            IceKeeperTblProperty.LIFECYCLE_MAX_DAYS: "1",
        },
    )

    rows = run_action_and_collect_journal(executor, Action.LIFECYCLE_DATA)
    assert len(rows) == 1
    assert rows[0].status == Status.SUCCESS.value
    assert "where ts < current_date() - interval '1' days" in rows[0].sql_stm
    expected_num_days_deleted = 3
    assert rows[0].lifecycle_deleted_data_files == expected_num_days_deleted
    assert rows[0].lifecycle_deleted_records == expected_num_days_deleted * ROWS_PER_PARTITION
    assert rows[0].lifecycle_changed_partition_count == expected_num_days_deleted


@pytest.mark.integration
def test_lifecycle_delete_past_two_days(executor: TaskExecutor) -> None:
    create_lifecycle_test_table(
        executor,
        {
            IceKeeperTblProperty.SHOULD_APPLY_LIFECYCLE: "true",
            IceKeeperTblProperty.LIFECYCLE_INGESTION_TIME_COLUMN: "ts",
            IceKeeperTblProperty.LIFECYCLE_MAX_DAYS: "2",
        },
    )

    rows = run_action_and_collect_journal(executor, Action.LIFECYCLE_DATA)
    assert len(rows) == 1
    assert rows[0].status == Status.SUCCESS.value
    assert "where ts < current_date() - interval '2' days" in rows[0].sql_stm
    expected_num_days_deleted = 2
    assert rows[0].lifecycle_deleted_data_files == expected_num_days_deleted
    assert rows[0].lifecycle_deleted_records == expected_num_days_deleted * ROWS_PER_PARTITION
    assert rows[0].lifecycle_changed_partition_count == expected_num_days_deleted


@pytest.mark.integration
def test_lifecycle_using_none_time_column(executor: TaskExecutor) -> None:
    create_lifecycle_test_table(
        executor,
        {IceKeeperTblProperty.SHOULD_APPLY_LIFECYCLE: "true", IceKeeperTblProperty.LIFECYCLE_INGESTION_TIME_COLUMN: "id"},
    )

    rows = run_action_and_collect_journal(executor, Action.LIFECYCLE_DATA)
    assert len(rows) == 1
    assert rows[0].status == Status.FAILED.value
    assert "left and right operands of the binary operator have incompatible types" in rows[0].status_details
