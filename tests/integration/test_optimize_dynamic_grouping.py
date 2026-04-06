import datetime

import pytest

from ice_keeper import Action, Status
from ice_keeper.ice_keeper import OptimizationStrategy
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL
from ice_keeper.table.schedule_entry import IceKeeperTblProperty
from ice_keeper.task import PartitionSummary
from ice_keeper.task.action.optimization.optimization import SubOptimizationStrategy
from ice_keeper.task.action.optimization.partition_diagnostic import PartitionDiagnosis
from tests.test_common import (
    FIVE_EXPECTED,
    ONE_EXPECTED,
    TEST_FULL_NAME,
    THREE_EXPECTED,
    TWO_EXPECTED,
)
from tests.utils import (
    compare_multiline_strings,
    create_generic_test_table,
    get_updated_mnt_props,
    run_action_and_collect_journal,
)


@pytest.mark.integration
def test_dynamic_grouping_binpack_groups_all_buckets(executor: TaskExecutor) -> None:
    """With a large grouping size, all buckets within the same age should be grouped into one PartitionDiagnosisResult."""
    dt = datetime.datetime(2025, 3, 3, 18, 33, 59, tzinfo=datetime.timezone.utc)
    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[dt],
        partitioned_by="days(ts), bucket(3, id)",
        optimization_strategy="binpack",
        properties={
            "write.delete.mode": "merge-on-read",
            IceKeeperTblProperty.MIN_PARTITION_TO_OPTIMIZE: "0d",
            IceKeeperTblProperty.MAX_PARTITION_TO_OPTIMIZE: "3000d",
            IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH: "-1",
            # Large threshold so all sub-partitions fit in one group
            IceKeeperTblProperty.OPTIMIZATION_GROUPING_SIZE_BYTES: "1073741824",
            IceKeeperTblProperty.BINPACK_MIN_INPUT_FILES: "0",
        },
    )

    mnt_props = get_updated_mnt_props()

    os = OptimizationStrategy(mnt_props)
    assert os.check_should_execute_action()

    spec_id = 0
    summary = PartitionSummary(mnt_props, spec_id, None)
    diagnosis = PartitionDiagnosis(mnt_props, spec_id)
    partition_diagnosys_results = diagnosis.find_partitions_to_optimize(summary)

    # All buckets should be in a single group
    assert len(partition_diagnosys_results) == ONE_EXPECTED
    result = partition_diagnosys_results[0]
    # Should have multiple partition filters (one per bucket)
    assert len(result.partition_filters) == THREE_EXPECTED, f"Expected 3 bucket filters, got {result.partition_filters}"

    # The WHERE clause should contain OR'd filters
    sos = SubOptimizationStrategy(result, spec_id, mnt_props, None)
    stmt = sos.prepare_statement_to_execute()
    assert " or " in stmt, f"Expected OR'd partition filters in WHERE clause, got: {stmt}"
    assert "local.system.bucket(3, id)" in stmt, f"Expected bucket filter in WHERE clause, got: {stmt}"

    rows = run_action_and_collect_journal(executor, Action.REWRITE_DATA_FILES)
    assert len(rows) == 1
    assert rows[0].status_details == ""
    assert rows[0].status == Status.SUCCESS.value
    actual_output = rows[0].sql_stm
    expected_output = """CALL local.system.rewrite_data_files(
            table => 'test.test'
            , options => map(
                'max-concurrent-file-group-rewrites', '100',
                'partial-progress.enabled', 'true',
                'delete-file-threshold', '1',
                'remove-dangling-deletes', 'true',
                'max-file-group-size-bytes', '214748364800',
                'target-file-size-bytes', '536870912',
                'output-spec-id', '0',
                'rewrite-all', 'false',
                'min-input-files', '1')
            , strategy => 'binpack'
            , where => " (( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) and ( local.system.bucket(3, id) = 0 )) or (( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) and ( local.system.bucket(3, id) = 1 )) or (( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) and ( local.system.bucket(3, id) = 2 )) "
            )
    """
    diff, details = compare_multiline_strings(expected_output, actual_output)
    if diff:
        msg = f"The actual output was {actual_output}.\nDifferences are {details}"
        raise Exception(msg)


@pytest.mark.integration
def test_dynamic_grouping_binpack_splits_into_multiple_groups(executor: TaskExecutor) -> None:
    """With a tiny grouping size, each bucket should end up in its own group."""
    dt = datetime.datetime(2025, 3, 3, 18, 33, 59, tzinfo=datetime.timezone.utc)
    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[dt],
        partitioned_by="days(ts), category",
        optimization_strategy="binpack",
        properties={
            "write.delete.mode": "merge-on-read",
            IceKeeperTblProperty.MIN_PARTITION_TO_OPTIMIZE: "0d",
            IceKeeperTblProperty.MAX_PARTITION_TO_OPTIMIZE: "3000d",
            IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH: "-1",
            # Tiny threshold so each sub-partition exceeds the limit
            IceKeeperTblProperty.OPTIMIZATION_GROUPING_SIZE_BYTES: "1",
            IceKeeperTblProperty.BINPACK_MIN_INPUT_FILES: "0",
        },
    )

    mnt_props = get_updated_mnt_props()

    os = OptimizationStrategy(mnt_props)
    assert os.check_should_execute_action()

    spec_id = 0
    summary = PartitionSummary(mnt_props, spec_id, None)
    diagnosis = PartitionDiagnosis(mnt_props, spec_id)
    partition_diagnosys_results = diagnosis.find_partitions_to_optimize(summary)

    # Each bucket should be in its own group
    assert len(partition_diagnosys_results) == FIVE_EXPECTED, (
        f"Expected 5 groups (one per category), got {len(partition_diagnosys_results)}"
    )
    for result in partition_diagnosys_results:
        assert len(result.partition_filters) == ONE_EXPECTED, f"Expected 1 filter per group, got {result.partition_filters}"
        sos = SubOptimizationStrategy(result, spec_id, mnt_props, None)
        stmt = sos.prepare_statement_to_execute()
        # Single filter should not have OR
        assert " or " not in stmt, f"Single-category group should not have OR in WHERE, got: {stmt}"

    rows = run_action_and_collect_journal(executor, Action.REWRITE_DATA_FILES)
    assert len(rows) == FIVE_EXPECTED
    for i, row in enumerate(rows):
        assert "" in row.status_details
        assert row.status == Status.SUCCESS.value, f"Row {i} failed with status {row.status} and details {row.status_details}"
        actual_output = row.sql_stm
        assert any(
            f"( ts >= date('2025-03-03') and ts < date('2025-03-03') + interval 1 day ) and ( category = 'category_{n}' )"
            in actual_output
            for n in range(5)
        ), f"Expected single-category WHERE clause, got: {actual_output}"


@pytest.mark.integration
def test_dynamic_grouping_different_days_are_separate(executor: TaskExecutor) -> None:
    """Different day partitions should always produce separate groups, regardless of grouping size."""
    dt1 = datetime.datetime(2025, 3, 3, 18, 33, 59, tzinfo=datetime.timezone.utc)
    dt2 = datetime.datetime(2025, 3, 4, 10, 0, 0, tzinfo=datetime.timezone.utc)
    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[dt1, dt2],
        partitioned_by="days(ts), bucket(3, id)",
        optimization_strategy="binpack",
        properties={
            "write.delete.mode": "merge-on-read",
            IceKeeperTblProperty.MIN_PARTITION_TO_OPTIMIZE: "0d",
            IceKeeperTblProperty.MAX_PARTITION_TO_OPTIMIZE: "3000d",
            IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH: "-1",
            # Large threshold — grouping should still not merge across days
            IceKeeperTblProperty.OPTIMIZATION_GROUPING_SIZE_BYTES: "1073741824",
            IceKeeperTblProperty.BINPACK_MIN_INPUT_FILES: "0",
        },
    )

    mnt_props = get_updated_mnt_props()
    os = OptimizationStrategy(mnt_props)
    assert os.check_should_execute_action()

    spec_id = 0
    summary = PartitionSummary(mnt_props, spec_id, None)
    diagnosis = PartitionDiagnosis(mnt_props, spec_id)
    results = diagnosis.find_partitions_to_optimize(summary)

    # Two days → two groups, each grouping all 3 buckets within that day
    assert len(results) == TWO_EXPECTED, f"Expected 2 groups (one per day), got {len(results)}"
    for result in results:
        assert len(result.partition_filters) == THREE_EXPECTED, (
            f"Expected 3 bucket filters per day group, got {result.partition_filters}"
        )

    rows = run_action_and_collect_journal(executor, Action.REWRITE_DATA_FILES)
    assert len(rows) == TWO_EXPECTED
    dates_seen = set()
    for row in rows:
        assert row.status == Status.SUCCESS.value
        if "2025-03-03" in row.sql_stm:
            dates_seen.add("2025-03-03")
        if "2025-03-04" in row.sql_stm:
            dates_seen.add("2025-03-04")
    assert dates_seen == {"2025-03-03", "2025-03-04"}, f"Expected both days in output, got dates: {dates_seen}"


@pytest.mark.integration
def test_dynamic_grouping_no_sub_partitions(executor: TaskExecutor) -> None:
    """With only days(ts) partitioning (no sub-partitions), grouping has nothing to group — one result per day."""
    dt1 = datetime.datetime(2025, 3, 3, 18, 33, 59, tzinfo=datetime.timezone.utc)
    dt2 = datetime.datetime(2025, 3, 4, 10, 0, 0, tzinfo=datetime.timezone.utc)
    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[dt1, dt2],
        partitioned_by="days(ts)",
        optimization_strategy="binpack",
        properties={
            "write.delete.mode": "merge-on-read",
            IceKeeperTblProperty.MIN_PARTITION_TO_OPTIMIZE: "0d",
            IceKeeperTblProperty.MAX_PARTITION_TO_OPTIMIZE: "3000d",
            IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH: "-1",
            IceKeeperTblProperty.OPTIMIZATION_GROUPING_SIZE_BYTES: "1073741824",
            IceKeeperTblProperty.BINPACK_MIN_INPUT_FILES: "0",
        },
    )

    mnt_props = get_updated_mnt_props()
    os = OptimizationStrategy(mnt_props)
    assert os.check_should_execute_action()

    spec_id = 0
    summary = PartitionSummary(mnt_props, spec_id, None)
    diagnosis = PartitionDiagnosis(mnt_props, spec_id)
    results = diagnosis.find_partitions_to_optimize(summary)

    # Two days, no sub-partitions → two groups with one filter each
    assert len(results) == TWO_EXPECTED, f"Expected 2 groups (one per day), got {len(results)}"
    for result in results:
        assert len(result.partition_filters) == ONE_EXPECTED, (
            f"Expected 1 filter per group (no sub-partitions), got {result.partition_filters}"
        )

    rows = run_action_and_collect_journal(executor, Action.REWRITE_DATA_FILES)
    assert len(rows) == TWO_EXPECTED
    for row in rows:
        assert row.status == Status.SUCCESS.value
        # No sub-partition filter (no " and ( category" or " and ( local.system.bucket")
        assert " and ( " not in row.sql_stm, f"Expected no sub-partition filter, got: {row.sql_stm}"


@pytest.mark.integration
def test_dynamic_grouping_large_sub_partition_alone_small_ones_grouped(executor: TaskExecutor) -> None:
    """A sub-partition exceeding the grouping threshold should be in its own group.

    Smaller sub-partitions are grouped together.
    """
    dt = datetime.datetime(2025, 3, 3, 18, 33, 59, tzinfo=datetime.timezone.utc)
    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[dt],
        partitioned_by="days(ts), category",
        optimization_strategy="binpack",
        properties={
            "write.delete.mode": "merge-on-read",
            IceKeeperTblProperty.MIN_PARTITION_TO_OPTIMIZE: "0d",
            IceKeeperTblProperty.MAX_PARTITION_TO_OPTIMIZE: "3000d",
            IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH: "-1",
            # Set threshold so one bloated category exceeds it but individual normal ones don't
            IceKeeperTblProperty.OPTIMIZATION_GROUPING_SIZE_BYTES: "100000",
            IceKeeperTblProperty.BINPACK_MIN_INPUT_FILES: "0",
        },
    )

    # Insert extra data into category_0 to make it much larger than others.
    # Each insert adds 9999 rows all in category_0, creating extra data files.
    for _ in range(5):
        STL.sql(
            f"""
            insert into {TEST_FULL_NAME}
                select
                    timestamp '{dt}' as ts,
                    CAST((rand() * 4294967296) - 2147483648 AS INT) as id,
                    uuid() as name,
                    'category_0' as category,
                    0 as category_int,
                    named_struct('ts', timestamp '{dt}') as submission
                from
                    range(1, 10000)
            """,
        )

    mnt_props = get_updated_mnt_props()
    os = OptimizationStrategy(mnt_props)
    assert os.check_should_execute_action()

    spec_id = 0
    summary = PartitionSummary(mnt_props, spec_id, None)
    diagnosis = PartitionDiagnosis(mnt_props, spec_id)
    results = diagnosis.find_partitions_to_optimize(summary)

    # category_0 is bloated and should be alone; the other 4 should be grouped.
    # Depending on threshold, we expect at least 2 groups but fewer than 5.
    assert len(results) >= TWO_EXPECTED, f"Expected at least 2 groups, got {len(results)}"
    assert len(results) < FIVE_EXPECTED, f"Expected fewer than 5 groups (small categories should be grouped), got {len(results)}"

    # Find the group containing category_0 — it should be alone
    category_0_groups = [r for r in results if any("category_0" in str(pf) for pf in r.partition_filters)]
    assert len(category_0_groups) == ONE_EXPECTED, f"Expected category_0 in exactly 1 group, got {len(category_0_groups)}"
    assert len(category_0_groups[0].partition_filters) == ONE_EXPECTED, (
        f"Expected category_0 to be alone in its group, got {category_0_groups[0].partition_filters}"
    )

    # At least one group should have multiple filters (the grouped small categories)
    multi_filter_groups = [r for r in results if len(r.partition_filters) > 1]
    assert len(multi_filter_groups) >= ONE_EXPECTED, "Expected at least one group with multiple small categories grouped together"
