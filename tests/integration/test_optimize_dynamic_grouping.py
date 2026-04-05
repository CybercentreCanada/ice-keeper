import datetime

import pytest

from ice_keeper import Action, Status
from ice_keeper.ice_keeper import OptimizationStrategy
from ice_keeper.pool import TaskExecutor
from ice_keeper.table.schedule_entry import IceKeeperTblProperty
from ice_keeper.task import PartitionSummary
from ice_keeper.task.action.optimization.optimization import SubOptimizationStrategy
from ice_keeper.task.action.optimization.partition_diagnostic import PartitionDiagnosis
from tests.integration.test_expire import run_action_and_collect_journal
from tests.test_common import (
    FIVE_EXPECTED,
    ONE_EXPECTED,
    THREE_EXPECTED,
)
from tests.utils import (
    compare_multiline_strings,
    create_generic_test_table,
    get_updated_mnt_props,
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
    assert "" in rows[0].status_details
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
