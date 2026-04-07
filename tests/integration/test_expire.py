import datetime

import pytest

from ice_keeper import Action, IceKeeperTblProperty, TimeProvider
from ice_keeper.pool import TaskExecutor
from tests.utils import compare_multiline_strings, create_empty_test_table, run_action_and_collect_journal


@pytest.mark.integration
def test_expire_snapshots_default(executor: TaskExecutor) -> None:
    TimeProvider.set(datetime.datetime(2023, 3, 3, 15, 0, 0, tzinfo=datetime.timezone.utc))

    create_empty_test_table(executor)

    rows = run_action_and_collect_journal(executor, Action.EXPIRE_SNAPSHOTS)
    # Then we should have the corresponding log.
    assert len(rows) == 1, "test_expire_snapshots should have only 1 single log of the expire operation"
    row = rows[0]
    print(row, flush=True)
    actual_output = row.sql_stm
    expected_output = """
        call local.system.expire_snapshots(
                table => 'test.test',
                older_than => timestamp '2023-02-24 15:00:00+00:00',
                retain_last => 1,
                stream_results => true)
    """
    diff, details = compare_multiline_strings(expected_output, actual_output)
    if diff:
        msg = f"Test test_expire_snapshots failed. The actual output was {actual_output}.\nDifferences are {details}"
        raise Exception(msg)


@pytest.mark.integration
def test_expire_snapshots_disabled_post_discovery(executor: TaskExecutor) -> None:
    TimeProvider.set(datetime.datetime(2023, 3, 3, 15, 0, 0, tzinfo=datetime.timezone.utc))

    create_empty_test_table(executor, properties={IceKeeperTblProperty.SHOULD_EXPIRE_SNAPSHOTS: "false"})

    rows = run_action_and_collect_journal(executor, Action.EXPIRE_SNAPSHOTS)
    assert len(rows) == 0, "test_expire_snapshots should be skipped"
