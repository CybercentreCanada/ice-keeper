import datetime
from pathlib import Path

import pytest

from ice_keeper import Action, IceKeeperTblProperty, TimeProvider
from ice_keeper.pool import TaskExecutor
from tests.test_common import load_test_table
from tests.utils import (
    compare_multiline_strings,
    create_empty_test_table,
    create_generic_test_table,
    run_action_and_collect_journal,
)


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
                stream_results => true,
                clean_expired_metadata => true)
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


@pytest.mark.integration
def test_expire_fast_snapshots_trims_metadata_only(executor: TaskExecutor) -> None:
    # Move retention cutoff far into the future so all historical snapshots are eligible.
    TimeProvider.set(datetime.datetime(2123, 3, 3, 15, 0, 0, tzinfo=datetime.timezone.utc))

    create_generic_test_table(
        executor,
        partitions_to_insert_into=[datetime.datetime(2023, 3, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)],
        num_inserts=4,
    )

    table_before = load_test_table()
    snapshots_before = len(table_before.metadata.snapshots)
    assert snapshots_before >= 2, "Expected multiple snapshots before fast expiration"

    table_location = table_before.location().replace("file://", "")
    data_files_before = sorted(str(path) for path in Path(table_location).glob("**/*.parquet"))

    rows = run_action_and_collect_journal(executor, Action.EXPIRE_FAST_SNAPSHOTS)
    assert len(rows) == 1, "Should have one journal entry for expire_fast"
    assert "pyiceberg.expire_snapshots(" in rows[0].sql_stm

    table_after = load_test_table()
    snapshots_after = len(table_after.metadata.snapshots)
    assert snapshots_after == 1, "Fast expiration should retain only the latest snapshot by default"

    data_files_after = sorted(str(path) for path in Path(table_location).glob("**/*.parquet"))
    assert data_files_after == data_files_before, "Fast expiration should not delete parquet data files"


@pytest.mark.integration
def test_expire_fast_snapshots_disabled_post_discovery(executor: TaskExecutor) -> None:
    TimeProvider.set(datetime.datetime(2023, 3, 3, 15, 0, 0, tzinfo=datetime.timezone.utc))

    create_empty_test_table(executor, properties={IceKeeperTblProperty.SHOULD_EXPIRE_SNAPSHOTS: "false"})

    rows = run_action_and_collect_journal(executor, Action.EXPIRE_FAST_SNAPSHOTS)
    assert len(rows) == 0, "test_expire_fast_snapshots should be skipped"
