import datetime
import os

import pytest

from ice_keeper import Action, IceKeeperTblProperty, Status
from ice_keeper.pool import TaskExecutor
from tests.test_common import SCOPE_TABLE, load_test_table
from tests.utils import (
    compare_multiline_strings,
    create_empty_test_table,
    create_generic_test_table,
    run_action_and_collect_journal,
)


@pytest.mark.integration
def test_rewrite_manifests_default_skip(executor: TaskExecutor) -> None:
    create_empty_test_table(executor)
    rows = run_action_and_collect_journal(executor, Action.REWRITE_MANIFESTS, scope=SCOPE_TABLE)
    # Then we should have the corresponding log.
    assert len(rows) == 0, "should be skipped"


@pytest.mark.integration
def test_rewrite_manifests_enabled_post_discovery(executor: TaskExecutor) -> None:
    create_empty_test_table(executor, properties={IceKeeperTblProperty.SHOULD_REWRITE_MANIFEST: "true"})

    rows = run_action_and_collect_journal(executor, Action.REWRITE_MANIFESTS, scope=SCOPE_TABLE)
    # Then we should have the corresponding log.
    assert len(rows) == 1, "should have 1 log"
    actual_output = rows[0].sql_stm
    expected_output = """
        call local.system.rewrite_manifests('test.test')
    """
    diff, details = compare_multiline_strings(expected_output, actual_output)
    if diff:
        msg = f"Test test_expire_snapshots failed. The actual output was {actual_output}.\nDifferences are {details}"
        raise Exception(msg)


@pytest.mark.integration
def test_rewrite_manifests_missing_snapshots(executor: TaskExecutor) -> None:
    dt = datetime.datetime(2025, 3, 3, 18, 33, 59, tzinfo=datetime.timezone.utc)
    create_generic_test_table(
        executor=executor, partitions_to_insert_into=[dt], properties={IceKeeperTblProperty.SHOULD_REWRITE_MANIFEST: "true"}
    )

    table = load_test_table()
    for s in table.metadata.snapshots:
        f = s.manifest_list
        f = f.replace("file:", "")
        os.remove(f)  # noqa: PTH107

    rows = run_action_and_collect_journal(executor, Action.REWRITE_MANIFESTS, scope=SCOPE_TABLE)
    # Then we should have the corresponding log.
    assert len(rows) == 1, "should have 1 log"
    actual_output = rows[0].sql_stm
    expected_output = """
        call local.system.rewrite_manifests('test.test')
    """
    diff, details = compare_multiline_strings(expected_output, actual_output)
    if diff:
        msg = f"Test test_expire_snapshots failed. The actual output was {actual_output}.\nDifferences are {details}"
        raise Exception(msg)

    assert rows[0].status == Status.FAILED.value, "Should fail"
    assert "org.apache.iceberg.exceptions.NotFoundException" in rows[0].status_details, "Should fail because of files missing"


@pytest.mark.integration
def test_rewrite_manifests_missing_metadata(executor: TaskExecutor) -> None:
    dt = datetime.datetime(2025, 3, 3, 18, 33, 59, tzinfo=datetime.timezone.utc)
    create_generic_test_table(
        executor=executor, partitions_to_insert_into=[dt], properties={IceKeeperTblProperty.SHOULD_REWRITE_MANIFEST: "true"}
    )

    table = load_test_table()
    os.remove(table.metadata_location.replace("file:", ""))  # noqa: PTH107
    for m in table.metadata.metadata_log:
        f = m.metadata_file
        f = f.replace("file:", "")
        os.remove(f)  # noqa: PTH107

    rows = run_action_and_collect_journal(executor, Action.REWRITE_MANIFESTS, scope=SCOPE_TABLE)
    # Then we should have the corresponding log.
    assert len(rows) == 1, "should have 1 log"
    assert rows[0].status == Status.FAILED.value, "Should fail"
    assert "Cannot open for reading" in rows[0].status_details, "Should fail because of files missing"
