import os

import pytest

from ice_keeper import Action, IceKeeperTblProperty, Status
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import Scope
from ice_keeper.table import MaintenanceSchedule
from ice_keeper.task import ActionTaskFactory
from tests.test_common import SCOPE_TABLE, load_test_table, set_tblproperty
from tests.utils import compare_multiline_strings, create_test_table, create_test_table_with_data


@pytest.mark.integration
def test_rewrite_manifests_default_skip(executor: TaskExecutor) -> None:
    create_test_table(executor)
    maintenance_schedule = MaintenanceSchedule(SCOPE_TABLE)
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_MANIFESTS, maintenance_schedule)
    assert len(tasks) == 1, "Should only find our test table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().stop().read(Scope()).collect()
    # Then we should have the corresponding log.
    assert len(rows) == 0, "should be skipped"


@pytest.mark.integration
def test_rewrite_manifests_enabled_post_discovery(executor: TaskExecutor) -> None:
    create_test_table(executor)
    set_tblproperty(IceKeeperTblProperty.SHOULD_REWRITE_MANIFEST, "true")

    maintenance_schedule = MaintenanceSchedule(SCOPE_TABLE)
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_MANIFESTS, maintenance_schedule)
    assert len(tasks) == 1, "Should only find our test table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().stop().read(Scope()).collect()
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
    create_test_table_with_data(executor)
    set_tblproperty(IceKeeperTblProperty.SHOULD_REWRITE_MANIFEST, "true")

    table = load_test_table()
    for s in table.metadata.snapshots:
        f = s.manifest_list
        f = f.replace("file:", "")
        os.remove(f)  # noqa: PTH107

    maintenance_schedule = MaintenanceSchedule(SCOPE_TABLE)
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_MANIFESTS, maintenance_schedule)
    assert len(tasks) == 1, "Should only find our test table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().stop().read(Scope()).collect()
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
    create_test_table_with_data(executor)
    set_tblproperty(IceKeeperTblProperty.SHOULD_REWRITE_MANIFEST, "true")

    table = load_test_table()
    os.remove(table.metadata_location.replace("file:", ""))  # noqa: PTH107
    for m in table.metadata.metadata_log:
        f = m.metadata_file
        f = f.replace("file:", "")
        os.remove(f)  # noqa: PTH107

    maintenance_schedule = MaintenanceSchedule(SCOPE_TABLE)
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_MANIFESTS, maintenance_schedule)
    assert len(tasks) == 1, "Should only find our test table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().stop().read(Scope()).collect()
    # Then we should have the corresponding log.
    assert len(rows) == 1, "should have 1 log"
    assert rows[0].status == Status.FAILED.value, "Should fail"
    assert "Cannot open for reading" in rows[0].status_details, "Should fail because of files missing"
