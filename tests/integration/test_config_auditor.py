import pytest

from ice_keeper import Action, IceKeeperTblProperty
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import Scope
from ice_keeper.table.schedule import MaintenanceSchedule
from ice_keeper.task.action.factory import ActionTaskFactory
from tests.test_common import (
    ONE_EXPECTED,
    SCOPE_WHERE_FULL_NAME,
    ZERO_EXPECTED,
)
from tests.utils import create_empty_test_table


@pytest.mark.integration
def test_all_valid_configs(executor: TaskExecutor) -> None:
    create_empty_test_table(
        executor=executor,
        properties={
            IceKeeperTblProperty.NOTIFICATION_EMAIL: "allo@hotmail.com",
            IceKeeperTblProperty.SHOULD_OPTIMIZE: "true",
            IceKeeperTblProperty.LIFECYCLE_MAX_DAYS: "2",
        },
    )

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    tasks = ActionTaskFactory.make_tasks(Action.CONFIG_AUDITOR, maintenance_schedule)
    assert len(tasks) == 1, "Should only find our test table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().stop().read(Scope()).collect()
    assert len(rows) == ZERO_EXPECTED


@pytest.mark.integration
def test_typo_in_ice_keeper_prefix(executor: TaskExecutor) -> None:
    create_empty_test_table(executor=executor, properties={"ice_keeper.should-apply-lifecycle": "false"})
    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    tasks = ActionTaskFactory.make_tasks(Action.CONFIG_AUDITOR, maintenance_schedule)
    assert len(tasks) == 1, "Should only find our test table"
    executor.submit_tasks_and_wait(tasks)
    journal_entries = executor.get_journal().flush().stop().read_journal_entries(Scope())
    assert len(journal_entries) == ONE_EXPECTED
