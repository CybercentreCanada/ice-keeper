import pytest

from ice_keeper import Action, IceKeeperTblProperty
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL, Scope
from ice_keeper.table.schedule import MaintenanceSchedule
from ice_keeper.task.action.factory import ActionTaskFactory
from tests.test_common import (
    ONE_EXPECTED,
    SCOPE_SCHEMA,
    SCOPE_WHERE_FULL_NAME,
    TEST_CATALOG_NAME,
    TEST_SCHEMA_NAME,
    TEST_TABLE_NAME,
    ZERO_EXPECTED,
)
from tests.utils import discover_tables


def create_empty_test_table(
    catalog: str = TEST_CATALOG_NAME,
    schema: str = TEST_SCHEMA_NAME,
    table_name: str = TEST_TABLE_NAME,
    partitioned_by: str | None = None,
    optimization_strategy: str | None = None,
    properties: dict[str, str] = {},  # noqa: B006
) -> None:
    STL.sql(f"drop table if exists {catalog}.{schema}.{table_name}")
    partition_by_stm = ""
    if partitioned_by:
        partition_by_stm = f"partitioned by ({partitioned_by})"

    props = {}
    if optimization_strategy:
        props[IceKeeperTblProperty.SHOULD_OPTIMIZE] = "true"
        props[IceKeeperTblProperty.OPTIMIZATION_STRATEGY] = optimization_strategy
    total_props = props | properties

    tblproperties_stm = ""
    if total_props:
        props_stm = ", ".join(f"'{key}' = '{value}'" for key, value in total_props.items())
        tblproperties_stm = f"tblproperties ({props_stm})"
    STL.sql(
        f"""
            create table {catalog}.{schema}.{table_name}
            (ts timestamp, id int, name string, category string, category_int int, submission struct<ts timestamp>)
            using iceberg
            {partition_by_stm}
            {tblproperties_stm}
        """,
    )


@pytest.mark.integration
def test_all_valid_configs(executor: TaskExecutor) -> None:
    create_empty_test_table(
        properties={
            IceKeeperTblProperty.NOTIFICATION_EMAIL: "allo@hotmail.com",
            IceKeeperTblProperty.SHOULD_OPTIMIZE: "true",
            IceKeeperTblProperty.LIFECYCLE_MAX_DAYS: "2",
        }
    )
    discover_tables(executor, SCOPE_SCHEMA)
    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    tasks = ActionTaskFactory.make_tasks(Action.CONFIG_AUDITOR, maintenance_schedule)
    assert len(tasks) == 1, "Should only find our test table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().stop().read(Scope()).collect()
    assert len(rows) == ZERO_EXPECTED


@pytest.mark.integration
def test_typo_in_ice_keeper_prefix(executor: TaskExecutor) -> None:
    create_empty_test_table(properties={"ice_keeper.should-apply-lifecycle": "false"})
    discover_tables(executor, SCOPE_SCHEMA)
    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    tasks = ActionTaskFactory.make_tasks(Action.CONFIG_AUDITOR, maintenance_schedule)
    assert len(tasks) == 1, "Should only find our test table"
    executor.submit_tasks_and_wait(tasks)
    journal_entries = executor.get_journal().flush().stop().read_journal_entries(Scope())
    assert len(journal_entries) == ONE_EXPECTED
