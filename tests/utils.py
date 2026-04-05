import datetime
import difflib
import re

from pyspark.sql import Row

from ice_keeper import Action, IceKeeperTblProperty, TimeProvider
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL, Scope
from ice_keeper.table import MaintenanceSchedule, MaintenanceScheduleEntry
from ice_keeper.task import ActionTaskFactory, DiscoveryTask
from tests.test_common import SCOPE_WHERE_FULL_NAME, TEST_CATALOG_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME


def create_generic_test_table(
    executor: TaskExecutor,
    partitions_to_insert_into: list[datetime.datetime],
    catalog: str = TEST_CATALOG_NAME,
    schema: str = TEST_SCHEMA_NAME,
    table_name: str = TEST_TABLE_NAME,
    partitioned_by: str | None = None,
    optimization_strategy: str | None = None,
    properties: dict[str, str] | None = None,
    num_inserts: int = 1,
) -> None:
    partition_by_stm = ""
    if partitioned_by:
        partition_by_stm = f"partitioned by ({partitioned_by})"

    props = {}
    if optimization_strategy:
        props[IceKeeperTblProperty.SHOULD_OPTIMIZE] = "true"
        props[IceKeeperTblProperty.OPTIMIZATION_STRATEGY] = optimization_strategy
    total_props = props
    if properties:
        total_props = total_props | properties

    tblproperties_stm = ""
    if total_props:
        props_stm = ", ".join(f"'{key}' = '{value}'" for key, value in total_props.items())
        tblproperties_stm = f"tblproperties ({props_stm})"

    full_name = f"{catalog}.{schema}.{table_name}"
    if num_inserts == 0:
        STL.sql(
            f"""
                create table {full_name}
                (ts timestamp, id int, name string, category string, category_int int, submission struct<ts timestamp>)
                using iceberg
                {partition_by_stm}
                {tblproperties_stm}
            """,
        )
    else:
        values_rows = ", ".join(f"(timestamp '{dt}')" for dt in partitions_to_insert_into)
        STL.sql(
            f"""
                create table {full_name}
                using iceberg
                {partition_by_stm}
                {tblproperties_stm}
                as (
                    select /*+ repartition(1) */
                        event_time as ts,
                        CAST((rand() * 4294967296) - 2147483648 AS INT) as id,
                        uuid() as name,
                        'category_' || (id % 5) as category,
                        (id % 5) as category_int,
                        named_struct('ts', event_time) as submission
                    from
                        (select id from range(1, 10000))
                    cross join
                        (select * from values {values_rows} as t(event_time)) times
                )
            """,
        )
        if num_inserts > 1:
            insert_data(
                catalog=catalog,
                schema=schema,
                table_name=table_name,
                partitions_to_insert_into=partitions_to_insert_into,
                num_inserts=(num_inserts - 1),
            )
    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))
    # discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))


def create_empty_test_table(
    executor: TaskExecutor,
    catalog: str = TEST_CATALOG_NAME,
    schema: str = TEST_SCHEMA_NAME,
    table_name: str = TEST_TABLE_NAME,
    partitioned_by: str | None = None,
    optimization_strategy: str | None = None,
    properties: dict[str, str] | None = None,
) -> None:
    create_generic_test_table(
        executor=executor,
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        partitions_to_insert_into=[],
        partitioned_by=partitioned_by,
        optimization_strategy=optimization_strategy,
        properties=properties,
        num_inserts=0,
    )


def get_updated_mnt_props() -> MaintenanceScheduleEntry:
    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    mnt_props = maintenance_schedule.entries()[0]
    assert mnt_props
    return maintenance_schedule.update_maintenance_schedule_entry(mnt_props)


def discover_tables(executor: TaskExecutor, scope: Scope) -> None:
    maintenance_schedule = MaintenanceSchedule(scope)
    tasks = DiscoveryTask.make_tasks(maintenance_schedule, scope)
    executor.submit_tasks_and_wait(tasks)


def run_action_and_collect_journal(
    executor: TaskExecutor,
    action: Action,
    scope: Scope = SCOPE_WHERE_FULL_NAME,
) -> list[Row]:
    maintenance_schedule = MaintenanceSchedule(scope)
    tasks = ActionTaskFactory.make_tasks(action, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    return executor.get_journal().flush().stop().read(Scope()).collect()


def insert_data(
    catalog: str = TEST_CATALOG_NAME,
    schema: str = TEST_SCHEMA_NAME,
    table_name: str = TEST_TABLE_NAME,
    partitions_to_insert_into: list[datetime.datetime] = [TimeProvider.current_datetime()],  # noqa: B006, B008
    num_inserts: int = 20,
) -> None:
    """Inserts synthetic data into the specified table.

    Each execution of the SQL statement generates 10,000 rows, and the statement is executed
    `num_inserts` times, resulting in a total of `num_inserts * 10,000` rows inserted.

    The generated rows have the following structure:
        - ts (timestamp): The provided event_time, constant across a single insert.
        - id (int): A randomly generated 32-bit signed integer.
        - name (string): A unique UUID string.
        - category (string): A string derived from `id % 5`, resulting in values
          like 'category_0', 'category_1', ..., 'category_4'.
        - submission (struct): A named struct containing the provided event_time as `ts`.
    """
    full_name = f"{catalog}.{schema}.{table_name}"
    values_rows = ", ".join(f"(timestamp '{dt}')" for dt in partitions_to_insert_into)
    for _i in range(num_inserts):
        STL.sql(
            f"""
                insert into {full_name}
                    select
                        event_time as ts,
                        CAST((rand() * 4294967296) - 2147483648 AS INT) as id,
                        uuid() as name,
                        'category_' || (id % 5) as category,
                        (id % 5) as category_int,
                        named_struct('ts', event_time) as submission
                    from
                        (select id from range(1, 10000))
                    cross join
                        (select * from values {values_rows} as t(event_time)) times
                """,
        )


def normalize_whitespace(text: str) -> str:
    """Remove extra whitespace and normalize spaces within a string."""
    return re.sub(r"\s+", " ", text).strip()


def compare_multiline_strings(expected: str, actual: str) -> tuple[bool, str | None]:
    """Compare two multiline strings while ignoring whitespace differences."""
    expected_lines = [normalize_whitespace(line) for line in expected.splitlines()]
    actual_lines = [normalize_whitespace(line) for line in actual.splitlines()]

    expected_lines = [line for line in expected_lines if len(line) > 0]
    actual_lines = [line for line in actual_lines if len(line) > 0]

    diff = list(difflib.unified_diff(expected_lines, actual_lines, lineterm="", fromfile="Expected", tofile="Actual"))
    if diff:
        return (True, "\n".join(diff))
    return (False, None)
