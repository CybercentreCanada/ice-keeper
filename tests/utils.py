import datetime
import difflib
import re

from ice_keeper import IceKeeperTblProperty, TimeProvider
from ice_keeper.config import Config
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL, Scope
from ice_keeper.table import MaintenanceSchedule
from ice_keeper.task import DiscoveryTask
from tests.test_common import SCOPE_SCHEMA, TEST_CATALOG_NAME, TEST_FULL_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME


def create_test_table_with_one_batch(
    event_time: datetime.datetime,
    catalog: str = TEST_CATALOG_NAME,
    schema: str = TEST_SCHEMA_NAME,
    table_name: str = TEST_TABLE_NAME,
    partitioned_by: str | None = None,
    optimization_strategy: str | None = None,
    properties: dict[str, str] = {},  # noqa: B006
) -> None:
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
            using iceberg
            {partition_by_stm}
            {tblproperties_stm}
            as (
                select
                    timestamp '{event_time}' as ts,
                    CAST((rand() * 4294967296) - 2147483648 AS INT) as id,
                    uuid() as name,
                    'category_' || (id % 5) as category,
                    (id % 5) as category_int,
                    named_struct('ts', timestamp '{event_time}') as submission
                from
                    range(1, 10000)
            )
        """,
    )


def create_empty_test_table(
    catalog: str = TEST_CATALOG_NAME,
    schema: str = TEST_SCHEMA_NAME,
    table_name: str = TEST_TABLE_NAME,
    partitioned_by: str | None = None,
    optimization_strategy: str | None = None,
    properties: dict[str, str] = {},  # noqa: B006
) -> None:
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


def discover_tables(executor: TaskExecutor, scope: Scope) -> None:
    maintenance_schedule = MaintenanceSchedule(scope)
    tasks = DiscoveryTask.make_tasks(maintenance_schedule, scope)
    executor.submit_tasks_and_wait(tasks)


def create_test_table(
    executor: TaskExecutor,
    partitioned_by: str | None = None,
    optimization_strategy: str | None = None,
    properties: dict[str, str] = {},  # noqa: B006
) -> str:
    return _create_test_table(executor, partitioned_by, optimization_strategy, properties)


def _create_test_table(
    executor: TaskExecutor,
    partitioned_by: str | None = None,
    optimization_strategy: str | None = None,
    properties: dict[str, str] = {},  # noqa: B006
) -> str:
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    # add table to maintenance schedule
    discover_tables(executor, SCOPE_SCHEMA)
    return TEST_FULL_NAME


def insert_data(
    table_name: str = TEST_TABLE_NAME,
    event_time: datetime.datetime = TimeProvider.current_datetime(),  # noqa: B008
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
    catalog = TEST_CATALOG_NAME
    schema = TEST_SCHEMA_NAME
    full_name = f"{catalog}.{schema}.{table_name}"
    for _i in range(num_inserts):
        STL.sql(
            f"""
                insert into {full_name}
                    select
                        timestamp '{event_time}' as ts,
                        CAST((rand() * 4294967296) - 2147483648 AS INT) as id,
                        uuid() as name,
                        'category_' || (id % 5) as category,
                        (id % 5) as category_int,
                        named_struct('ts', timestamp '{event_time}') as submission
                    from
                        range(1, 10000)
                """,
        )


def create_test_table_with_data(
    executor: TaskExecutor,
    partitioned_by: str | None = None,
    optimization_strategy: str | None = None,
    properties: dict[str, str] = {},  # noqa: B006
) -> str:
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    # use a fixed timestamp
    dt = datetime.datetime(2025, 3, 3, 18, 33, 59, tzinfo=datetime.timezone.utc)
    insert_data(event_time=dt, num_inserts=8)
    # add table to maintenance schedule
    discover_tables(executor, SCOPE_SCHEMA)
    return TEST_FULL_NAME


def delete_test_tables() -> None:
    maintenance_schedule_table_name = Config.instance().maintenance_schedule_table_name
    partition_health_table_name = Config.instance().partition_health_table_name
    journal_table_name = Config.instance().journal_table_name
    catalog = maintenance_schedule_table_name.split(".")[0]
    schema = maintenance_schedule_table_name.split(".")[1]
    rows = STL.get().sql(f"show tables in {catalog}.{schema}").collect()
    for row in rows:
        full_name = f"{catalog}.{schema}.{row.tableName}"
        if full_name not in [maintenance_schedule_table_name, partition_health_table_name, journal_table_name]:
            print(f"deleting test table named {full_name}")
            STL.get().sql(f"drop table {full_name} purge")


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
