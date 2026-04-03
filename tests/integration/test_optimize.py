import datetime
from datetime import timezone

import pytest
from click.testing import CliRunner

from ice_keeper import Action, Status
from ice_keeper.ice_keeper import diagnose
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL, Scope
from ice_keeper.table import MaintenanceSchedule
from ice_keeper.table.journal import Journal
from ice_keeper.table.schedule_entry import IceKeeperTblProperty
from ice_keeper.task import ActionTaskFactory
from tests.test_common import (
    FIVE_EXPECTED,
    ONE_EXPECTED,
    SCOPE_SCHEMA,
    SCOPE_WHERE_FULL_NAME,
    TEST_CATALOG_NAME,
    TEST_FULL_NAME,
    TEST_SCHEMA_NAME,
    TWO_EXPECTED,
    ZERO_EXPECTED,
)
from tests.utils import (
    compare_multiline_strings,
    create_empty_test_table,
    create_test_table_with_data,
    create_test_table_with_one_batch,
    discover_tables,
    insert_data,
)


@pytest.mark.integration
def test_optimize_two_partitions(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "id asc"
    properties = {
        IceKeeperTblProperty.MIN_PARTITION_TO_OPTIMIZE: "0d",
        IceKeeperTblProperty.MAX_PARTITION_TO_OPTIMIZE: "3000d",
        IceKeeperTblProperty.BINPACK_MIN_INPUT_FILES: "0",
        IceKeeperTblProperty.SORT_CORR_THRESHOLD: "2",
    }
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    create_test_table_with_one_batch(
        event_time=dt, partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties
    )
    dt = datetime.datetime(2025, 3, 3, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=1)
    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."

    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)

    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == TWO_EXPECTED, "Should have a two log"
    status0 = rows[0].status
    status_details0 = rows[0].status_details
    status1 = rows[1].status
    status_details1 = rows[1].status_details
    assert status0 == Status.SUCCESS.value
    assert status1 == Status.SUCCESS.value
    assert status_details0 == ""
    assert status_details1 == ""
    actual_output0 = rows[0].sql_stm
    actual_output1 = rows[1].sql_stm
    for actual_output in [actual_output0, actual_output1]:
        # normalize outputs
        if "2025-03-03" in actual_output:
            normalized_output = actual_output.replace("2025-03-03", "2020-01-01")
        elif "2025-12-01" in actual_output:
            normalized_output = actual_output.replace("2025-12-01", "2020-01-01")
        else:
            pytest.fail(reason="Day to optimize should one of 2025-12-01 or 2025-03-03")

        expected_output = """
            CALL local.system.rewrite_data_files(
                table => 'test.test'
                , options => map(
                    'max-concurrent-file-group-rewrites', '100',
                    'partial-progress.enabled', 'true',
                    'delete-file-threshold', '1',
                    'remove-dangling-deletes', 'true',
                    'max-file-group-size-bytes', '214748364800',
                    'target-file-size-bytes', '536870912',
                    'output-spec-id', '0',
                    'rewrite-all', 'true',
                    'min-input-files', '1',
                    'shuffle-partitions-per-file', '8')
                , strategy => 'sort'
                , where => " ( ts >= date('2020-01-01') and ts < date('2020-01-01') + interval 1 day ) "
                , sort_order => 'id asc'
            )
        """
        diff, details = compare_multiline_strings(expected_output, normalized_output)
        if diff:
            msg = (
                f"Test test_optimize_two_partitions failed. The actual output was {normalized_output}.\nDifferences are {details}"
            )
            raise Exception(msg)


@pytest.mark.integration
def test_optimize_invalid_column(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "invalid_column_name asc"
    properties = {IceKeeperTblProperty.MIN_PARTITION_TO_OPTIMIZE: "0d", IceKeeperTblProperty.MAX_PARTITION_TO_OPTIMIZE: "3000d"}
    create_test_table_with_data(executor, partitioned_by, optimization_strategy, properties)
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt)

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."

    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == ONE_EXPECTED, "Should have a one log"
    status = rows[0].status
    status_details = rows[0].status_details
    assert status == Status.FAILED.value
    assert "is configured to sort columns [invalid_column_name] but these columns do not exist." in status_details


@pytest.mark.integration
def test_optimize_binpack_min_num_files(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "binpack"
    properties = {IceKeeperTblProperty.MIN_PARTITION_TO_OPTIMIZE: "0d", IceKeeperTblProperty.MAX_PARTITION_TO_OPTIMIZE: "3000d"}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=5)
    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == ZERO_EXPECTED, "Should not binpack a single file"

    Journal.reset()
    insert_data(event_time=dt, num_inserts=1)
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == ONE_EXPECTED, "Should binpack since more than 5 files"


@pytest.mark.integration
def test_optimize_partitioned_by_category_sorted_by_id_max_age_2(executor: TaskExecutor) -> None:
    partitioned_by = "category"
    optimization_strategy = "id ASC"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "2", IceKeeperTblProperty.MAX_AGE_TO_OPTIMIZE: "200"}
    create_test_table_with_data(executor, partitioned_by, optimization_strategy, properties)
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=5)
    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == FIVE_EXPECTED, (
        "The category column is not temporal, max age should not affect it. All 5 categories should be sorted."
    )


@pytest.mark.integration
def test_optimize_partitioned_by_category_and_by_day_ts(executor: TaskExecutor) -> None:
    partitioned_by = "category, days(ts)"
    optimization_strategy = "id ASC"
    properties = {IceKeeperTblProperty.MIN_PARTITION_TO_OPTIMIZE: "0d", IceKeeperTblProperty.MAX_PARTITION_TO_OPTIMIZE: "3000d"}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=5)
    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))

    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == ONE_EXPECTED, "Should fail, so 1 journal row."
    assert rows[0].status == Status.FAILED.value
    assert "Unable to optimize table because the temporal partition is not the first one" in rows[0].status_details


@pytest.mark.integration
def test_optimize_special_partition_column(executor: TaskExecutor) -> None:
    special_column_name = "col+plus|pipe"
    sql = f"""
            create table {TEST_FULL_NAME}
            (`{special_column_name}` string, id int)
            using iceberg
            partitioned by ( truncate(8, `{special_column_name}`) )
            tblproperties (
            '{IceKeeperTblProperty.SHOULD_OPTIMIZE}'='true',
            '{IceKeeperTblProperty.OPTIMIZATION_STRATEGY}'='binpack',
            '{IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE}'='1'
            )
        """
    STL.sql_and_log(sql)

    for _i in range(6):
        STL.sql(
            f"""
                insert into {TEST_FULL_NAME}
                    select
                        'samestring' as `{special_column_name}`,
                        id as id
                    from
                        range(1, 1000)
                """,
        )

    discover_tables(executor, SCOPE_SCHEMA)
    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."

    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == ONE_EXPECTED, "Should have a one log"
    status = rows[0].status
    assert status == Status.SUCCESS.value
    assert "( local.system.truncate(8, `col+plus|pipe`) = 'samestri' )" in rows[0].sql_stm


@pytest.mark.integration
def test_optimize_special_partition_struct(executor: TaskExecutor) -> None:
    catalog = TEST_CATALOG_NAME
    schema = TEST_SCHEMA_NAME
    table_name = "test_optimize_special_partition_struct"
    full_name = f"{catalog}.{schema}.{table_name}"
    special_column_name = "col+plus|pipe.dot"
    sql = f"""
            create table {full_name}
            (base_struct struct<`{special_column_name}` string>, id int)
            using iceberg
            partitioned by ( truncate(8, base_struct.`{special_column_name}`) )
            tblproperties (
            '{IceKeeperTblProperty.SHOULD_OPTIMIZE}'='true',
            '{IceKeeperTblProperty.OPTIMIZATION_STRATEGY}'='binpack',
            '{IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE}'='1'
            )
        """
    STL.sql_and_log(sql)

    for _i in range(6):
        STL.sql(
            f"""
                insert into {full_name}
                    select
                        named_struct('{special_column_name}', 'samestring') as base_struct,
                        id as id
                    from
                        range(1, 1000)
                """,
        )

    discover_tables(executor, Scope(catalog, schema))
    maintenance_schedule = MaintenanceSchedule(Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME, table_name))
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."

    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    rows = executor.get_journal().flush().read(Scope()).collect()
    # If we have an expected procedure call.
    assert len(rows) == ONE_EXPECTED, "Should have a one log"
    status = rows[0].status
    assert status == Status.SUCCESS.value
    assert "( local.system.truncate(8, base_struct.`col+plus|pipe.dot`) = 'samestri' )" in rows[0].sql_stm


@pytest.mark.integration
def test_optimize_null_category(executor: TaskExecutor) -> None:
    # This demonstrates the limitation of ice-keeper in handling null
    # See https://github.chimera.cyber.gc.ca/CCCS/ice-keeper/issues/103
    partitioned_by = "category"
    optimization_strategy = "id ASC"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "1"}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    event_time = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)

    num_datafiles_per_category = 6
    num_category_including_null = 5
    for _i in range(num_datafiles_per_category):
        STL.sql(
            f"""
                insert into {TEST_FULL_NAME}
                    select
                        timestamp '{event_time}' as ts,
                        CAST((rand() * 4294967296) - 2147483648 AS INT) as id,
                        uuid() as name,
                        case when (id % {num_category_including_null} = 0) then NULL
                        else 'category_' || (id % {num_category_including_null})
                        end as category,
                        null as category_int,
                        named_struct('ts', timestamp '{event_time}') as submission
                    from
                        range(1, 10000)
                    """,
        )

    df = STL.sql_and_log(f"select partition, count(*) as num_files from {TEST_FULL_NAME}.data_files group by partition")
    rows = df.collect()
    print(rows)

    # Partitions to optimize in local.test.test_optimize_null_category
    # ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
    # ┃ partition_age ┃ category   ┃
    # ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
    # │ 1             │ category_4 │
    # │ 2             │ category_3 │
    # │ 3             │ category_2 │
    # │ 4             │ category_1 │
    # │ 5             │ None       │
    # └───────────────┴────────────┘

    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))
    maintenance_schedule = MaintenanceSchedule(SCOPE_WHERE_FULL_NAME)
    assert len(maintenance_schedule.entries()) == 1, "Scoped to one table, should have one maintenance entry."
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    assert len(tasks) == 1, "Should find the schedule entry for given table"
    executor.submit_tasks_and_wait(tasks)
    results = executor.get_journal().flush().read_journal_entries(Scope())
    number_of_partition_rewritten = len(results)
    assert number_of_partition_rewritten == num_category_including_null

    warnings = []
    for result in results:
        assert result.status in [Status.SUCCESS.value, Status.WARNING.value]
        if result.status == Status.WARNING.value:
            warnings.append(result)
        else:
            # Verify category_1, category_2 partitions were optimized.
            assert result.rewritten_data_files_count == num_datafiles_per_category
            assert result.added_data_files_count == 1
    assert len(warnings) == ONE_EXPECTED

    result = warnings[0]
    assert "partition field category cannot be NULL" in result.status_details
    # This shows that if we could handle optimize_partition_depth=0
    # We would be able to optimize the data in the NULL partition field.
    #
    # df = STL.sql_and_log(f"select partition, count(*) as num_files from {full_name}.data_files group by partition")
    # rows = df.collect()
    # print(rows)
    # sql = """
    #         CALL local.system.rewrite_data_files(
    #                         table => 'test.test_optimize_null_category',
    #         options => map('max-concurrent-file-group-rewrites', '100',
    #         'partial-progress.enabled', 'true',
    #         'delete-file-threshold', '1',
    #         'remove-dangling-deletes', 'true',
    #         'max-file-group-size-bytes', '214748364800',
    #         'target-file-size-bytes', '536870912',
    #         'output-spec-id', '0',
    #         'rewrite-all', 'true',
    #         'shuffle-partitions-per-file', '8'),
    #         , strategy => 'sort'
    #         sort_order => 'id ASC'
    #                     )
    # """
    # rows = STL.sql_and_log(sql).collect()
    # print(rows)

    # df = STL.sql_and_log(f"select partition, count(*) as num_files from {full_name}.data_files group by partition")
    # rows = df.collect()
    # print(rows)


@pytest.mark.integration
def test_diagnose_cli(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "id ASC"
    properties = {IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "1"}
    create_empty_test_table(partitioned_by=partitioned_by, optimization_strategy=optimization_strategy, properties=properties)
    dt = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    insert_data(event_time=dt, num_inserts=5)
    # add table to maintenance schedule
    discover_tables(executor, Scope(TEST_CATALOG_NAME, TEST_SCHEMA_NAME))
    full_name = TEST_FULL_NAME

    # Use CliRunner to invoke the diagnose command
    runner = CliRunner()
    result = runner.invoke(
        diagnose,
        [
            "--full_name",
            full_name,
            "--min_age_to_diagnose",
            "1",
            "--max_age_to_diagnose",
            "14",
            "--optimization_strategy",
            "id ASC",
            "--target_file_size_bytes",
            "-1",
        ],
    )

    # Assert the command executed successfully
    assert result.exit_code == 0, f"CliRunner failed: {result.output}"
