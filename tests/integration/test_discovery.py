import pytest

from ice_keeper import escape_identifier
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL, Scope
from ice_keeper.table import MaintenanceSchedule
from ice_keeper.table.schedule_entry import IceKeeperTblProperty
from ice_keeper.task import DiscoveryTask
from tests.test_common import (
    ONE_EXPECTED,
    SCOPE_SCHEMA,
    TEST_CATALOG_NAME,
    TWO_EXPECTED,
    get_admin_catalog_and_schema,
    set_tblproperties,
)
from tests.utils import create_test_table, discover_tables


@pytest.mark.integration
def test_discovery_of_admin_schema(executor: TaskExecutor) -> None:
    catalog, schema = get_admin_catalog_and_schema()
    maintenance_schedule = MaintenanceSchedule(Scope())
    tasks = DiscoveryTask.make_tasks(maintenance_schedule, Scope(catalog, schema))
    executor.submit_tasks_and_wait(tasks)

    table_names = [entry.table_name for entry in maintenance_schedule.entries()]
    assert "maintenance_schedule" in table_names, "Should have maintenance_schedule"
    assert "journal" in table_names, "Should have journal"
    assert "partition_health" in table_names, "Should have partition_health"


@pytest.mark.integration
def test_discovery_of_local_catalog(executor: TaskExecutor) -> None:
    maintenance_schedule = MaintenanceSchedule(Scope())
    tasks = DiscoveryTask.make_tasks(maintenance_schedule, Scope(TEST_CATALOG_NAME))
    executor.submit_tasks_and_wait(tasks)

    table_names = [entry.table_name for entry in maintenance_schedule.entries()]
    assert "maintenance_schedule" in table_names, "Should have maintenance_schedule"
    assert "journal" in table_names, "Should have journal"
    assert "partition_health" in table_names, "Should have partition_health"


@pytest.mark.integration
def test_discovery_of_local_catalog_multi_schemas(executor: TaskExecutor) -> None:
    create_test_table(executor)
    maintenance_schedule = MaintenanceSchedule(Scope())
    tasks = DiscoveryTask.make_tasks(maintenance_schedule, Scope(TEST_CATALOG_NAME))
    executor.submit_tasks_and_wait(tasks)

    full_names = [entry.full_name for entry in maintenance_schedule.entries()]
    assert "local.admin_test.maintenance_schedule" in full_names, "Should have maintenance_schedule"
    assert "local.admin_test.journal" in full_names, "Should have journal"
    assert "local.admin_test.partition_health" in full_names, "Should have partition_health"
    assert "local.test.test" in full_names, "Should have table just created"


@pytest.mark.integration
def test_discovery_changed_properties(executor: TaskExecutor) -> None:
    initial_snap_history = 7
    full_name = create_test_table(
        executor,
        properties={
            IceKeeperTblProperty.SHOULD_EXPIRE_SNAPSHOTS: "true",
            IceKeeperTblProperty.SHOULD_REMOVE_ORPHAN_FILES: "true",
            "history.expire.max-snapshot-age-ms": str(initial_snap_history * 24 * 60 * 60 * 1000),
            IceKeeperTblProperty.OPTIMIZATION_STRATEGY: "binpack",
        },
    )
    maintenance_schedule = MaintenanceSchedule(Scope())
    tasks = DiscoveryTask.make_tasks(maintenance_schedule, SCOPE_SCHEMA)
    executor.submit_tasks_and_wait(tasks)

    mnt_props = maintenance_schedule.get_maintenance_entry(full_name)
    assert mnt_props, "Should find it"
    mnt_props = maintenance_schedule.update_maintenance_schedule_entry(mnt_props)
    assert len(mnt_props.partition_specs.get_specifications()) == ONE_EXPECTED

    assert not mnt_props.partition_specs[0].is_partitioned
    assert mnt_props.should_expire_snapshots is True
    assert mnt_props.should_remove_orphan_files is True
    assert mnt_props.retention_days_snapshots == initial_snap_history
    assert mnt_props.optimization_strategy == "binpack"
    assert mnt_props.should_optimize is False

    # now let's simulate running the discovery process and updating an existing entry
    # first step is reading above entry
    # then using properties found on the table now to update the schedule
    expected_snapshot_retention_days = 5
    set_tblproperties(
        {
            IceKeeperTblProperty.SHOULD_EXPIRE_SNAPSHOTS: "false",
            "history.expire.max-snapshot-age-ms": str(expected_snapshot_retention_days * 24 * 60 * 60 * 1000),
            IceKeeperTblProperty.OPTIMIZATION_STRATEGY: "id ASC NULLS LAST",
            IceKeeperTblProperty.SHOULD_OPTIMIZE: "true",
        }
    )
    STL.sql_and_log(f"alter table {full_name} add partition field days(ts)")

    new_entry = maintenance_schedule.update_maintenance_schedule_entry(mnt_props, force=True)

    assert len(new_entry.partition_specs.get_specifications()) == TWO_EXPECTED
    assert new_entry.partition_specs[1].is_partitioned
    assert len(new_entry.partition_specs[1].partition_list) == ONE_EXPECTED
    assert new_entry.partition_specs[1].partition_list[0].transformation.source_field_path_escaped == "ts"
    assert new_entry.partition_specs[1].partition_list[0].transformation.partition_field_escaped == "ts_day"
    assert new_entry.should_expire_snapshots is False
    assert new_entry.should_remove_orphan_files is True
    assert new_entry.retention_days_snapshots == expected_snapshot_retention_days
    assert new_entry.optimization_strategy == "id ASC NULLS LAST"
    assert new_entry.should_optimize is True


@pytest.mark.integration
def test_discovery_of_table_name_with_special_characters(executor: TaskExecutor) -> None:
    schema = "test1"
    table_name = "table'name'with'quote"
    STL.sql(
        f"""
            create table {TEST_CATALOG_NAME}.{escape_identifier(schema)}.{escape_identifier(table_name)}
            (id int)
            using iceberg
        """,
    )
    discover_tables(executor, Scope(TEST_CATALOG_NAME))
    maintenance_schedule = MaintenanceSchedule(Scope())
    schemas = maintenance_schedule.list_schemas_in_catalog(TEST_CATALOG_NAME)  # to load schemas in cache
    assert schema in schemas
    table_names = maintenance_schedule.list_table_names_in_schema(TEST_CATALOG_NAME, schema)  # to load tables in cache
    assert table_name in table_names

    maintenance_schedule = MaintenanceSchedule(Scope(TEST_CATALOG_NAME, schema, table_name))
    entries = maintenance_schedule.entries()
    assert len(entries) == ONE_EXPECTED
    assert entries[0].catalog == TEST_CATALOG_NAME
    assert entries[0].schema == schema
    assert entries[0].table_name == table_name


@pytest.mark.integration
def test_discovery_of_schema_with_special_characters(executor: TaskExecutor) -> None:
    schema = "schema'name'with'quote"
    table_name = "table1"
    STL.sql(
        f"""
            create table {TEST_CATALOG_NAME}.{escape_identifier(schema)}.{escape_identifier(table_name)}
            (id int)
            using iceberg
        """,
    )
    discover_tables(executor, Scope(TEST_CATALOG_NAME))
    maintenance_schedule = MaintenanceSchedule(Scope())
    schemas = maintenance_schedule.list_schemas_in_catalog(TEST_CATALOG_NAME)  # to load schemas in cache
    assert schema in schemas
    table_names = maintenance_schedule.list_table_names_in_schema(TEST_CATALOG_NAME, schema)  # to load tables in cache
    assert table_name in table_names

    maintenance_schedule = MaintenanceSchedule(Scope(TEST_CATALOG_NAME, schema, table_name))
    entries = maintenance_schedule.entries()
    assert len(entries) == ONE_EXPECTED
    assert entries[0].catalog == TEST_CATALOG_NAME
    assert entries[0].schema == schema
    assert entries[0].table_name == table_name
