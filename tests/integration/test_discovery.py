import pytest

from ice_keeper import escape_identifier
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL, Scope
from ice_keeper.table import MaintenanceSchedule
from ice_keeper.table.schedule_entry import IceKeeperTblProperty
from ice_keeper.task import DiscoveryTask
from ice_keeper.task.discovery import list_table_names_in_schema
from tests.test_common import (
    ONE_EXPECTED,
    SCOPE_SCHEMA,
    TEST_CATALOG_NAME,
    TEST_FULL_NAME,
    TWO_EXPECTED,
    get_admin_catalog_and_schema,
    set_tblproperties,
)
from tests.utils import create_empty_test_table, discover_tables


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
    create_empty_test_table(executor)
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
    create_empty_test_table(
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

    mnt_props = maintenance_schedule.get_maintenance_entry(TEST_FULL_NAME)
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
    STL.sql_and_log(f"alter table {TEST_FULL_NAME} add partition field days(ts)")

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
    table_name = "table'name\\\\with\\quote-dash"
    STL.sql(
        f"""
            create table {TEST_CATALOG_NAME}.{escape_identifier(schema)}.{escape_identifier(table_name)}
            (id int)
            using iceberg
        """,
    )
    table_names = list_table_names_in_schema(TEST_CATALOG_NAME, schema)
    assert table_name in table_names, "Should find the table name with special characters."

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
    schema = "schema'name'with\\quote-dash"
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


@pytest.mark.integration
def test_discovery_succeeds_with_bad_property_values(executor: TaskExecutor) -> None:
    """Discovery must not fail when a table has invalid or unparsable property values.

    Bad values (e.g. depth=-2, non-numeric strings for int fields) are stored as-is
    or fall back to defaults when unparsable. Validation errors are raised only at
    runtime when ice-keeper actually tries to use the values.
    """
    create_empty_test_table(
        executor,
        properties={
            # Booleans with unrecognized values (not "true"/"1"/"yes")
            IceKeeperTblProperty.SHOULD_OPTIMIZE: "maybe",
            IceKeeperTblProperty.SHOULD_EXPIRE_SNAPSHOTS: "nah",
            IceKeeperTblProperty.SHOULD_REMOVE_ORPHAN_FILES: "oui",
            IceKeeperTblProperty.SHOULD_REWRITE_MANIFEST: "2",
            IceKeeperTblProperty.SHOULD_APPLY_LIFECYCLE: "sure",
            # Ints with unparsable values
            IceKeeperTblProperty.OPTIMIZATION_GROUPING_SIZE_BYTES: "not_a_number",
            IceKeeperTblProperty.BINPACK_MIN_INPUT_FILES: "abc",
            IceKeeperTblProperty.RETENTION_DAYS_SNAPSHOTS: "",
            IceKeeperTblProperty.RETENTION_DAYS_ORPHAN_FILES: "five",
            IceKeeperTblProperty.RETENTION_NUM_SNAPSHOTS: "lots",
            IceKeeperTblProperty.OPTIMIZATION_TARGET_FILE_SIZE_BYTES: "big",
            IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE: "old",
            IceKeeperTblProperty.MAX_AGE_TO_OPTIMIZE: "very_old",
            IceKeeperTblProperty.LIFECYCLE_MAX_DAYS: "forever",
            IceKeeperTblProperty.WIDENING_RULE_MIN_AGE_TO_WIDEN: "soon",
            # Iceberg native ints with unparsable values
            IceKeeperTblProperty.HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS: "a_week",
            IceKeeperTblProperty.HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP: "many",
            IceKeeperTblProperty.WRITE_TARGET_FILE_SIZE_BYTES: "huge",
            # Float with unparsable value
            IceKeeperTblProperty.SORT_CORR_THRESHOLD: "high",
            # Int with parsable but invalid value
            IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH: "-2",
            # Strings accept anything, but set unusual values for completeness
            IceKeeperTblProperty.OPTIMIZATION_STRATEGY: "!!!invalid strategy!!!",
            IceKeeperTblProperty.NOTIFICATION_EMAIL: "not-an-email",
            IceKeeperTblProperty.MIN_PARTITION_TO_OPTIMIZE: "???",
            IceKeeperTblProperty.MAX_PARTITION_TO_OPTIMIZE: "???",
            IceKeeperTblProperty.LIFECYCLE_INGESTION_TIME_COLUMN: "",
            IceKeeperTblProperty.WIDENING_RULE_SELECT_CRITERIA: "",
            IceKeeperTblProperty.WIDENING_RULE_REQUIRED_PARTITION_COLUMNS: "",
            IceKeeperTblProperty.WIDENING_RULE_SRC_PARTITION: "",
            IceKeeperTblProperty.WIDENING_RULE_DST_PARTITION: "",
            IceKeeperTblProperty.WIDENING_RULE_MIN_PARTITION_TO_WIDEN: "???",
            IceKeeperTblProperty.WIDENING_RULE_MAX_PARTITION_TO_WIDEN: "???",
        },
    )

    # Discovery should succeed — bad values must not prevent table from being discovered
    maintenance_schedule = MaintenanceSchedule(Scope())
    tasks = DiscoveryTask.make_tasks(maintenance_schedule, SCOPE_SCHEMA)
    executor.submit_tasks_and_wait(tasks)

    mnt_props = maintenance_schedule.get_maintenance_entry(TEST_FULL_NAME)
    assert mnt_props, "Table with bad properties should still be discovered"
    mnt_props = maintenance_schedule.update_maintenance_schedule_entry(mnt_props)

    # --- Booleans: unrecognized strings are treated as False ---
    assert mnt_props.should_optimize is False
    assert mnt_props.should_expire_snapshots is False
    assert mnt_props.should_remove_orphan_files is False
    assert mnt_props.should_rewrite_manifest is False
    assert mnt_props.should_apply_lifecycle is False

    # --- Unparsable ints fall back to defaults ---
    assert mnt_props.optimization_grouping_size_bytes == 17179869184
    assert mnt_props.binpack_min_input_files == 5
    assert mnt_props.retention_days_snapshots == 7
    assert mnt_props.retention_days_orphan_files == 5
    assert mnt_props.retention_num_snapshots == 1
    assert mnt_props.target_file_size_bytes == 536870912
    assert mnt_props.min_age_to_optimize == -1
    assert mnt_props.max_age_to_optimize == -1
    assert mnt_props.lifecycle_max_days == 330
    assert mnt_props.widening_rule_min_age_to_widen == -1

    # --- Unparsable float falls back to default ---
    assert mnt_props.sort_corr_threshold == -1.0

    # --- Strings are stored as-is (any value is accepted at discovery time) ---
    assert mnt_props.optimization_strategy == "!!!invalid strategy!!!"
    assert mnt_props.notification_email == "not-an-email"
    # --- Parsable but invalid: validated only at runtime on access ---
    with pytest.raises(
        ValueError, match=r"Unsupported interval suffix '\?' in '\?\?\?'. Expected one of: \['h', 'd', 'm', 'y'\]"
    ):
        _ = mnt_props.min_partition_to_optimize
    with pytest.raises(
        ValueError, match=r"Unsupported interval suffix '\?' in '\?\?\?'. Expected one of: \['h', 'd', 'm', 'y'\]"
    ):
        _ = mnt_props.max_partition_to_optimize
    with pytest.raises(
        ValueError, match=r"Unsupported interval suffix '\?' in '\?\?\?'. Expected one of: \['h', 'd', 'm', 'y'\]"
    ):
        _ = mnt_props.widening_rule_min_partition_to_widen
    with pytest.raises(
        ValueError, match=r"Unsupported interval suffix '\?' in '\?\?\?'. Expected one of: \['h', 'd', 'm', 'y'\]"
    ):
        _ = mnt_props.widening_rule_max_partition_to_widen

    # --- Parsable but invalid: validated only at runtime on access ---
    with pytest.raises(ValueError, match="Invalid optimize_partition_depth=-2"):
        _ = mnt_props.optimize_partition_depth
