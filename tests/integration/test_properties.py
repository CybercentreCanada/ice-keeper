import os

import pytest
from pyiceberg.table import Table

from ice_keeper import IceKeeperTblProperty, get_user_name
from ice_keeper.pool import TaskExecutor
from ice_keeper.spec.optimization import OptimizationSpec
from ice_keeper.table.schedule_entry import DEFAULTS, MaintenanceScheduleRecord
from tests.test_common import load_test_table
from tests.utils import create_test_table


@pytest.fixture
def table(executor: TaskExecutor) -> Table:
    create_test_table(executor)
    return load_test_table()


@pytest.mark.integration
def test_sort_quoted(table: Table) -> None:
    s = OptimizationSpec.from_string("`hbs-agent-id`, other_col")
    assert s.is_sorted()
    assert s.sorted_column_names[0] == "`hbs-agent-id`"
    assert s.sorted_column_names[1] == "other_col"
    table.metadata.properties[IceKeeperTblProperty.SHOULD_OPTIMIZE] = "true"
    table.metadata.properties[IceKeeperTblProperty.OPTIMIZATION_STRATEGY] = "`hbs-agent-id`, other_col"
    mnt_props = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    assert mnt_props.optimization_strategy == "`hbs-agent-id`, other_col"


@pytest.mark.integration
def test_default_tblproperties(table: Table) -> None:
    entry = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    assert entry.max_age_to_optimize == DEFAULTS["max_age_to_optimize"]
    assert entry.last_updated_by == get_user_name()
    assert entry.optimization_strategy == DEFAULTS["optimization_strategy"]
    assert entry.retention_days_orphan_files == DEFAULTS["retention_days_orphan_files"]
    assert entry.should_remove_orphan_files, "Defaults to true"
    assert entry.should_expire_snapshots, "Defaults to true"
    assert not entry.should_rewrite_manifest, "Defaults to false"
    assert not entry.should_apply_lifecycle, "Defaults to false"
    assert not entry.should_optimize, "Defaults to false"
    assert entry.target_file_size_bytes == 536870912, "Defaults to 512MB"  # noqa: PLR2004


def test_explicit_diabled_tblproperties(table: Table) -> None:
    table.metadata.properties[IceKeeperTblProperty.SHOULD_OPTIMIZE] = "false"
    table.metadata.properties[IceKeeperTblProperty.SHOULD_EXPIRE_SNAPSHOTS] = "false"
    table.metadata.properties[IceKeeperTblProperty.SHOULD_REMOVE_ORPHAN_FILES] = "false"
    table.metadata.properties[IceKeeperTblProperty.SHOULD_REWRITE_MANIFEST] = "false"
    table.metadata.properties[IceKeeperTblProperty.SHOULD_APPLY_LIFECYCLE] = "false"
    entry = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    assert not entry.should_remove_orphan_files
    assert not entry.should_expire_snapshots
    assert not entry.should_rewrite_manifest
    assert not entry.should_apply_lifecycle
    assert not entry.should_optimize


@pytest.mark.integration
def test_explicit_enabled_tblproperties(table: Table) -> None:
    table.metadata.properties[IceKeeperTblProperty.SHOULD_OPTIMIZE] = "true"
    table.metadata.properties[IceKeeperTblProperty.SHOULD_EXPIRE_SNAPSHOTS] = "true"
    table.metadata.properties[IceKeeperTblProperty.SHOULD_REMOVE_ORPHAN_FILES] = "true"
    table.metadata.properties[IceKeeperTblProperty.SHOULD_REWRITE_MANIFEST] = "true"
    table.metadata.properties[IceKeeperTblProperty.SHOULD_APPLY_LIFECYCLE] = "true"
    entry = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    assert entry.should_remove_orphan_files
    assert entry.should_expire_snapshots
    assert entry.should_rewrite_manifest
    assert entry.should_apply_lifecycle
    assert entry.should_optimize


@pytest.mark.integration
def test_same_config_maintenance_schedule_entry(table: Table) -> None:
    entry1 = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    entry2 = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    assert entry1.record.same_config_as(entry2.record)
    # This field is not a configuration so it does not participate in the comparison.
    # The name of the user who created a maintenance entry is not considered when we compare two entries.
    os.environ["USER"] = "TEST"
    entry3 = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    assert entry1.record.same_config_as(entry3.record)
    # This field is a configuration, changing it causes the same_config_as to return False
    table.metadata.properties[IceKeeperTblProperty.MIN_AGE_TO_OPTIMIZE] = "10"
    entry4 = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    assert not entry1.record.same_config_as(entry4.record)


@pytest.mark.integration
def test_max_age_to_optimize(table: Table) -> None:
    table.metadata.properties[IceKeeperTblProperty.MAX_AGE_TO_OPTIMIZE] = "1"
    entry = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    assert entry.max_age_to_optimize == 1


@pytest.mark.integration
def test_optimization_spec() -> None:
    spec = OptimizationSpec.from_string("binpack")
    assert spec.is_binpack()
    assert not spec.is_sorted()
    spec = OptimizationSpec.from_string("col1")
    assert not spec.is_binpack()
    assert spec.is_sorted()
    spec = OptimizationSpec.from_string("col1, col2")
    assert not spec.is_binpack()
    assert spec.is_sorted()
    spec = OptimizationSpec.from_string("zorder(col1, col2)")
    assert not spec.is_binpack()
    assert spec.is_sorted()


@pytest.mark.integration
def test_should_expire_snapshots(table: Table) -> None:
    table.metadata.properties[IceKeeperTblProperty.SHOULD_EXPIRE_SNAPSHOTS] = "TRUE"
    entry = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    assert entry.should_expire_snapshots is True


@pytest.mark.integration
def test_target_file_size_bytes(table: Table) -> None:
    table.metadata.properties[IceKeeperTblProperty.WRITE_TARGET_FILE_SIZE_BYTES] = "10000"
    entry = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    assert entry.target_file_size_bytes == 10000  # noqa: PLR2004
    table.metadata.properties[IceKeeperTblProperty.WRITE_TARGET_FILE_SIZE_BYTES] = "20000"
    entry = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    assert entry.target_file_size_bytes == 20000  # noqa: PLR2004
    table.metadata.properties[IceKeeperTblProperty.OPTIMIZATION_TARGET_FILE_SIZE_BYTES] = "30000"
    table.metadata.properties[IceKeeperTblProperty.WRITE_TARGET_FILE_SIZE_BYTES] = "10000"
    entry = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    # icekeeper overrides iceberg's table config
    assert entry.target_file_size_bytes == 30000  # noqa: PLR2004


@pytest.mark.integration
def test_retention_days_snapshots(table: Table) -> None:
    expected_snapshot_retention_days = 7
    table.metadata.properties["history.expire.max-snapshot-age-ms"] = str(expected_snapshot_retention_days * 24 * 60 * 60 * 1000)
    entry = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
    assert entry.retention_days_snapshots == expected_snapshot_retention_days
