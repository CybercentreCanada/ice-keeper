"""Unit tests for ice_keeper/task/discovery.py.

The two module-level helpers that hit Spark are mocked throughout:
  - list_table_names_in_schema
  - list_schemas_in_catalog

The MaintenanceSchedule is always a MagicMock whose relevant methods are
configured per test.  No real Spark, catalog, or database is required.
"""

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    # This import only runs during type checking (e.g., by MyPy or Pyright)
    from ice_keeper.table import MaintenanceScheduleEntry

from ice_keeper.task.discovery import DiscoveryTask, PruneDeletedSchemasTask

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODULE: str = "ice_keeper.task.discovery"
CATALOG: str = "my_catalog"
SCHEMA: str = "my_schema"

# Table / schema names that contain characters which require backtick-escaping
# in Spark SQL but must pass through Python set operations unmodified.
SPECIAL_TABLE_NAMES: list[str] = [
    "table-with-dashes",
    "table,with,commas",
    "table'with'single'quotes",
    'table"with"double"quotes',
    "table`with`backticks",
    "table with spaces",
    # Dots are the catalog.schema.table separator — the trickiest case.
    "table.with.dots",
]

SPECIAL_SCHEMA_NAMES: list[str] = [
    "schema-with-dashes",
    "schema'single'quoted",
    'schema"double"quoted',
    "schema with spaces",
    "schema.with.dots",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_schedule() -> MagicMock:
    """Return a fresh MagicMock that stands in for MaintenanceSchedule."""
    return MagicMock()


# ---------------------------------------------------------------------------
# PruneDeletedSchemasTask.prune_schemas_from_catalog
# ---------------------------------------------------------------------------


class TestPruneSchemasFromCatalog:
    @patch(f"{MODULE}.list_schemas_in_catalog")
    def test_removes_schemas_no_longer_in_catalog(self, mock_list_schemas: MagicMock) -> None:
        """Schemas in the schedule that are absent from the catalog are deleted."""
        mock_list_schemas.return_value = {"live_schema"}

        schedule = make_schedule()
        schedule.list_schemas_in_catalog.return_value = {"live_schema", "gone_schema"}

        task = PruneDeletedSchemasTask(schedule, CATALOG)
        task.prune_schemas_from_catalog()

        mock_list_schemas.assert_called_once_with(CATALOG)
        schedule.delete_schemas.assert_called_once_with(CATALOG, {"gone_schema"})

    @patch(f"{MODULE}.list_schemas_in_catalog")
    def test_no_deletion_when_all_schemas_present(self, mock_list_schemas: MagicMock) -> None:
        """Nothing is deleted when every scheduled schema still exists."""
        mock_list_schemas.return_value = {"schema_a", "schema_b"}

        schedule = make_schedule()
        schedule.list_schemas_in_catalog.return_value = {"schema_a", "schema_b"}

        task = PruneDeletedSchemasTask(schedule, CATALOG)
        task.prune_schemas_from_catalog()

        schedule.delete_schemas.assert_not_called()

    @patch(f"{MODULE}.list_schemas_in_catalog")
    def test_removes_all_schemas_when_catalog_is_empty(self, mock_list_schemas: MagicMock) -> None:
        """All scheduled schemas are pruned when the catalog returns nothing."""
        mock_list_schemas.return_value = set()

        schedule = make_schedule()
        schedule.list_schemas_in_catalog.return_value = {"orphan_a", "orphan_b"}

        task = PruneDeletedSchemasTask(schedule, CATALOG)
        task.prune_schemas_from_catalog()

        schedule.delete_schemas.assert_called_once_with(CATALOG, {"orphan_a", "orphan_b"})

    @patch(f"{MODULE}.list_schemas_in_catalog")
    def test_no_deletion_when_schedule_is_empty(self, mock_list_schemas: MagicMock) -> None:
        """No deletion is attempted when the maintenance schedule has no entries."""
        mock_list_schemas.return_value = {"schema_a"}

        schedule = make_schedule()
        schedule.list_schemas_in_catalog.return_value = set()

        task = PruneDeletedSchemasTask(schedule, CATALOG)
        task.prune_schemas_from_catalog()

        schedule.delete_schemas.assert_not_called()

    @pytest.mark.parametrize("schema_name", SPECIAL_SCHEMA_NAMES)
    @patch(f"{MODULE}.list_schemas_in_catalog")
    def test_removes_deleted_schema_with_special_chars(self, mock_list_schemas: MagicMock, schema_name: str) -> None:
        """A schema whose name contains special characters is pruned correctly
        via plain Python set subtraction — no SQL escaping involved.
        """  # noqa: D205
        mock_list_schemas.return_value = set()  # catalog has no schemas

        schedule = make_schedule()
        schedule.list_schemas_in_catalog.return_value = {schema_name}

        task = PruneDeletedSchemasTask(schedule, CATALOG)
        task.prune_schemas_from_catalog()

        schedule.delete_schemas.assert_called_once_with(CATALOG, {schema_name})

    @pytest.mark.parametrize("schema_name", SPECIAL_SCHEMA_NAMES)
    @patch(f"{MODULE}.list_schemas_in_catalog")
    def test_retains_live_schema_with_special_chars(self, mock_list_schemas: MagicMock, schema_name: str) -> None:
        """A schema with special characters that still exists in the catalog is
        not passed to delete_schemas.
        """  # noqa: D205
        mock_list_schemas.return_value = {schema_name}

        schedule = make_schedule()
        schedule.list_schemas_in_catalog.return_value = {schema_name}

        task = PruneDeletedSchemasTask(schedule, CATALOG)
        task.prune_schemas_from_catalog()

        schedule.delete_schemas.assert_not_called()


# ---------------------------------------------------------------------------
# DiscoveryTask.sync_schema_found_in_catalog
# ---------------------------------------------------------------------------


class TestSyncSchemaFoundInCatalog:
    @patch(f"{MODULE}.load_table")
    @patch(f"{MODULE}.MaintenanceScheduleRecord")
    @patch(f"{MODULE}.list_table_names_in_schema")
    def test_adds_new_table_not_in_schedule(
        self,
        mock_list_tables: MagicMock,
        mock_msr: MagicMock,
        mock_load_table: MagicMock,
    ) -> None:
        """A table present in the catalog but absent from the schedule is added."""
        mock_list_tables.return_value = {"new_table"}

        schedule = make_schedule()
        schedule.list_table_names_in_schema.return_value = set()

        fake_entry = MagicMock()
        mock_msr.from_iceberg_table.return_value.to_entry.return_value = fake_entry

        task = DiscoveryTask(schedule, CATALOG, SCHEMA)
        task.sync_schema_found_in_catalog()

        mock_list_tables.assert_called_once_with(CATALOG, SCHEMA)
        mock_load_table.assert_called_once_with(CATALOG, SCHEMA, "new_table")
        schedule.merge_entries.assert_called_once_with({fake_entry})
        schedule.delete_table_names.assert_not_called()

    @patch(f"{MODULE}.load_table")
    @patch(f"{MODULE}.list_table_names_in_schema")
    def test_removes_table_deleted_from_catalog(
        self,
        mock_list_tables: MagicMock,
        mock_load_table: MagicMock,
    ) -> None:
        """A table tracked in the schedule that no longer exists in the catalog is removed."""
        mock_list_tables.return_value = set()  # catalog is empty

        schedule = make_schedule()
        schedule.list_table_names_in_schema.return_value = {"stale_table"}

        task = DiscoveryTask(schedule, CATALOG, SCHEMA)
        task.sync_schema_found_in_catalog()

        schedule.delete_table_names.assert_called_once_with(CATALOG, SCHEMA, {"stale_table"})
        schedule.merge_entries.assert_not_called()
        mock_load_table.assert_not_called()

    @patch(f"{MODULE}.load_table")
    @patch(f"{MODULE}.list_table_names_in_schema")
    def test_skips_table_already_in_schedule(
        self,
        mock_list_tables: MagicMock,
        mock_load_table: MagicMock,
    ) -> None:
        """A table already tracked is neither re-added nor removed."""
        mock_list_tables.return_value = {"existing_table"}

        schedule = make_schedule()
        schedule.list_table_names_in_schema.return_value = {"existing_table"}

        task = DiscoveryTask(schedule, CATALOG, SCHEMA)
        task.sync_schema_found_in_catalog()

        mock_load_table.assert_not_called()
        schedule.delete_table_names.assert_not_called()
        schedule.merge_entries.assert_not_called()

    @patch(f"{MODULE}.load_table")
    @patch(f"{MODULE}.MaintenanceScheduleRecord")
    @patch(f"{MODULE}.list_table_names_in_schema")
    def test_mixed_add_and_remove(
        self,
        mock_list_tables: MagicMock,
        mock_msr: MagicMock,
        mock_load_table: MagicMock,
    ) -> None:
        """Simultaneous additions and removals are both handled correctly."""
        mock_list_tables.return_value = {"new_table", "existing_table"}

        schedule = make_schedule()
        schedule.list_table_names_in_schema.return_value = {"stale_table", "existing_table"}

        fake_entry = MagicMock()
        mock_msr.from_iceberg_table.return_value.to_entry.return_value = fake_entry

        task = DiscoveryTask(schedule, CATALOG, SCHEMA)
        task.sync_schema_found_in_catalog()

        schedule.delete_table_names.assert_called_once_with(CATALOG, SCHEMA, {"stale_table"})
        mock_load_table.assert_called_once_with(CATALOG, SCHEMA, "new_table")
        schedule.merge_entries.assert_called_once_with({fake_entry})

    @patch(f"{MODULE}.load_table")
    @patch(f"{MODULE}.MaintenanceScheduleRecord")
    @patch(f"{MODULE}.list_table_names_in_schema")
    def test_continues_after_load_table_failure(
        self,
        mock_list_tables: MagicMock,
        mock_msr: MagicMock,
        mock_load_table: MagicMock,
    ) -> None:
        """A failure loading one table is caught; the remaining table is still merged.
        Only the successfully loaded table's entry ends up in the merge call.
        """  # noqa: D205
        mock_list_tables.return_value = {"bad_table", "good_table"}

        schedule = make_schedule()
        schedule.list_table_names_in_schema.return_value = set()

        good_entry = MagicMock()
        mock_msr.from_iceberg_table.return_value.to_entry.return_value = good_entry

        def load_side_effect(catalog: str, schema: str, table_name: str) -> MagicMock:  # noqa: ARG001
            if table_name == "bad_table":
                msg = "Spark exploded"
                raise RuntimeError(msg)
            return MagicMock()

        mock_load_table.side_effect = load_side_effect

        task = DiscoveryTask(schedule, CATALOG, SCHEMA)
        task.sync_schema_found_in_catalog()

        assert mock_load_table.call_count == 2
        merged: set[MaintenanceScheduleEntry] = schedule.merge_entries.call_args[0][0]
        assert len(merged) == 1

    @patch(f"{MODULE}.load_table")
    @patch(f"{MODULE}.list_table_names_in_schema")
    def test_empty_catalog_and_empty_schedule(
        self,
        mock_list_tables: MagicMock,
        mock_load_table: MagicMock,
    ) -> None:
        """When both catalog and schedule are empty, no loads or deletes occur."""
        mock_list_tables.return_value = set()

        schedule = make_schedule()
        schedule.list_table_names_in_schema.return_value = set()

        task = DiscoveryTask(schedule, CATALOG, SCHEMA)
        task.sync_schema_found_in_catalog()

        mock_load_table.assert_not_called()
        schedule.delete_table_names.assert_not_called()
        schedule.merge_entries.assert_not_called()


# ---------------------------------------------------------------------------
# Special characters in table names
# ---------------------------------------------------------------------------


class TestSpecialCharsInTableNames:
    """Characters that need backtick-escaping in Spark SQL must survive Python
    set operations and be forwarded to load_table / delete_table_names verbatim.
    """  # noqa: D205

    @pytest.mark.parametrize("table_name", SPECIAL_TABLE_NAMES)
    @patch(f"{MODULE}.load_table")
    @patch(f"{MODULE}.MaintenanceScheduleRecord")
    @patch(f"{MODULE}.list_table_names_in_schema")
    def test_new_table_with_special_chars_is_added(
        self,
        mock_list_tables: MagicMock,
        mock_msr: MagicMock,
        mock_load_table: MagicMock,
        table_name: str,
    ) -> None:
        """load_table receives the raw table name with special characters intact."""
        mock_list_tables.return_value = {table_name}

        schedule = make_schedule()
        schedule.list_table_names_in_schema.return_value = set()

        fake_entry = MagicMock()
        mock_msr.from_iceberg_table.return_value.to_entry.return_value = fake_entry

        task = DiscoveryTask(schedule, CATALOG, SCHEMA)
        task.sync_schema_found_in_catalog()

        mock_load_table.assert_called_once_with(CATALOG, SCHEMA, table_name)
        schedule.merge_entries.assert_called_once_with({fake_entry})

    @pytest.mark.parametrize("table_name", SPECIAL_TABLE_NAMES)
    @patch(f"{MODULE}.load_table")
    @patch(f"{MODULE}.list_table_names_in_schema")
    def test_stale_table_with_special_chars_is_removed(
        self,
        mock_list_tables: MagicMock,
        mock_load_table: MagicMock,
        table_name: str,
    ) -> None:
        """delete_table_names receives the exact name including special characters."""
        mock_list_tables.return_value = set()  # gone from catalog

        schedule = make_schedule()
        schedule.list_table_names_in_schema.return_value = {table_name}

        task = DiscoveryTask(schedule, CATALOG, SCHEMA)
        task.sync_schema_found_in_catalog()

        schedule.delete_table_names.assert_called_once_with(CATALOG, SCHEMA, {table_name})
        mock_load_table.assert_not_called()

    @pytest.mark.parametrize("table_name", SPECIAL_TABLE_NAMES)
    @patch(f"{MODULE}.load_table")
    @patch(f"{MODULE}.list_table_names_in_schema")
    def test_existing_table_with_special_chars_is_not_touched(
        self,
        mock_list_tables: MagicMock,
        mock_load_table: MagicMock,
        table_name: str,
    ) -> None:
        """A table already in the schedule is recognised via set equality even
        when its name contains special characters, so it is left untouched.
        """  # noqa: D205
        mock_list_tables.return_value = {table_name}

        schedule = make_schedule()
        schedule.list_table_names_in_schema.return_value = {table_name}

        task = DiscoveryTask(schedule, CATALOG, SCHEMA)
        task.sync_schema_found_in_catalog()

        mock_load_table.assert_not_called()
        schedule.delete_table_names.assert_not_called()
        schedule.merge_entries.assert_not_called()
