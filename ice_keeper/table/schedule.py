import logging
import threading
from pathlib import Path
from urllib.parse import urlparse

from ice_keeper import ActionFailed, get_user_name, quote_literal_value
from ice_keeper.catalog import load_table
from ice_keeper.config import Config, TemplateName
from ice_keeper.output import print_df_to_console_vertical
from ice_keeper.stm import STL, Scope

from .schedule_entry import MaintenanceScheduleEntry, MaintenanceScheduleRecord

logger = logging.getLogger("ice-keeper")


class MaintenanceSchedule:
    maintenance_table_lock = threading.RLock()

    def __init__(self, scope: Scope) -> None:
        self.scope = scope
        self.is_cached = False
        self.maintenance_entry_map: dict[str, MaintenanceScheduleEntry] = {}
        self.maintenance_entry_tblproperties_sync_done: dict[str, bool] = {}

    @classmethod
    def reset(cls) -> None:
        with cls.maintenance_table_lock:
            full_name = Config.instance().maintenance_schedule_table_name
            STL.sql_and_log(f"drop table if exists {full_name}", "Dropping maintenance schedule table")
            ddl = MaintenanceScheduleRecord.get_ddl()
            # Ensures the base ends with a slash so the name is appended correctly
            table_location = Config.instance().get_table_location(full_name)
            admin_table_notification_email = Config.instance().admin_table_notification_email
            template = Config.instance().load_template(TemplateName.SCHEDULE)
            sql = template.render(
                full_name=full_name,
                ddl=ddl,
                notification_email=admin_table_notification_email,
                location=table_location,
            )
            STL.sql_and_log(sql)

    def _check_init(self) -> None:
        with self.maintenance_table_lock:
            if not self.is_cached:
                entries = self._read_entries()
                for entry in entries:
                    self.maintenance_entry_map[entry.full_name] = entry
                self.is_cached = True

    def get_maintenance_entry(self, full_name: str) -> MaintenanceScheduleEntry | None:
        self._check_init()
        return self.maintenance_entry_map.get(full_name)

    def list_table_names_in_schema(self, catalog: str, schema: str) -> set[str]:
        """List all table names in a given catalog and schema in the maintenance schedule."""
        self._check_init()
        return {
            entry.table_name
            for entry in self.maintenance_entry_map.values()
            if (entry.catalog == catalog) and (entry.schema == schema)
        }

    def list_schemas_in_catalog(self, catalog: str) -> set[str]:
        """List all schemas in a given catalog in the maintenance schedule."""
        self._check_init()
        return {entry.schema for entry in self.maintenance_entry_map.values() if (entry.catalog == catalog)}

    def entries(self) -> list[MaintenanceScheduleEntry]:
        self._check_init()
        return list(self.maintenance_entry_map.values())

    def _read_entries(self) -> list[MaintenanceScheduleEntry]:
        sql = self.read_sql()
        rows = STL.sql_and_log(sql, "Reading maintenance schedule entries").collect()
        return [MaintenanceScheduleRecord.from_row(row).to_entry() for row in rows]

    def read_sql(self) -> str:
        sql_filter = self.scope.make_scoping_stmt()
        return f"""
            select
                *
            from
                {Config.instance().maintenance_schedule_table_name}
            where
                {sql_filter}
        """

    def show(self) -> None:
        sql = self.read_sql()
        drop_df = STL.sql(sql, "Showing maintenance schedule entries").drop("catalog").drop("schema").drop("table_name")
        print_df_to_console_vertical(drop_df, n=1000000, truncate=150)

    def update(
        self,
        scope: Scope,
        set_clause: str | None = None,
    ) -> None:
        sql_filter = scope.make_scoping_stmt()
        set_clause_with_user = f"{set_clause}, last_updated_by='{get_user_name()}'"
        sql = f"""
            update {Config.instance().maintenance_schedule_table_name}
            set {set_clause_with_user}
            where {sql_filter}
        """
        with self.maintenance_table_lock:
            df = STL.sql_and_log(sql)
            print_df_to_console_vertical(df, n=3, truncate=150)

    def _table_names_to_in_stmt(self, table_names: set[str]) -> str:
        """Create an IN statement list with properly escaped SQL string literals."""
        literals = [quote_literal_value(table_name) for table_name in table_names]
        return "(" + ", ".join(literals) + ")"

    def _schemas_to_in_stmt(self, schemas: set[str]) -> str:
        """Create an IN statement list with properly escaped SQL string literals."""
        literals = [quote_literal_value(schema) for schema in schemas]
        return "(" + ", ".join(literals) + ")"

    def delete_table_names(self, catalog: str, schema: str, table_names: set[str]) -> None:
        """Delete all entries matching the given catalog, schema and table names."""
        assert table_names, "Should not call with empty set."
        logger.debug("Removing table_names from the maintenance schedule.")
        table_names_in_str = self._table_names_to_in_stmt(table_names)
        scope_stmt = Scope(catalog=catalog, schema=schema).make_scoping_stmt()
        sql = f"""
            delete from {Config.instance().maintenance_schedule_table_name}
            where
                {scope_stmt}
                and table_name in {table_names_in_str}
            """
        with self.maintenance_table_lock:
            STL.sql_and_log(sql)

    def delete_schemas(self, catalog: str, schemas: set[str]) -> None:
        """Delete all entries matching the given catalog and schemas."""
        assert schemas, "Should not call with empty set."
        logger.debug("Removing schemas from the maintenance schedule.")
        schemas_in_str = self._schemas_to_in_stmt(schemas)
        scope_stmt = Scope(catalog=catalog).make_scoping_stmt()
        sql = f"""
            delete from {Config.instance().maintenance_schedule_table_name}
            where
                {scope_stmt}
                and schema in {schemas_in_str}
            """
        with self.maintenance_table_lock:
            STL.sql_and_log(sql)

    def merge_an_entry(self, maintenance_entry: MaintenanceScheduleEntry) -> None:
        """Merge a single maintenance entry into the schedule.

        If an entry for the same table already exists, it will be updated,
        otherwise it will be inserted.
        """
        self.merge_entries({maintenance_entry})

    def merge_entries(self, maintenance_entries: set[MaintenanceScheduleEntry]) -> None:
        """Merge multiple maintenance entries into the schedule."""
        assert maintenance_entries, "Should not call with empty set."
        with self.maintenance_table_lock:
            rows = [entry.record.to_row() for entry in maintenance_entries]
            rows_df = STL.get().createDataFrame(rows, MaintenanceScheduleRecord.get_ddl())
            rows_df.createOrReplaceTempView("new_maintenance_entries")
            sql = f"""
                merge into {Config.instance().maintenance_schedule_table_name} t
                using ( select * from new_maintenance_entries ) s
                on (
                    t.catalog = s.catalog
                    and t.schema = s.schema
                    and t.table_name = s.table_name
                )
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            STL.sql_and_log(sql, "Merging maintenance entries")
            for mnt_props in maintenance_entries:
                self.maintenance_entry_map[mnt_props.full_name] = mnt_props
                self.maintenance_entry_tblproperties_sync_done[mnt_props.full_name] = True

    def update_maintenance_schedule_entry(
        self, mnt_props: MaintenanceScheduleEntry, *, force: bool = False
    ) -> MaintenanceScheduleEntry:
        updated_props = mnt_props
        sync_done = self.maintenance_entry_tblproperties_sync_done.get(mnt_props.full_name)
        if not sync_done or force:
            # this table is already in the maintenance schedule, update this entry with the table's properties.
            table = load_table(mnt_props.catalog, mnt_props.schema, mnt_props.table_name)
            configured_entry = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
            if not configured_entry.record.same_config_as(mnt_props.record):
                logger.info("updating maintenance entry of table %s", mnt_props.full_name)
                self.merge_an_entry(configured_entry)
                # Return the updated schedule entry
                updated_props = configured_entry

            self.maintenance_entry_tblproperties_sync_done[mnt_props.full_name] = True
        return updated_props

    def check_table_location_is_unique(self, full_name: str, table_location: str) -> None:
        """Checks no table share the same location and raises and Exception if it is not.

        The overview metrics table holds the location of all tables.
        We use it to search for other Iceberg tables that might be sharing the
        same location on disk. We not only check for exact matching location
        but also for sub-folders under this table's location.
        """
        parsed_uri = urlparse(table_location)
        # Retrieve the path and netloc (without scheme)
        clean_path = Path(f"{parsed_uri.netloc}{parsed_uri.path}")
        scheme = parsed_uri.scheme

        # >> clean_path = '/tmp/pytest-of-unknown/pytest-340/iceberg-0/test'
        # >> Path(clean_path).parents
        # '/tmp/pytest-of-unknown/pytest-340/iceberg-0/test'
        # '/tmp/pytest-of-unknown/pytest-340/iceberg-0'
        # '/tmp/pytest-of-unknown/pytest-340'
        # '/tmp/pytest-of-unknown'
        # '/tmp'

        # Get all sub-paths leading up to the path
        sub_paths = [str(clean_path.parents[i]) for i in range(len(clean_path.parents) - 1)]
        parent_dirs = [f"{scheme}://{sub_path}" for sub_path in sub_paths]

        # Default to checking no other table has exact same location.
        other_tables_with_same_parent_location_stmt = "table_location = '{table_location}'"
        if len(parent_dirs) > 0:
            # If our table has parent directories, then make sure none of those locations are used by other tables
            other_tables_locations = parent_dirs
            other_tables_locations.append(table_location)
            other_tables_with_same_parent_location_stmt = f"table_location in {tuple(other_tables_locations)!s}"

        other_tables_with_location_under_our_own_stmt = f"startswith(table_location, '{table_location}/')"

        sql = f"""
            select
                distinct
                full_name,
                table_location
            from
                {Config.instance().maintenance_schedule_table_name}
            where
                full_name <> '{full_name}'
                and (
                    {other_tables_with_location_under_our_own_stmt}
                    or {other_tables_with_same_parent_location_stmt}
                )
            """
        rows = STL.sql_and_log(sql, "Assert table location is unique").collect()
        if len(rows) > 0:
            other_tables = {row.full_name for row in rows}
            other_locations = {row.table_location for row in rows}
            msg = (
                f"Skipped remove_orphan_files because the table location [{table_location}] is shared"
                f"with following locations: [{other_locations}] of the following tables: [{other_tables}]."
            )
            raise ActionFailed(msg)
