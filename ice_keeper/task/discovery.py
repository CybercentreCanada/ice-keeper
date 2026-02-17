import logging

from ice_keeper.catalog import load_table
from ice_keeper.stm import STL, Scope
from ice_keeper.table import MaintenanceSchedule, MaintenanceScheduleRecord

from .task import SparkTask, SubTaskExecutor, Task, TaskResult

logger = logging.getLogger("ice-keeper")


class PruneDeletedSchemasTask(Task):
    def __init__(self, maintenance_schedule: MaintenanceSchedule, catalog: str) -> None:
        self.maintenance_schedule = maintenance_schedule
        self.catalog = catalog

    def task_name(self) -> str:
        return self.catalog

    def task_description(self) -> str:
        return f"Pruning tables from {self.catalog}"

    @classmethod
    def make_task(cls, maintenance_schedule: MaintenanceSchedule, catalog: str) -> Task:
        return SparkTask(cls(maintenance_schedule, catalog))

    def execute(self, sub_executor: SubTaskExecutor) -> TaskResult:  # noqa: ARG002
        schemas_in_schedule = self.find_schema_in_maintenance_schedule()
        schemas_in_catalog = self.find_schema_in_catalog()
        schemas_to_remove = [schema for schema in schemas_in_schedule if schema not in schemas_in_catalog]

        full_names_to_remove = [
            full_name for schema in schemas_to_remove for full_name in self.maintenance_schedule.full_names(self.catalog, schema)
        ]

        self.maintenance_schedule.remove(full_names_to_remove)
        return TaskResult()

    def find_schema_in_maintenance_schedule(self) -> set[str]:
        current_entries = self.maintenance_schedule.full_names(self.catalog)
        return {full_name.split(".")[1] for full_name in current_entries}

    def find_schema_in_catalog(self) -> list[str]:
        sql = f"show schemas in {self.catalog}"
        rows = STL.sql(sql, sql).collect()
        return [row.namespace for row in rows]


class DiscoveryTask(Task):
    """Each discovery task process one schema at a time."""

    def __init__(self, maintenance_schedule: MaintenanceSchedule, catalog: str, schema: str) -> None:
        self.maintenance_schedule = maintenance_schedule
        self.catalog = catalog
        self.schema = schema

    def task_name(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def task_description(self) -> str:
        return f"Discovering tables in [{self.catalog}.{self.schema}]"

    @classmethod
    def get_or_use_schemas(cls, catalog: str, schema: str | None) -> list[str]:
        if schema:
            return [schema]
        sql = f"show schemas in {catalog}"
        rows = STL.sql(sql, sql).collect()
        return [row.namespace for row in rows]

    @classmethod
    def make_tasks(cls, maintenance_schedule: MaintenanceSchedule, scope: Scope) -> list[Task]:
        tasks: list[Task] = []
        assert scope.catalog, "Must be specified"
        assert not scope.table_name, "Unsuported"
        assert not scope.where, "Unsuported"
        if scope.schema:
            tasks = [SparkTask(cls(maintenance_schedule, scope.catalog, scope.schema))]
        else:
            schemas = cls.get_or_use_schemas(scope.catalog, scope.schema)
            tasks = [SparkTask(cls(maintenance_schedule, scope.catalog, schema)) for schema in schemas]
        prune_schemas = PruneDeletedSchemasTask.make_task(maintenance_schedule, scope.catalog)
        tasks.append(prune_schemas)
        return tasks

    def execute(self, sub_executor: SubTaskExecutor) -> TaskResult:  # noqa: ARG002
        self.discover_schema()
        return TaskResult()

    def discover_schema(self) -> None:
        full_names_in_catalog = self.find_tables_in_catalog()
        current_entries = self.maintenance_schedule.full_names(self.catalog, self.schema)
        self.prune_deleted_tables(full_names_in_catalog, current_entries)
        self.add_newly_created_tables(full_names_in_catalog, current_entries)

    def find_tables_in_catalog(self) -> list[str]:
        sql = f"show tables in {self.catalog}.{self.schema}"
        rows = STL.sql(sql, sql).collect()
        return [
            f"{self.catalog}.{self.schema}.{row.tableName}"
            for row in rows
            if not row.tableName.endswith("__dbt_tmp")  # Ignore the tmp tables produced by CMT's dbt jobs
        ]

    def prune_deleted_tables(self, full_names_in_catalog: list[str], current_entries: list[str]) -> None:
        full_names_to_remove = [full_name for full_name in current_entries if full_name not in full_names_in_catalog]
        self.maintenance_schedule.remove(full_names_to_remove)

    def add_newly_created_tables(self, full_names_in_catalog: list[str], current_entries: list[str]) -> None:
        maintenance_entries_to_update = []
        # full name of table with no quotes, don't use in sql statement.
        for full_name in full_names_in_catalog:
            try:
                if full_name not in current_entries:
                    table_name = full_name.replace(f"{self.catalog}.{self.schema}.", "")
                    logger.debug("adding a new maintenance entry for table %s", full_name)
                    table = load_table(self.catalog, self.schema, table_name)
                    entry = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
                    maintenance_entries_to_update.append(entry)

            except Exception as e:
                logger.exception("Failed to load table %s: %s", full_name, e)

        self.maintenance_schedule.merge(maintenance_entries_to_update)
