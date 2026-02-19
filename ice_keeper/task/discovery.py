import logging

from ice_keeper import escape_identifier
from ice_keeper.catalog import load_table
from ice_keeper.stm import STL, Scope
from ice_keeper.table import MaintenanceSchedule, MaintenanceScheduleEntry, MaintenanceScheduleRecord

from .task import SparkTask, SubTaskExecutor, Task, TaskResult

logger = logging.getLogger("ice-keeper")


def list_table_names_in_schema(catalog: str, schema: str) -> set[str]:
    """List all table names in a given schema within a catalog.

    Args:
        catalog (str): The catalog name.
        schema (str): The schema name.

    Returns:
        set[str]: A set of table names in the schema.
    """
    sql = f"show tables in {escape_identifier(catalog)}.{escape_identifier(schema)}"
    rows = STL.sql(sql, sql).collect()
    return {row.tableName for row in rows if not row.tableName.endswith("__dbt_tmp")}  # Ignore dbt temp tables.


def remove_backticks_from_schema(schema: str) -> str:
    """Spark strangely returns schema names with backticks.

    We remove them to be consistent with the way listing table names work
    and with the fact that internally we keep the names of tables and schemas without backticks.
    """
    if schema.startswith("`") and schema.endswith("`"):
        return schema[1:-1]
    return schema


def list_schemas_in_catalog(catalog: str) -> set[str]:
    """List all schemas in a given catalog.

    Args:
        catalog (str): The catalog name.

    Returns:
        set[str]: A set of schema names in the catalog.
    """
    sql = f"show schemas in {escape_identifier(catalog)}"
    rows = STL.sql(sql, sql).collect()
    return {remove_backticks_from_schema(row.namespace) for row in rows}


class PruneDeletedSchemasTask(Task):
    """Task to remove schemas that no longer exist in the catalog.

    This task compares the schemas in the catalog with those in the maintenance schedule
    and removes any schemas from the schedule that are no longer present in the catalog.

    Attributes:
        maintenance_schedule (MaintenanceSchedule): The maintenance schedule to update.
        catalog (str): The catalog name.
    """

    def __init__(self, maintenance_schedule: MaintenanceSchedule, catalog: str) -> None:
        self.maintenance_schedule = maintenance_schedule
        self.catalog = catalog

    def task_name(self) -> str:
        return self.catalog

    def task_description(self) -> str:
        return f"Pruning schemas from {self.catalog}"

    @classmethod
    def make_task(cls, maintenance_schedule: MaintenanceSchedule, catalog: str) -> Task:
        return SparkTask(cls(maintenance_schedule, catalog))

    def execute(self, sub_executor: SubTaskExecutor) -> TaskResult:  # noqa: ARG002
        self.prune_schemas_from_catalog()
        return TaskResult()

    def prune_schemas_from_catalog(self) -> None:
        schemas_in_schedule = self.maintenance_schedule.list_schemas_in_catalog(self.catalog)
        schemas_in_catalog = list_schemas_in_catalog(self.catalog)
        schemas_to_remove = schemas_in_schedule - schemas_in_catalog
        if schemas_to_remove:
            self.maintenance_schedule.delete_schemas(self.catalog, schemas_to_remove)


class DiscoveryTask(Task):
    """Each discovery task processes one schema at a time.

    For a given schema that exists in the catalog, it compares the list of tables in the catalog to the list of tables
    in the maintenance schedule and removes any tables from the schedule that no longer exist in the catalog
    and adds any new tables to the schedule that are present in the catalog but not in the schedule.
    """

    def __init__(self, maintenance_schedule: MaintenanceSchedule, catalog: str, schema: str) -> None:
        self.maintenance_schedule = maintenance_schedule
        self.catalog = catalog
        self.schema = schema

    def task_name(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def task_description(self) -> str:
        return f"Discovering (sync) tables in the schema [{self.catalog}.{self.schema}]"

    @classmethod
    def make_tasks(cls, maintenance_schedule: MaintenanceSchedule, scope: Scope) -> list[Task]:
        tasks: list[Task] = []
        assert scope.catalog, "Must be specified"
        assert not scope.table_name, "Unsupported"
        assert not scope.where, "Unsupported"
        # Must have a catalog and optionally a schema, but not a table or where clause.
        if scope.schema:
            # If asked to focus on a specific schema, only create a task for that schema.
            tasks = [SparkTask(cls(maintenance_schedule, scope.catalog, scope.schema))]
        else:
            # If no specific schema is requested, create a task for each schema in the catalog.
            schemas = list_schemas_in_catalog(scope.catalog)
            tasks = [SparkTask(cls(maintenance_schedule, scope.catalog, schema)) for schema in schemas]
        # In either case, also add a task to prune deleted schemas after syncing tables.
        prune_schemas = PruneDeletedSchemasTask.make_task(maintenance_schedule, scope.catalog)
        tasks.append(prune_schemas)
        return tasks

    def execute(self, sub_executor: SubTaskExecutor) -> TaskResult:  # noqa: ARG002
        self.sync_schema_found_in_catalog()
        return TaskResult()

    def sync_schema_found_in_catalog(self) -> None:
        table_names_in_catalog = list_table_names_in_schema(self.catalog, self.schema)
        table_names_in_schedule = self.maintenance_schedule.list_table_names_in_schema(self.catalog, self.schema)
        self.prune_deleted_tables(table_names_in_catalog, table_names_in_schedule)
        self.add_newly_created_tables(table_names_in_catalog, table_names_in_schedule)

    def prune_deleted_tables(self, table_names_in_catalog: set[str], table_names_in_schedule: set[str]) -> None:
        table_names_to_remove = table_names_in_schedule - table_names_in_catalog
        logger.debug(
            "DiscoveryTask catalog:%s, schema:%s, table_names_in_catalog: %s", self.catalog, self.schema, table_names_in_catalog
        )
        logger.debug(
            "DiscoveryTask catalog:%s, schema:%s, table_names_in_schedule: %s", self.catalog, self.schema, table_names_in_schedule
        )
        logger.debug(
            "DiscoveryTask catalog:%s, schema:%s, table_names_to_remove: %s", self.catalog, self.schema, table_names_to_remove
        )
        if table_names_to_remove:
            self.maintenance_schedule.delete_table_names(self.catalog, self.schema, table_names_to_remove)

    def add_newly_created_tables(self, table_names_in_catalog: set[str], table_names_in_schedule: set[str]) -> None:
        table_names_to_add = table_names_in_catalog - table_names_in_schedule
        logger.debug("DiscoveryTask catalog:%s, schema:%s, table_names_to_add: %s", self.catalog, self.schema, table_names_to_add)
        maintenance_entries_to_add: set[MaintenanceScheduleEntry] = set()
        for table_name in table_names_to_add:
            full_name = f"{self.catalog}.{self.schema}.{table_name}"
            logger.debug("adding a new maintenance entry for table %s", full_name)
            try:
                table = load_table(self.catalog, self.schema, table_name)
                entry = MaintenanceScheduleRecord.from_iceberg_table(table).to_entry()
                maintenance_entries_to_add.add(entry)
            except Exception as e:
                logger.exception("Failed to load table %s: %s", full_name, e)

        if maintenance_entries_to_add:
            self.maintenance_schedule.merge_entries(maintenance_entries_to_add)
