import logging
from typing import Any

from pyspark.sql.types import StructType
from typing_extensions import override

from ice_keeper import Action, escape_identifier
from ice_keeper.config import Config, TemplateName
from ice_keeper.stm import STL
from ice_keeper.task.task import SubTaskExecutor

from .action import ActionStrategy

logger = logging.getLogger("ice-keeper")


class LifecycleStrategy(ActionStrategy):
    """Strategy for managing lifecycle data operations on an Iceberg table.

    This strategy facilitates the removal of data that exceeds a certain lifecycle duration,
    using configured properties like `lifecycle_ingestion_time_column` and `lifecycle_max_days`.
    """

    @override
    @classmethod
    def get_action(cls) -> Action:
        """Returns the specific action type for the lifecycle strategy.

        This indicates that the action corresponds to lifecycle data operations.
        """
        return Action.LIFECYCLE_DATA

    @override
    def task_description(self, full_name: str) -> str:
        """Provides a description of the lifecycle operation for the specified table.

        Parameters:
            full_name (str): The fully qualified name of the table.

        Returns:
            str: A descriptive string explaining the lifecycle task.
        """
        return f"Lifecycle data for table: {full_name}"

    @override
    def check_should_execute_action(self) -> bool:
        """Determines whether the lifecycle operation should be executed.

        The lifecycle action is executed if:
        - `mnt_props.should_apply_lifecycle` is enabled.
        - The specified column for lifecycle (`lifecycle_ingestion_time_column`) exists in the table's schema.
        - The maximum lifecycle duration (`lifecycle_max_days`) is at least 1 day.

        If any of the above conditions are not met, an exception is raised or journaling is disabled.

        Returns:
            bool: True if the lifecycle operation should proceed, False otherwise.
        """
        should_execute = False
        if self.mnt_props.should_apply_lifecycle:
            # Validate if lifecycle column exists in the table's schema
            if not self._column_exists(self.mnt_props.lifecycle_ingestion_time_column):
                msg = f"{self.mnt_props.full_name} is configured to apply lifecycle using column [{self.mnt_props.lifecycle_ingestion_time_column}] but this column does not exist."
                raise Exception(msg)

            # Validate lifecycle duration is valid
            if self.mnt_props.lifecycle_max_days < 1:
                msg = f"{self.mnt_props.full_name} is configured to apply lifecycle with max days of [{self.mnt_props.lifecycle_max_days}]. Value must be 1 or greater."
                raise Exception(msg)

            should_execute = True
        else:
            logger.debug("Lifecycle operation is disabled, skipping %s.", self.mnt_props.full_name)

        if not should_execute:
            self.disable_journaling()

        return should_execute

    @override
    def prepare_statement_to_execute(self) -> str:
        """Prepares the SQL statement for performing the lifecycle operation.

        The operation removes data older than the specified `lifecycle_max_days`, based on the
        ingestion time column (`lifecycle_ingestion_time_column`), using an SQL `DELETE` statement.

        Returns:
            str: The SQL query for performing the lifecycle operation.
        """
        logger.debug("Preparing lifecycle operation for table: %s", self.mnt_props.full_name)
        template = Config.instance().load_template(TemplateName.LIFECYCLE_DATA)
        return template.render(
            catalog=escape_identifier(self.mnt_props.catalog),
            schema=escape_identifier(self.mnt_props.schema),
            table_name=escape_identifier(self.mnt_props.table_name),
            lifecycle_ingestion_time_column=self.mnt_props.lifecycle_ingestion_time_column,
            lifecycle_max_days=self.mnt_props.lifecycle_max_days,
        )

    @override
    def execute_statement(self, sub_executor: SubTaskExecutor, sql_stm: str) -> dict[str, Any]:
        """Executes the lifecycle SQL statement and retrieves the operation's results.

        After completing the SQL operation, this method queries the table's metadata to fetch
        details about the changes made, such as the number of data files deleted, records deleted, and
        partitions affected.

        Parameters:
            sub_executor (SubTaskExecutor): The context of the current task execution.
            sql_stm (str): The SQL statement to execute.

        Returns:
            dict[str, Any]: A dictionary containing a summary of the deleted files,
                            records, and partitions from the lifecycle operation.
        """
        description = f"Executing lifecycle procedure {self.get_action().value} for table: [{self.mnt_props.full_name}]"
        STL.sql_and_log(sql_stm, description=description)

        return self._get_lifecycle_results_from_delete_snapshot()

    def _column_exists(self, column: str) -> bool:
        """Checks if the specified column exists in the table's schema.

        Handles both simple and nested column names using dot notation. If the column
        is nested within a `StructType`, it inspects the sub-schema recursively.

        Parameters:
            column (str): The column to verify, using dot notation for nested fields (e.g., `parent.child`).

        Returns:
            bool: True if the column exists in the schema, False otherwise.
        """
        field_hierarchy = [field.strip("`") for field in column.split(".")]  # Remove any surrounding backticks
        schema: StructType = STL.sql(f"select * from {self.mnt_props.full_name}", "Getting schema").schema

        # Navigate through nested fields if column uses dot notation
        for field_name in field_hierarchy:
            if field_name not in schema.fieldNames():
                return False
            field = schema[field_name]
            # If the field is a StructType, proceed with its sub-schema
            if isinstance(field.dataType, StructType):
                schema = field.dataType

        return True

    def _get_lifecycle_results_from_delete_snapshot(self) -> dict[str, Any]:
        """Fetch the summary of the lifecycle operation from the table's snapshots.

        Reviews the snapshots in the table to check the last lifecycle data deletion process.
        Retrieves metadata about the data files and partitions affected.

        Returns:
            dict[str, Any]: A dictionary with three keys:
                - `lifecycle_deleted_data_files`: Number of data files that were deleted.
                - `lifecycle_deleted_records`: Number of records that were deleted.
                - `lifecycle_changed_partition_count`: Number of partitions that were modified.

            If no rows are available, default values of -1 are returned.
        """
        app_id = STL.get().sparkContext.applicationId
        sql = f"""
            select
                bigint(nvl(summary.`deleted-data-files`, 0)) as lifecycle_deleted_data_files,
                bigint(nvl(summary.`deleted-records`, 0)) as lifecycle_deleted_records,
                bigint(nvl(summary.`changed-partition-count`, 0)) as lifecycle_changed_partition_count
            from
                {self.mnt_props.full_name}.snapshots
            where
                operation = 'delete'
                and summary.`app-id` = '{app_id}'
        """
        rows = STL.sql_and_log(sql, "Retrieving results of lifecycle operation.").take(1)
        if len(rows) == 1:
            row = rows[0]
            return row.asDict()

        # Default values if no rows were found
        return {
            "lifecycle_deleted_data_files": -1,
            "lifecycle_deleted_records": -1,
            "lifecycle_changed_partition_count": -1,
        }
