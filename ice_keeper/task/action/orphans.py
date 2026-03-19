import logging
from datetime import timedelta
from typing import Any

from typing_extensions import override

from ice_keeper import Action, TimeProvider, escape_identifier
from ice_keeper.config import Config, TemplateName
from ice_keeper.output import rows_log_debug
from ice_keeper.stm import STL, Scope
from ice_keeper.table import StorageInventoryReport
from ice_keeper.table.schedule import MaintenanceSchedule
from ice_keeper.table.schedule_entry import MaintenanceScheduleEntry
from ice_keeper.task.task import SubTaskExecutor

from .action import ActionStrategy

logger = logging.getLogger("ice-keeper")


class RemoveOrphanFilesStrategy(ActionStrategy):
    """Strategy for identifying and removing orphan files in tables.

    Orphan files are unused files in storage that are no longer referenced by
    the metadata system of an Iceberg table. This strategy efficiently identifies
    and removes them to free up storage space and maintain table consistency.
    """

    name_of_file_list_view = "file_list_view"

    def __init__(self, mnt_props: MaintenanceScheduleEntry) -> None:
        """Initialize the strategy for removing orphan files.

        Args:
            mnt_props (MaintenanceScheduleEntry): Properties associated with the maintenance strategy.
        """
        super().__init__(mnt_props)
        # Initialize variables for older-than calculation and file system
        self.older_than = TimeProvider.current_date() - timedelta(days=500)
        self.storage_inventory_report: StorageInventoryReport | None = None

    @override
    @classmethod
    def get_action(cls) -> Action:
        """Return the specific action type for orphan file removal."""
        return Action.REMOVE_ORPHAN_FILES

    @override
    def task_description(self, full_name: str) -> str:
        """Provide a description of the orphan file removal task."""
        return f"Removing orphan files from {full_name}"

    @override
    def check_should_execute_action(self) -> bool:
        """Determine whether orphan file removal should be performed.

        Conditions:
        - `should_remove_orphan_files` in `mnt_props` must be True.
        - The table should be recently modified (`_is_table_recently_modified`).

        If not satisfied:
        - Log the status and disable journaling to avoid unnecessary entries.

        Returns:
            bool: True if action should be executed, False otherwise.
        """
        should_execute = False
        if self.mnt_props.should_remove_orphan_files:
            # Check if the table was recently modified
            dirty = self._is_table_recently_modified()
            if dirty:
                should_execute = True
            else:
                logger.debug("No recent changes found, skipping removal of orphan files for table %s", self.mnt_props.full_name)
        else:
            logger.debug("Orphan file removal is disabled, skipping %s", self.mnt_props.full_name)

        if not should_execute:
            self.disable_journaling()

        return should_execute

    @override
    def prepare_statement_to_execute(self) -> str:
        """Prepare the SQL query to remove orphan files after any necessary setup.

        This function performs:
        - Setting `older_than` retention parameters.
        - Initialization of storage inventory and file systems.

        Returns:
            str: The SQL statement to execute the orphan file removal.
        """
        logger.debug("Preparing for orphan file removal for table: %s", self.mnt_props.full_name)
        if Config.instance().storage_inventory_report_table_name:
            self.storage_inventory_report = StorageInventoryReport(self.mnt_props)
            self._init_older_than()

        # Throw an exception if this table shares location with any other known table location
        MaintenanceSchedule(Scope()).check_table_location_is_unique(self.mnt_props.full_name, self.mnt_props.table_location)

        return self._make_procedure_call_stm()

    @override
    def execute_statement(self, sub_executor: SubTaskExecutor, sql_stm: str) -> dict[str, Any]:
        """Execute the SQL procedure for orphan file removal after deleting empty folders.

        Parameters:
            sub_executor (SubTaskExecutor): The task's execution context.
            sql_stm (str): SQL statement for the orphan file removal procedure.

        Returns:
            dict[str, Any]: A summary of the orphan file removal task, containing:
              - `num_orphan_files_deleted`: Number of orphan files deleted.
        """
        description = f"Executing Iceberg procedure {self.get_action().value} for table: [{self.mnt_props.full_name}]"
        df = STL.sql_and_log(sql_stm, description=description)
        logger.debug("Counting number of orphan files deleted")

        # Count rows in the DataFrame to determine the number of orphan files deleted
        num_orphan_files_deleted = df.count()
        return {"num_orphan_files_deleted": num_orphan_files_deleted}

    def _make_procedure_call_stm(self) -> str:
        """Construct the SQL statement for the `remove_orphan_files` procedure.

        Utilizes additional configurations such as:
        - Using file inventory lists (`file_list_view`) if available.
        - Adjusting Spark configurations for tables with a large number of snapshots.
        - Defining `equal_authorities` to normalize storage path mappings.

        Returns:
            str: Fully constructed SQL call to execute the orphan file removal.
        """
        view_created = self._create_file_list_view()

        # Disable broadcast join for tables with potentially large datasets, and adjust partitions if required
        STL.set_config("spark.sql.autoBroadcastJoinThreshold", "-1")

        desc = "Counting number of snapshots in table"
        n_snapshots = STL.sql(f"select * from {self.mnt_props.full_name}.snapshots", desc).count()
        if n_snapshots > 50000:  # noqa: PLR2004
            logger.debug(
                "%s has %s snapshots, increasing spark.sql.shuffle.partitions to 10,000",
                self.mnt_props.full_name,
                n_snapshots,
            )
            STL.set_config("spark.sql.shuffle.partitions", "10000")
        elif n_snapshots > 5000:  # noqa: PLR2004
            logger.debug(
                "%s has %s snapshots, increasing spark.sql.shuffle.partitions to 1,000",
                self.mnt_props.full_name,
                n_snapshots,
            )
            STL.set_config("spark.sql.shuffle.partitions", "1000")

        template = Config.instance().load_template(TemplateName.REMOVE_ORPHAN_FILES)
        file_list_view: str | None = None
        if view_created:
            file_list_view = self.name_of_file_list_view
        return template.render(
            catalog=escape_identifier(self.mnt_props.catalog),
            schema=escape_identifier(self.mnt_props.schema),
            table_name=escape_identifier(self.mnt_props.table_name),
            older_than=self.older_than,
            file_list_view=file_list_view,
        )

    def _init_older_than(self) -> None:
        """Calculate the `older_than` value based on inventory age and retention period.

        Combines the age of the current inventory report with the configured
        `retention_days_orphan_files` to account for any file modifications or delays.
        Ensures that no recently recreated files are accidentally deleted.
        """
        assert self.storage_inventory_report, "Storage inventory report must be initialized to calculate older_than"
        inventory_date = self.storage_inventory_report.get_most_recent_inventory_report_date()
        age_of_inventory_report = 1 + abs((TimeProvider.current_date() - inventory_date).days)
        n_days = age_of_inventory_report + self.mnt_props.retention_days_orphan_files
        self.older_than = TimeProvider.current_date() - timedelta(days=n_days)

    def _create_file_list_view(self) -> bool:
        """Generate a view of file paths from the storage inventory.

        If the storage inventory contains valid file paths, this method creates a temporary
        view with the file list for future operations. If the inventory lacks data, a fallback
        process is utilized.

        Returns:
            bool: Indicating if a valid file list view was created (`True`) or not (`False`).
        """
        if self.storage_inventory_report is None:
            return False

        file_list_view_sql = self.storage_inventory_report.select_iceberg_files_from_inventory_stmt(self.older_than)
        sample_inventory = STL.sql_and_log(file_list_view_sql, "Sampling inventory").take(3)
        if len(sample_inventory) == 0:
            logger.debug("No files found in inventory, falling back to full directory listing.")
            return False

        rows_log_debug(sample_inventory, "Inventory Files")

        sql = self.storage_inventory_report.select_files_and_empty_folders_from_inventory_stmt(self.older_than)
        STL.sql(sql, "Create inventory file list view including empty dirs").createOrReplaceTempView(self.name_of_file_list_view)
        return True
