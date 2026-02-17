import logging
from datetime import timedelta
from typing import Any

from typing_extensions import override

from ice_keeper import Action, TimeProvider, escape_identifier
from ice_keeper.config import Config, TemplateName
from ice_keeper.stm import STL
from ice_keeper.task.task import SubTaskExecutor

from .action import ActionStrategy

logger = logging.getLogger("ice-keeper")


class ExpireSnapshotsStrategy(ActionStrategy):
    """Strategy to handle the expiration of old snapshots in an Iceberg table.

    This strategy invokes a procedure in Iceberg to remove outdated and unnecessary
    snapshots based on table properties for snapshot retention.
    """

    @override
    @classmethod
    def get_action(cls) -> Action:
        """Returns the specific action type for the ExpireSnapshots strategy."""
        return Action.EXPIRE_SNAPSHOTS

    @override
    def task_description(self, full_name: str) -> str:
        """Returns a description of the snapshot expiration task for the given table."""
        return f"Expiring snapshots from table: {full_name}"

    @override
    def check_should_execute_action(self) -> bool:
        """Determine whether the snapshot expiration task should be executed.

        The task is executed if:
        1. `mnt_props.should_expire_snapshots` is enabled (indicating that snapshot expiration is allowed).
        2. The table has been recently modified, as determined by `_is_table_recently_modified`.

        If either condition is not met:
        - Logs relevant information about why the action is being skipped.
        - Disables journaling to prevent unnecessary log entries.

        Returns:
            bool: True if the snapshot expiration action should be performed, False otherwise.
        """
        should_execute = False
        if self.mnt_props.should_expire_snapshots:
            # Check if the table was recently modified
            dirty = self._is_table_recently_modified()
            if dirty:
                should_execute = True
            else:
                logger.debug("No recent changes found, skipping snapshot expiration for %s", self.mnt_props.full_name)
        else:
            logger.debug("Snapshot expiration is disabled, skipping %s", self.mnt_props.full_name)

        if not should_execute:
            self.disable_journaling()

        return should_execute

    @override
    def prepare_statement_to_execute(self) -> str:
        """Prepare the SQL call to expire old snapshots in the target Iceberg table.

        This method generates a SQL statement to expire snapshots based on:
        - `retention_days_snapshots`: Defines the maximum age of snapshots to retain.
        - `retention_num_snapshots`: Defines the minimum number of snapshots to retain in the table.
        - The table's catalog, schema, and name.

        These settings ensure that the table's storage consumption is optimized
        without retaining an excessive amount of metadata.

        Returns:
            str: The SQL statement to invoke the `expire_snapshots` procedure.
        """
        logger.debug("Preparing SQL statement to expire snapshots for table: %s", self.mnt_props.full_name)

        n_days = self.mnt_props.retention_days_snapshots  # Number of days to retain snapshots
        older_than = TimeProvider.current_datetime() - timedelta(days=n_days)  # Calculate the cutoff timestamp

        template = Config.instance().load_template(TemplateName.EXPIRE_SNAPSHOTS)
        return template.render(
            catalog=escape_identifier(self.mnt_props.catalog),
            schema=escape_identifier(self.mnt_props.schema),
            table_name=escape_identifier(self.mnt_props.table_name),
            older_than=older_than,
            retain_last=self.mnt_props.retention_num_snapshots,
        )

    @override
    def execute_statement(self, sub_executor: SubTaskExecutor, sql_stm: str) -> dict[str, Any]:
        """Execute the SQL statement for snapshot expiration and return the results.

        This method runs the Iceberg `expire_snapshots` procedure, which removes
        old and unnecessary snapshots based on the provided parameters. A summary of the execution
        is then returned.

        Parameters:
            sub_executor (SubTaskExecutor): The execution context of the task.
            sql_stm (str): The SQL statement to execute.

        Returns:
            dict[str, Any]: A dictionary containing the procedure's results, such as the number of
                            expired snapshots or other details.
        """
        STL.set_config("spark.sql.autoBroadcastJoinThreshold", "-1")
        description = f"Executing Iceberg procedure {self.get_action().value} for table: [{self.mnt_props.full_name}]"
        df = STL.sql_and_log(sql_stm, description=description)

        # The procedure returns a one-row summary of the results
        result_row = df.take(1)[0]
        return result_row.asDict()
