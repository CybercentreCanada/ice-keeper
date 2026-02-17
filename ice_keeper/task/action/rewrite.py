import logging
from typing import Any

from typing_extensions import override

from ice_keeper import Action, escape_identifier
from ice_keeper.config import Config, TemplateName
from ice_keeper.stm import STL
from ice_keeper.task.task import SubTaskExecutor

from .action import ActionStrategy

logger = logging.getLogger("ice-keeper")


class RewriteManifestStrategy(ActionStrategy):
    """Strategy to rewrite manifests for an Iceberg table.

    Manifests in Iceberg tables contain metadata about data files. This strategy
    rewrites those manifests to optimize and consolidate them when needed.
    """

    @override
    @classmethod
    def get_action(cls) -> Action:
        """Return the specific action type for the RewriteManifest strategy."""
        return Action.REWRITE_MANIFESTS

    @override
    def task_description(self, full_name: str) -> str:
        """Provide a brief description of the rewrite manifests task for a table."""
        return f"Rewriting manifests for table: {full_name}"

    @override
    def check_should_execute_action(self) -> bool:
        """Determine if the rewrite manifests action should be executed.

        Conditions:
        - `mnt_props.should_rewrite_manifest` must be enabled, indicating that
          the table's configuration allows for manifest rewriting.

        If the action shouldn't be executed:
        - Log the reason for skipping and disable journaling to avoid unnecessary journal entries.

        Returns:
            bool: True if the action should be executed, False otherwise.
        """
        should_execute = False
        if self.mnt_props.should_rewrite_manifest:
            # If the table is configured to allow manifest rewriting, enable execution
            should_execute = True
        else:
            logger.debug("Rewrite manifest is disabled, skipping %s", self.mnt_props.full_name)

        # Disable journaling if not executing
        if not should_execute:
            self.disable_journaling()

        return should_execute

    @override
    def prepare_statement_to_execute(self) -> str:
        """Prepare the SQL call for the Iceberg `rewrite_manifests` procedure.

        This procedure rewrites all manifests for a table, optimizing metadata organization.

        Returns:
            str: The SQL call for rewriting manifests.
        """
        logger.debug("Preparing SQL statement to rewrite manifests for table: %s", self.mnt_props.full_name)
        template = Config.instance().load_template(TemplateName.REWRITE_MANIFESTS)
        return template.render(
            catalog=escape_identifier(self.mnt_props.catalog),
            schema=escape_identifier(self.mnt_props.schema),
            table_name=escape_identifier(self.mnt_props.table_name),
        )

    @override
    def execute_statement(self, sub_executor: SubTaskExecutor, sql_stm: str) -> dict[str, Any]:
        """Execute the given SQL procedure for rewriting manifests.

        The Iceberg `rewrite_manifests` procedure returns a one-row summary with metadata
        about the operation, such as the number of manifests rewritten and added.

        Parameters:
            sub_executor (SubTaskExecutor): The task execution context.
            sql_stm (str): The SQL statement for rewriting manifests.

        Returns:
            dict[str, Any]: A dictionary summarizing the results of the procedure.
        """
        description = f"Executing Iceberg procedure {self.get_action().value} for table: [{self.mnt_props.full_name}]"
        df = STL.sql_and_log(sql_stm, description=description)

        # The procedure returns a DataFrame with a single row summarizing the operation
        result_row = df.take(1)[0]
        return result_row.asDict()
