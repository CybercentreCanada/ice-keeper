import logging
from typing import Any

import humanfriendly
from pyspark.sql.types import Row
from typing_extensions import override

from ice_keeper import Action, ActionWarning, escape_identifier
from ice_keeper.config import Config, TemplateName
from ice_keeper.spec.widening_rule import WideningRule
from ice_keeper.stm import STL
from ice_keeper.table.schedule_entry import MaintenanceScheduleEntry
from ice_keeper.task.action.action import ActionStrategy
from ice_keeper.task.task import SubTaskExecutor

logger = logging.getLogger("ice-keeper")


class SubOptimizationStrategy(ActionStrategy):
    """A strategy for optimizing a specific partition by rewriting data files.

    This strategy runs within the context of a larger optimization strategy and focuses
    on optimizing individual partitions. It can handle both binpack and sort-based optimization,
    and executes widening rules for partition rewrites when applicable.
    """

    def __init__(self, row: Row, spec_id: int, mnt_props: MaintenanceScheduleEntry, widening_rule: None | WideningRule) -> None:
        """Initialize the strategy for optimizing a specific partition."""
        super().__init__(mnt_props)
        self.row = row
        self.spec_id = spec_id
        self.spec = self.mnt_props.partition_specs[self.spec_id]
        self.optimization_spec = self.mnt_props.optimization_spec
        self.widening_rule = widening_rule

    @override
    @classmethod
    def get_action(cls) -> Action:
        """Returns the specific action type for this strategy: REWRITE_DATA_FILES."""
        return Action.REWRITE_DATA_FILES

    @override
    def task_description(self, full_name: str) -> str:
        """Returns a description for the partition optimization task.

        Args:
            full_name (str): Fully qualified name of the target table.

        Returns:
            str: Description of the task.
        """
        return f"Optimizing partition of table {full_name}"

    @override
    def check_should_execute_action(self) -> bool:
        """Check if the partition optimization task should be executed.

        If the partition has a widening rule, this function checks if it's safe to widen the partition.

        Returns:
            bool: Always returns True unless a warning is raised.

        Raises:
            ActionWarning: If the widening rule detects unsafe columns with null values.
        """
        if self._has_widening_rule():
            # Validate if it is safe to widen the partition. Raises exception if unsafe.
            self._check_is_safe_to_widen_partition()

        return True

    @override
    def prepare_statement_to_execute(self) -> str:
        """Prepare the `rewrite_data_files` SQL statement for the specific partition.

        Returns:
            str: A SQL string to execute the compaction (rewrite) process.
        """
        return self._rewrite_data_files_statement()

    @override
    def execute_statement(self, sub_executor: SubTaskExecutor, sql_stm: str) -> dict[str, Any]:
        """Execute the SQL statement for partition optimization and return results.

        Args:
            sub_executor (SubTaskExecutor): The execution context for the task.
            sql_stm (str): The SQL procedure for rewriting data files.

        Returns:
            dict[str, Any]: A dictionary containing the results of the procedure.
        """
        description = f"Executing Iceberg procedure {self.get_action().value} for [{self.mnt_props.full_name}]"
        df = STL.sql_and_log(sql_stm, description=description)

        # Procedure returns a 1-row summary of the procedure call results
        result_row = df.take(1)[0]
        return result_row.asDict()

    def _rewrite_data_files_statement(self) -> str:
        """Constructs the SQL statement for the `rewrite_data_files` procedure.

        The statement includes:
        - Optimization strategy (binpack or sort).
        - Partition specifications and rewrite options.
        - Additional options such as shuffle partitions.

        Returns:
            str: SQL statement for the rewrite data files procedure.
        """
        # Define rewrite options and parameters
        options: dict[str, str] = {
            "max-concurrent-file-group-rewrites": "100",
            "partial-progress.enabled": "true"
            if not self.widening_rule
            else "false",  # When widening a partition partial progress fails.
            "delete-file-threshold": "1",
            "remove-dangling-deletes": "true",
            "max-file-group-size-bytes": str(humanfriendly.parse_size("200 GB", binary=True)),
            "target-file-size-bytes": str(self.mnt_props.target_file_size_bytes),
            "output-spec-id": str(self.spec_id),
            "rewrite-all": "true" if not self.optimization_spec.is_binpack() else "false",
            "min-input-files": "1",  # The diagnosis process checks if we have the necessary number of files to trigger an optimization of the partition. No need to check minimum here.
        }

        # Add additional options when optimizing sorted data
        if self.optimization_spec.is_sorted():
            per_file = self._evaluate_shuffle_partitions_per_file(self.mnt_props.target_file_size_bytes)
            options["shuffle-partitions-per-file"] = str(per_file)

        self._sanity_check_partition_field_values()

        # Add sort order argument if optimization is sorted
        sort_order: str | None = None
        if self.optimization_spec.is_sorted():
            sort_order = self.mnt_props.optimization_strategy

        # shuffle-partitions-per-file => Number of shuffle partitions to use for each output file.
        # Iceberg will use a custom coalesce operation to stitch these sorted partitions back together into a single sorted file.
        template = Config.instance().load_template(TemplateName.REWRITE_DATA_FILES)
        return template.render(
            catalog=escape_identifier(self.mnt_props.catalog),
            schema=escape_identifier(self.mnt_props.schema),
            table_name=escape_identifier(self.mnt_props.table_name),
            strategy=self.optimization_spec.strategy.value,
            sort_order=sort_order,
            where=self._convert_to_rewrite_data_files_partition_filter_stmt(),
            options=options,
        )

    def _evaluate_shuffle_partitions_per_file(self, target_file_size_bytes: int) -> int:
        """Determine the number of shuffle partitions per output file.

        Uses a heuristic based on file size to optimize shuffle partitions.

        Args:
            target_file_size_bytes (int): Target file size in bytes.

        Returns:
            int: Number of shuffle partitions to use for each file.
        """
        # Use more partitions for files greater than 64 MB
        n = int(target_file_size_bytes / (64 * 1024 * 1024))
        return max(1, n)

    def _sanity_check_partition_field_values(self) -> None:
        try:
            self.spec.sanity_check_partition_field_values(self.row)
        except ValueError as e:
            msg = f"Cannot optimize a partition in table {self.mnt_props.full_name}. Caused by {e!s}"
            raise ActionWarning(msg)  # noqa: B904

    def _convert_to_rewrite_data_files_partition_filter_stmt(self) -> str:
        """Generates a WHERE clause for filtering the partitions to optimize.

        This ensures that the optimization targets specific partitions according
        to the maintenance properties.

        Returns:
            str: A SQL WHERE clause to filter specific partitions.
        """
        stmt = self.spec.convert_to_rewrite_data_files_partition_filter_stmt(self.row)
        logger.debug("Adding partition to optimization list: %s", stmt)
        return stmt

    def _has_widening_rule(self) -> bool:
        """Check if a widening rule exists for the specified partition.

        Returns:
            bool: True if a widening rule is defined, False otherwise.
        """
        return bool(self.widening_rule)

    def _check_is_safe_to_widen_partition(self) -> None:
        """Ensures that the partition widening operation is safe to execute.

        Verifies that the required partition columns do not contain null values.
        If null values are found, raises an `ActionWarning` to skip execution.

        Raises:
            ActionWarning: If required columns contain null values, preventing safe widening.
        """
        # Retrieve the widening rule for the partition spec
        assert self.widening_rule, "Widening rule must be defined."

        # Create the validation filter for the partition
        validation_filter = self.widening_rule.make_widening_validation_filter(self.row)

        sql = f"""
          -- Checking if safe to run widening rewrite
          select
            1
          from
            {self.mnt_props.full_name}
          where
            {validation_filter}
        """
        # Execute the validation query
        rows = STL.sql_and_log(sql, "Check if safe to widen partition").take(1)
        if len(rows) > 0:
            warning_message = (
                "Partition cannot be widened. Some required columns have null values, which prevents a safe widening operation. "
                f"Required columns: {self.widening_rule.required_fixed_columns}. "
                "All required columns should have a value set for the given date range. "
                f"The following query should not return any rows: {sql}"
            )
            logger.debug(warning_message)
            raise ActionWarning(warning_message)
