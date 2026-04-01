import logging
from typing import Any

from pyspark.sql.types import Row, StructType
from typing_extensions import override

from ice_keeper import Action
from ice_keeper.output import rows_log_debug
from ice_keeper.spec import WideningRule
from ice_keeper.stm import STL
from ice_keeper.table import PartitionHealth
from ice_keeper.task import SparkTask
from ice_keeper.task.action.action import ActionStrategy, ActionTask
from ice_keeper.task.action.optimization.datafile_summary import DataFilesSummary
from ice_keeper.task.task import SubTaskExecutor

from .optimization_partition import SubOptimizationStrategy
from .partition_diagnostic import PartitionDiagnosis, PartitionSummary

logger = logging.getLogger("ice-keeper")


class OptimizationStrategy(ActionStrategy):
    """Strategy for optimizing partition health by compacting data files in an Iceberg table.

    This strategy evaluates partition health and applies compaction (via rewrite_data_files) to partitions
    that do not meet the defined health thresholds.
    """

    @override
    @classmethod
    def get_action(cls) -> Action:
        """Returns the specific action type for the Optimization strategy."""
        return Action.REWRITE_DATA_FILES

    @override
    def task_description(self, full_name: str) -> str:
        """Returns a human-readable description of the optimization task.

        Args:
            full_name (str): Fully qualified name of the table.

        Returns:
            str: Task description indicating the table being optimized.
        """
        return f"Optimizing table: {full_name}"

    @override
    def check_should_execute_action(self) -> bool:
        """Determines whether the optimization action should proceed.

        Conditions:
        - `should_optimize` in maintenance properties must be enabled.
        - The table must be recently modified.
        - Validates the compaction strategy (binpack or sort) is configured.
        - Ensures the sorted columns exist in the table schema, if sorting is configured.

        Raises:
            Exception: If any required configuration or column is invalid.

        Returns:
            bool: True if the action should be executed, otherwise False.
        """
        should_execute = False
        if self.mnt_props.should_optimize:
            # Check if the table was recently modified
            if self._is_table_recently_modified():
                # Validate widening rule, if any. Raises exception if incorrectly configured.
                self.create_widening_rule_if_any()

                # Ensure a compaction strategy is configured
                if not self.mnt_props.optimization_spec.is_binpack() and not self.mnt_props.optimization_spec.is_sorted():
                    msg = f"{self.mnt_props.full_name} is configured for optimization but no compaction strategy is specified."
                    raise Exception(msg)

                # Ensure sorted columns exist if sorting is configured
                if self.mnt_props.optimization_spec.is_sorted():
                    missing_columns = [
                        column_name
                        for column_name in self.mnt_props.optimization_spec.sorted_column_names
                        if not self._sorted_column_exists(column_name)
                    ]
                    if missing_columns:
                        missing_str = ", ".join(f"[{col}]" for col in missing_columns)
                        msg = f"{self.mnt_props.full_name} is configured to sort columns {missing_str} but these columns do not exist."
                        raise Exception(msg)

                should_execute = True
            else:
                logger.debug("No recent changes detected; skipping optimization for table %s", self.mnt_props.full_name)
        else:
            logger.debug("Optimization is disabled; skipping %s", self.mnt_props.full_name)

        if not should_execute:
            self.disable_journaling()
        return should_execute

    @override
    def prepare_statement_to_execute(self) -> str:
        """Not applicable for this strategy as it does not prepare a SQL statement."""
        return ""

    @override
    def execute_statement(self, sub_executor: SubTaskExecutor, sql_stm: str) -> dict[str, Any]:
        """Executes the optimization strategy by evaluating partition health and optimizing partitions.

        This function identifies partition specs to optimize and processes them using
        the applicable compaction strategy (binpack or sorted).

        Args:
            sub_executor (SubTaskExecutor): The task execution context.
            sql_stm (str): Not used for optimization but required to match method signature.

        Returns:
            dict[str, Any]: An empty dictionary as the parent does not require journaling.
        """
        self.find_and_optimize_specs(sub_executor)
        # Disable parent journaling if all tasks were successfully executed
        self.disable_journaling()
        return {}

    def find_and_optimize_specs(self, sub_executor: SubTaskExecutor) -> None:
        unique_spec_ids = self._find_specs_to_optimize()

        for spec_id in unique_spec_ids:
            logger.debug("START Optimizing spec_id: %s -> %s", spec_id, self.mnt_props.partition_specs[spec_id])
            did_some_optimizations = False
            # Collect partition summary for the spec_id
            summary = PartitionSummary(self.mnt_props, spec_id, self.get_widening_rule(spec_id))
            summary.show(100)

            try:
                # Diagnose the partitions for optimization opportunities
                diagnosis = PartitionDiagnosis(self.mnt_props, spec_id)

                rows = diagnosis.find_partitions_to_optimize(summary)
                if len(rows) > 0:
                    rows_log_debug(rows, f"Partitions to optimize in {self.mnt_props.full_name}")
                    did_some_optimizations = True
                    self._execute_sub_tasks(sub_executor, rows, spec_id)
                else:
                    logger.debug("All partitions in spec_id: %s are healthy", spec_id)

                # In the context of executing optimization, we want to save the results back to the partition health table
                partition_health = PartitionHealth()
                summary.save_diff(partition_health, did_some_optimizations=did_some_optimizations)
            finally:
                logger.debug("END Optimizing spec_id: %s", spec_id)
                summary.uncache_views(did_some_optimizations=did_some_optimizations)

    def _run_partition_spec_diagnostics(self, *, estimate_optimization_results: bool) -> None:
        unique_spec_ids = self._find_specs_to_optimize()
        for spec_id in unique_spec_ids:
            logger.debug(
                "START Diagnosing spec_id: %s -> %s",
                spec_id,
                self.mnt_props.partition_specs[spec_id],
            )
            spec = self.mnt_props.partition_specs[spec_id]
            widening_rule = self.get_widening_rule(spec_id)
            datafiles_summary = DataFilesSummary(self.mnt_props, spec, spec_id, widening_rule)
            sql = datafiles_summary.create_summary_stmt(estimate_optimization_results=estimate_optimization_results)
            rows = STL.sql_and_log(sql, "Retrieve rows from partition summary").take(10000)
            rows_log_debug(rows, f"Diagnostic Partition Summary of {self.mnt_props.full_name}, spec: {spec}")

    def diagnose_partition_specs(self) -> None:
        self._run_partition_spec_diagnostics(estimate_optimization_results=False)

    def estimate_optimization_results_partition_specs(self) -> None:
        self._run_partition_spec_diagnostics(estimate_optimization_results=True)

    def create_widening_rule_if_any(self) -> None | WideningRule:
        """Attach a widening rule to the partition specs, if defined in the table configuration.

        Widening rules are used to handle partition transitions, such as day to month partitions.
        """
        rule = None
        if (
            self.mnt_props.widening_rule_src_partition
            or self.mnt_props.widening_rule_dst_partition
            or self.mnt_props.widening_rule_select_criteria
        ):
            rule = WideningRule(
                self.mnt_props.partition_specs,
                self.mnt_props.widening_rule_src_partition,
                self.mnt_props.widening_rule_dst_partition,
                self.mnt_props.get_widening_rule_required_partition_columns(),
                self.mnt_props.widening_rule_select_criteria,
            )
        return rule

    def get_widening_rule(self, spec_id: int) -> None | WideningRule:
        rule = self.create_widening_rule_if_any()
        if rule and rule.spec_id == spec_id:
            return rule
        return None

    def _execute_sub_tasks(self, sub_executor: SubTaskExecutor, rows: list[Row], spec_id: int) -> None:
        """Execute sub-tasks for optimizing partitions.

        Sub-tasks are created based on rows containing partitions that require optimization.

        Args:
            sub_executor (SubTaskExecutor): The task execution context.
            rows (list[Row]): List of partitions to optimize.
            spec_id (int): The partition spec ID being processed.
        """
        # sub_tasks: list[Task] = []
        for row in rows:
            strategy = SubOptimizationStrategy(row, spec_id, self.mnt_props, self.get_widening_rule(spec_id))
            task = SparkTask(ActionTask(strategy, self.mnt_props))
            sub_executor.submit_subtasks_and_wait([task])
            # sub_tasks.append(task)
        # context.submit_subtasks_and_wait(sub_tasks)

    def _find_specs_to_optimize(self) -> list[int]:
        """Identify distinct spec IDs to optimize.

        The function collects spec IDs from the table's data files and combines them
        with any spec IDs that are defined in widening rules.

        This function makes sure to process partition specs that have a widening rule first.
        This is important to pick up unsorted files from the source partition and move them to the destination.

        Returns:
            list[int]: A list of unique spec IDs to optimize.
        """
        sql = f"select distinct spec_id from {self.mnt_props.full_name}.data_files"
        spec_id_rows = STL.sql(sql, "Finding spec IDs to optimize").collect()

        spec_ids_with_data_files = {row.spec_id for row in spec_id_rows}
        unique_spec_ids = list(spec_ids_with_data_files)

        rule = self.create_widening_rule_if_any()
        widening_rule_dst_spec_id = None
        if rule:
            widening_rule_dst_spec_id = rule.spec_id

        # Ensure the widening rule's destination spec id is first, if there is no widening rule then make sure the default spec ID is included first
        if self.mnt_props.partition_specs.default_spec_id in unique_spec_ids:
            unique_spec_ids.remove(self.mnt_props.partition_specs.default_spec_id)
        if widening_rule_dst_spec_id in unique_spec_ids:
            unique_spec_ids.remove(widening_rule_dst_spec_id)

        unique_spec_ids.insert(0, self.mnt_props.partition_specs.default_spec_id)
        if widening_rule_dst_spec_id is not None and widening_rule_dst_spec_id not in unique_spec_ids:
            unique_spec_ids.insert(0, widening_rule_dst_spec_id)

        logger.debug("Partition specs to optimize: %s", unique_spec_ids)

        # Validate that all spec IDs exist in the metadata
        for spec_id in unique_spec_ids:
            if spec_id < 0 or spec_id >= len(self.mnt_props.partition_specs.get_specifications()):
                msg = f"Partition spec id found: {spec_id} does not exist in the list of partition specs in metadata."
                raise Exception(msg)
        return unique_spec_ids

    def _sorted_column_exists(self, column: str) -> bool:
        """Verify that a column exists in the table schema.

        Supports nested columns by handling dot notation for structured schema types.

        Args:
            column (str): The column to check, supports nested fields with dot notation.

        Returns:
            bool: True if the column exists, False otherwise.
        """
        field_hierarchy = [field.strip("`") for field in column.split(".")]  # Remove backticks
        schema: StructType = STL.sql(f"select * from {self.mnt_props.full_name}", "Getting schema").schema

        for field_name in field_hierarchy:
            if field_name not in schema.fieldNames():
                return False
            field = schema[field_name]
            if isinstance(field.dataType, StructType):  # Handle nested schema
                schema = field.dataType
        return True
