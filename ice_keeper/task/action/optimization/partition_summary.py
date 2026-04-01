import logging

from ice_keeper.output import rows_log_debug
from ice_keeper.spec.widening_rule import WideningRule
from ice_keeper.stm import STL
from ice_keeper.table import MaintenanceScheduleEntry, PartitionHealth

from .datafile_summary import DataFilesSummary

logger = logging.getLogger("ice-keeper")


class PartitionSummary:
    """Creates a summary of partition health, for diagnostics and optimization.

    This class is responsible for generating partition summaries and identifying
    which partitions need optimization. It uses cached views for staged evaluation.
    """

    def __init__(self, mnt_props: MaintenanceScheduleEntry, spec_id: int, windening_rule: None | WideningRule = None) -> None:
        """Initializes the PartitionSummary object."""
        self.mnt_props = mnt_props
        self.spec = mnt_props.partition_specs[spec_id]
        self.datafiles_summary = DataFilesSummary(self.mnt_props, self.spec, spec_id, windening_rule)

        # Create meaningful names for cached Spark views for easier identification in the Spark UI
        view_name_base = f"{self.mnt_props.catalog}_{self.mnt_props.schema}_{self.mnt_props.table_name}"
        self.summary_before_view_name = f"BEFORE_DIAGNOSIS_FOR_{view_name_base}"
        self.summary_after_view_name = f"AFTER_DIAGNOSIS_FOR_{view_name_base}"

        # Create and cache the initial partition summary
        self._create_and_cache(self.summary_before_view_name)

    def save_diff(self, partition_health: PartitionHealth, *, did_some_optimizations: bool) -> None:
        """Store the results of the optimization to the partition health table.

        If optimizations were performed, computes and caches the summary after updates.
        """
        if did_some_optimizations:
            # Cache a new summary after optimization to ensure results are reusable
            self._create_and_cache(self.summary_after_view_name)
            partition_health.write(self.summary_before_view_name, self.summary_after_view_name, self.mnt_props)

    def show(self, max_rows: int = 10000) -> None:
        """Displays partition summary rows for debugging.

        Args:
            max_rows (int): Maximum number of rows to display in logs. Defaults to 10,000.
        """
        rows = STL.sql(f"select * from {self.summary_before_view_name}", "Retrieve rows from partition summary").take(max_rows)
        rows_log_debug(rows, f"Partition Summary of {self.mnt_props.full_name}")

    def uncache_views(self, *, did_some_optimizations: bool) -> None:
        """Uncache the summary views.

        Args:
            did_some_optimizations (bool): Indicates whether optimization was performed.
        """
        if did_some_optimizations:
            self._uncache_view(self.summary_after_view_name)
        self._uncache_view(self.summary_before_view_name)

    def _create_and_cache(self, view_name: str) -> None:
        """Creates and caches a temporary summary view."""
        sql = self.datafiles_summary.create_summary_stmt()
        df = STL.sql_and_log(sql, "Create temporary view")
        df.createOrReplaceTempView(view_name)
        STL.sql(f"cache table {view_name}", f"Caching table: {view_name}")

    def _uncache_view(self, view_name: str) -> None:
        """Uncache the temporary summary view.

        Args:
            view_name (str): The name of the view to uncache.
        """
        STL.sql(f"uncache table if exists {view_name}", f"Un-caching table: {view_name}")
