import logging

from pyspark.sql.types import Row

from ice_keeper.stm import STL
from ice_keeper.table import MaintenanceScheduleEntry

from .partition_summary import PartitionSummary

logger = logging.getLogger("ice-keeper")


class PartitionDiagnosis:
    """Diagnoses the health of partitions and identifies those that need optimization.

    This class is responsible for identifying partitions in a table
    that require optimization. The diagnosis process is based on metadata
    from the `PartitionSummary` and optimization configurations defined
    in the maintenance properties.
    """

    def __init__(self, mnt_props: MaintenanceScheduleEntry, spec_id: int) -> None:
        """Initialize a `PartitionDiagnosis` instance.

        Args:
            mnt_props (MaintenanceScheduleEntry): Maintenance properties that provide
                details about the table and its optimization configuration.
            spec_id (int): The identifier of the partition specification used for
                partition diagnostics.
        """
        self.mnt_props = mnt_props
        self.spec = mnt_props.partition_specs[spec_id]

    def find_partitions_to_optimize(self, summary: PartitionSummary) -> list[Row]:
        """Find partitions that need optimization based on the optimization criteria.

        The method determines which partitions require binpacking or sorting by evaluating
        the `PartitionSummary`. The decision is based on the `should_binpack` or
        `should_sort` flags from the summary view, depending on the optimization
        configuration in `mnt_props.optimization_spec`.

        A suitable SQL query is generated to retrieve the partition information:
        - For binpacking, partitions flagged with `should_binpack = true` are selected.
        - For sorting, partitions flagged with `should_sort = true` are selected.

        The query groups partitions based on the `grouping_stmt` and sorts them by
        their age in ascending order to prioritize older partitions for optimization.

        Args:
            summary (PartitionSummary): An instance that contains summary data about
                the partitions, including metrics to evaluate optimization criteria.

        Returns:
            list[Row]: A list of rows, each containing partition information for
                       the partitions marked for optimization.
        """
        # Determine the appropriate optimization criteria based on the spec
        criteria = "should_binpack = true" if self.mnt_props.optimization_spec.is_binpack() else "should_sort = true"

        # Generate the grouping statement based on the partition structure
        grouping_stmt = self.spec.make_diagnosis_grouping_stmt(self.mnt_props.optimize_partition_depth)

        # Construct SQL query to retrieve partitions that satisfy the optimization criteria
        sql = f"""
                -- Identifying partitions to optimize for table {self.mnt_props.full_name}
                select
                    partition_age,
                    {grouping_stmt},
                    first(target_file_size) as target_file_size -- all values are the same per partition
                from
                    {summary.summary_before_view_name}
                where
                    {criteria}
                group by
                    partition_age,
                    {grouping_stmt}
                order by
                    partition_age asc
                """
        # Log the SQL query for debugging
        logger.debug("Executing SQL to find partitions to optimize:\n%s", sql)

        # Execute the SQL query and collect the results
        return STL.sql_and_log(sql, "Find partitions to optimize").collect()
