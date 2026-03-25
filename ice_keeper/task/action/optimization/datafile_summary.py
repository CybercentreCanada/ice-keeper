import logging
from abc import ABC, abstractmethod

from typing_extensions import override

from ice_keeper import IceKeeperTblProperty
from ice_keeper.spec import PartitionSpecification, WideningRule
from ice_keeper.table import MaintenanceScheduleEntry

from .datafiles_bounds import Bounds, BoundsBinpack, BoundsSort, BoundsZorderSort

logger = logging.getLogger("ice-keeper")


class DataFiles(ABC):
    def __init__(
        self,
        mnt_props: MaintenanceScheduleEntry,
        spec: PartitionSpecification,
    ) -> None:
        """Base class for selecting data file to optimize."""
        self.mnt_props = mnt_props
        self.spec = spec

    @abstractmethod
    def make_data_files_stmt(self) -> str:
        """Abstract method to fetch the SQL statement for retrieving data files.

        Returns:
            str: SQL statement for fetching data files.
        """

    @abstractmethod
    def get_min_age_to_optimize(self) -> int:
        """Abstract method to fetch the minimum age for optimization.

        Returns:
            int: Minimum age (in days or equivalent units) for optimization eligibility.
        """


class DataFilesBinpack(DataFiles):
    @override
    def get_min_age_to_optimize(self) -> int:
        """Fetch the minimum age for binpack optimizations.

        Returns:
            int: Minimum number of days before files are eligible for optimization.
        """
        return self.mnt_props.min_age_to_optimize

    @override
    def make_data_files_stmt(self) -> str:
        """Generate a SQL query to retrieve data files for binpacking optimization.

        Returns:
            str: SQL query for fetching relevant data files.
        """
        return f"""
            select
                {self.spec.make_alias_stmt()},
                spec_id,
                content,
                record_count,
                file_size_in_bytes,
                readable_metrics,
                false as is_data_file_from_widening_src_partition
            from
                {self.mnt_props.full_name}.files
        """


class DataFilesSort(DataFiles):
    @override
    def get_min_age_to_optimize(self) -> int:
        """Fetch the minimum age for sorting-based optimizations.

        Returns:
            int: Minimum number of days before files are eligible for optimization.
        """
        return self.mnt_props.min_age_to_optimize

    @override
    def make_data_files_stmt(self) -> str:
        """Generate a SQL query to retrieve data files for sorting optimization.

        Returns:
            str: SQL query for fetching relevant data files.
        """
        return f"""
            select
                {self.spec.make_alias_stmt()},
                spec_id,
                content,
                record_count,
                file_size_in_bytes,
                readable_metrics,
                false as is_data_file_from_widening_src_partition
            from
                {self.mnt_props.full_name}.files
        """


class DataFilesWideningSort(DataFiles):
    def __init__(self, mnt_props: MaintenanceScheduleEntry, spec: PartitionSpecification, widening_rule: WideningRule) -> None:
        """Initialize for sorting operations based on widening rules."""
        super().__init__(mnt_props, spec)
        self.widening_rule = widening_rule
        self._sanity_check_widening_rule_preconditions()

    def _sanity_check_widening_rule_preconditions(self) -> None:
        """Validate preconditions for the widening rewrite operation.

        Ensures that settings like partition depth meet the requirements of the current
        optimization strategy.

        Raises:
            Exception: If the required preconditions for widening are not met.
        """
        if self.mnt_props.optimize_partition_depth < self.widening_rule.partition_depth_required:
            msg = (
                f"Widening partition in table '{self.mnt_props.full_name}': The table is configured with "
                f"an '{IceKeeperTblProperty.OPTIMIZE_PARTITION_DEPTH}' of {self.mnt_props.optimize_partition_depth}, "
                f"which is insufficient for the widening rule. The rule requires a minimum "
                f"'optimize_partition_depth' of {self.widening_rule.partition_depth_required}. "
                f"Running a rewrite with an insufficient partition depth may lead to unintended side effects."
            )
            raise Exception(msg)
        if not self.spec.is_partitioned:
            msg = f"Widening partition in table {self.mnt_props.full_name}, however widening partition spec is not partitioned."
            raise Exception(msg)

        if not self.widening_rule.dst_widening:
            msg = f"Widening rule for table {self.mnt_props.full_name} does not match any destination partition spec."
            raise Exception(msg)
        if not self.widening_rule.src_widening:
            msg = f"Widening rule for table {self.mnt_props.full_name} does not match any source partition spec."
            raise Exception(msg)

    def get_min_age_to_optimize(self) -> int:
        """Fetch the minimum age required for applying the widening rule.

        Returns:
            int: Minimum age to optimize based on the widening rule.
        """
        return self.widening_rule.min_age_to_widen

    @override
    def make_data_files_stmt(self) -> str:
        """Generate an SQL query to fetch data files for widening sort optimization.

        This method generates a query to retrieve data files for optimization based on the
        widening rule. The optimization involves the destination partition of the widening rule,
        which means selecting data files from the destination partition `spec_id`.

        For the widening rule, data files from the source partition
        (`widening_rule.src_widening.partition_spec.spec_id`) must also be considered.
        However, not all files from the source partition are selected — only those
        that meet the criteria specified by the widening rule (`widening_rule.filter_expr`).

        Therefore, this query combines:
        1. Data files from the destination partition.
        2. A filtered subset of data files from the source partition, based on
        the widening rule. These source data files are re-labeled with the
        `spec_id` of the destination partition.

        The result is a list of data files associated with the destination partition.
        This ensures that the widening rule includes files from both the destination and relevant
        source partitions for optimization.

        Returns:
            str: SQL query for retrieving and processing data files according to the
                widening rule criteria.
        """
        return f"""
            (
                select
                    {self.spec.make_alias_stmt()},
                    spec_id,
                    content,
                    record_count,
                    file_size_in_bytes,
                    readable_metrics,
                    false as is_data_file_from_widening_src_partition
                from
                    {self.mnt_props.full_name}.files
            )
            union all
            (
                select
                    {self._make_partition_widening_stmt()},
                    {self.widening_rule.dst_widening.partition_spec.spec_id} as spec_id,
                    content,
                    record_count,
                    file_size_in_bytes,
                    readable_metrics,
                    true as is_data_file_from_widening_src_partition
                from
                    {self.mnt_props.full_name}.files
                where
                    spec_id = {self.widening_rule.src_widening.partition_spec.spec_id}
                    and {self.widening_rule.filter_expr}
            )
            """

    def _make_partition_widening_stmt(self) -> str:
        """Generate SQL expression for partition widening based on the rules.

        Returns:
            str: SQL statement for partition aliasing with widening expressions, if applicable.
        """
        dst_widening_expr = self.widening_rule.make_diagnosis_widening_expr_stmt()
        dst_widening_partition = self.widening_rule.dst_widening.partition
        widening_partition_alias = f"{dst_widening_expr} as {dst_widening_partition.partition_field_alias}"

        partition_alias = [
            widening_partition_alias
            if partition == dst_widening_partition
            else f"partition.{partition.transformation.partition_field_escaped} as {partition.partition_field_alias}"
            for partition in self.spec.partition_list
        ]
        return ",".join(partition_alias)


class DataFilesSummary:
    def __init__(
        self, mnt_props: MaintenanceScheduleEntry, spec: PartitionSpecification, spec_id: int, widening_rule: None | WideningRule
    ) -> None:
        """Initialize the data file summary process for optimization diagnostics."""
        self.mnt_props = mnt_props
        self.spec = spec
        self.spec_id = spec_id
        assert mnt_props.optimization_spec.is_binpack() or mnt_props.optimization_spec.is_sorted()
        self.bounds: Bounds = BoundsBinpack(mnt_props)
        self.datafiles: DataFiles = DataFilesBinpack(mnt_props, self.spec)
        if mnt_props.optimization_spec.is_sorted():
            self.bounds = BoundsZorderSort(mnt_props) if mnt_props.optimization_spec.is_zordered else BoundsSort(mnt_props)
            if widening_rule:
                self.datafiles = DataFilesWideningSort(mnt_props, self.spec, widening_rule)
            else:
                self.datafiles = DataFilesSort(mnt_props, self.spec)
        elif widening_rule:
            msg = f"Widening partition in table {mnt_props.full_name}, however table is configured to be binpacked which is not supported yet."
            raise Exception(msg)

    def make_age_filter_stmt(self, min_age_to_optimize: int, max_age_to_optimize: int) -> str:
        """Create a filter to consider only partitions within the specified age range.

        This method generates a filter based on the age of temporal partitions in the table, ensuring
        that only partitions fitting within the age bounds are considered for optimization.

        If no temporal partition exists in the partitioning scheme
        (e.g., bucket(category), truncate(category), or identity(category)), the filter will include all partitions.

        Additionally, if the table uses a temporal column as a sub-partition (e.g., category, days(ingestion_time)),
        the function raises an exception because this schema is not supported for diagnostics. Temporal sub-partitions
        create ambiguities in determining partition age, which can lead to improper optimizations.
        """
        filter_stmt = "1=1"
        if self.spec.is_partitioned:
            # If the base partition (first-level partition) is temporal (e.g., days or hours),
            # create a filter based on the age range.
            if self.spec.get_base_partition().is_temporal_transformation() or self.spec.get_base_partition().is_temporal_column():
                filter_stmt = f"partition_age >= {min_age_to_optimize} and partition_age <= ({max_age_to_optimize})"
            else:
                # The first partition is not temporal. Check if any sub-partitions are temporal.
                # Unsupported case: a temporal partition as a secondary partition.
                for p in self.spec.partition_list:
                    if p.is_temporal_transformation():
                        msg = (
                            f"Unable to optimize table because the temporal partition is not the first one:"
                            f"[{self.spec}] is not supported.Temporal partitions must be the first in order."
                        )
                        raise Exception(msg)
                # If no partitions are temporal, all partitions are assumed to be categorical
                # (e.g., bucket, truncate, or identity). In this case, age filtering does not apply.

        return filter_stmt

    def create_summary_stmt(self) -> str:
        """Generate an SQL query for creating a partition diagnostics summary.

        This summary includes metrics such as correlation factors, number of files
        to optimize and provides flags to determine optimization needs.

        Returns:
            str: SQL query for analyzing partition health and optimization readiness.
        """
        min_file_size_bytes = int(self.mnt_props.target_file_size_bytes * 0.75)
        max_file_size_bytes = int(self.mnt_props.target_file_size_bytes * 1.8)

        num_files_targetted_for_rewrite_threshold = 5

        grouping_stmt = self.spec.make_grouping_stmt()
        data_files_stmt = self.datafiles.make_data_files_stmt()
        lower_bounds_expr = self.bounds.make_lower_bounds_expr_stmt()
        upper_bounds_expr = self.bounds.make_upper_bounds_expr_stmt()
        base_column_name_stmt = self.spec.get_base_partition().partition_field_alias
        corr_threshold_expr = self.bounds.make_corr_threshold_expr_stmt()
        min_age_to_optimize = self.datafiles.get_min_age_to_optimize()
        max_age_to_optimize = self.mnt_props.max_age_to_optimize
        order_by = self.spec.make_order_stmt()
        age_filter_stmt = self.make_age_filter_stmt(min_age_to_optimize, max_age_to_optimize)

        return f"""
            -- Diagnosing partitioned table '{self.mnt_props.full_name}' for optimization
            -- All data files to consider for optimization.
            with data_files as (
                select
                    {grouping_stmt},
                    content,
                    record_count,
                    file_size_in_bytes,
                    readable_metrics,
                    is_data_file_from_widening_src_partition
                from
                    ({data_files_stmt})
            ),
            -- Add the lower/upper bound to each data file. Note these bounds are dependent on the optimization strategy sort/zorder.
            data_files_with_bounds as (
                select
                    {grouping_stmt},
                    content,
                    record_count,
                    file_size_in_bytes,
                    {lower_bounds_expr} as the_lower_bound,
                    {upper_bounds_expr} as the_upper_bound,
                    is_data_file_from_widening_src_partition
                from
                    data_files
            ),
            -- Give data files a rank number based on the ordering of their bounds.
            ranked_data_files as (
                select
                    {grouping_stmt},
                    content,
                    record_count,
                    file_size_in_bytes,
                    row_number() over (partition by {grouping_stmt} order by the_lower_bound) rn1,
                    row_number() over (partition by {grouping_stmt} order by the_upper_bound) rn2,
                    is_data_file_from_widening_src_partition
                from
                    data_files_with_bounds
                where
                    spec_id = {self.spec_id}
            ),
            -- Aggregate the metrics per partition.
            agg_data_files as (
                select
                    {grouping_stmt},

                    dense_rank() over(order by {base_column_name_stmt} desc) as partition_age,

                    {self.spec.make_to_json_stmt()} as partition_desc,

                    -- Aggregations for content = 0 (data files)
                    count_if(content = 0) as n_files,

                    count_if(
                        content = 0 and
                        (file_size_in_bytes < {min_file_size_bytes}
                         or file_size_in_bytes > {max_file_size_bytes})
                    ) as num_files_targetted_for_rewrite,

                    sum(case when content = 0 then record_count end) as n_records,
                    avg(case when content = 0 then file_size_in_bytes end) as avg_file_size,
                    min(case when content = 0 then file_size_in_bytes end) as min_file_size,
                    max(case when content = 0 then file_size_in_bytes end) as max_file_size,
                    sum(case when content = 0 then file_size_in_bytes end) as sum_file_size,

                    count_if(
                        content = 0 and
                        is_data_file_from_widening_src_partition = true
                    ) as num_files_to_widen,

                    -- Calculate correlation factor; defaulting null values to 1
                    -- if only a single file in partition, set corr to 1
                    cast(
                        case when count_if(content = 0) <= 1 then 1
                        else coalesce(corr(rn1, rn2), 1)
                    end as float) as corr,

                    {corr_threshold_expr} as corr_threshold,

                    -- Aggregations for content > 0 (delete files)
                    count_if(content > 0) as n_delete_files,

                    sum(case when content > 0 then record_count else 0 end) as n_delete_records
                from
                    ranked_data_files
                group by
                    {grouping_stmt}
            ),
            -- Add should optimize flags to the aggregate.
            final as (
                select
                    {grouping_stmt},
                    partition_age,
                    partition_desc,
                    n_files,
                    num_files_targetted_for_rewrite,
                    n_records,
                    avg_file_size,
                    min_file_size,
                    max_file_size,
                    sum_file_size,
                    num_files_to_widen,
                    corr,
                    corr_threshold,
                    n_delete_files,
                    n_delete_records,
                    -- Determine necessity for sorting based on correlation threshold or delete files
                    (corr < corr_threshold or n_delete_files > 0 or num_files_to_widen > 0) as should_sort,
                    -- Determine necessity for binpacking based on number of rewritten files or delete files
                    (num_files_targetted_for_rewrite > {num_files_targetted_for_rewrite_threshold} or n_delete_files > 0) as should_binpack
                from
                    agg_data_files
                where
                    {age_filter_stmt}
                order by
                    {order_by}
            )

            select * from final
        """
