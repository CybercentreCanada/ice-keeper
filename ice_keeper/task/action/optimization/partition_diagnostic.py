import logging

from jinja2 import Template

from ice_keeper.spec import PartitionDiagnosisResult, WideningRule
from ice_keeper.stm import STL
from ice_keeper.table import MaintenanceScheduleEntry

from .partition_summary import PartitionSummary, rows_log_debug

logger = logging.getLogger("ice-keeper")


class PartitionDiagnosis:
    """Diagnoses the health of partitions and identifies those that need optimization.

    This class is responsible for identifying partitions in a table
    that require optimization. The diagnosis process is based on metadata
    from the `PartitionSummary` and optimization configurations defined
    in the maintenance properties.
    """

    def __init__(self, mnt_props: MaintenanceScheduleEntry, spec_id: int, widening_rule: None | WideningRule = None) -> None:
        """Initialize a `PartitionDiagnosis` instance.

        Args:
            mnt_props (MaintenanceScheduleEntry): Maintenance properties that provide
                details about the table and its optimization configuration.
            spec_id (int): The identifier of the partition specification used for
                partition diagnostics.
            widening_rule (WideningRule | None): An optional rule that determines how partitions should be widened during optimization.
        """
        self.mnt_props = mnt_props
        self.spec = mnt_props.partition_specs[spec_id]
        self.widening_rule = widening_rule

    def _find_partitions_to_optimize_dynamic_grouping(
        self, summary: PartitionSummary, diagnostic_depth: int
    ) -> list[PartitionDiagnosisResult]:
        """Find partitions to optimize using dynamic grouping based on cumulative size.

        Instead of producing one result row per partition (as fixed-depth does),
        this method packs multiple partitions into size-bounded groups. It computes
        a running cumulative size over partitions (ordered by age, then descending
        size) and assigns a group label each time the cumulative total crosses the
        ``optimization_grouping_size_bytes`` threshold. Each group becomes a single
        rewrite_data_files call whose WHERE clause OR's together all the partitions
        in that group.

        The query pipeline is:
        1. **summary** Selects the most granular sub-partitions from the summary
           view at ``diagnostic_depth``.
        2. **running_partition_size** Filters to partitions needing optimization
           (binpack or sort) and computes a running cumulative size within each
           (partition_age, target_file_size) window.
        3. **labeled_partition_groupings** Assigns a bucket label by dividing the
           cumulative size by the grouping threshold.
        4. **final** Aggregates partition column values into a
           ``partition_filters`` array per group.

        Args:
            summary: The partition summary containing the data files metrics view.
            diagnostic_depth: Number of partition levels to include in the grouping.

        Returns:
            A list of diagnosis results, each representing a dynamically grouped
            set of partitions to optimize together.
        """
        sql_template = Template("""
                -- Identifying partitions to optimize for table {{ full_name }}
                with summary as (
                    select
                        partition_age,
                        {{ list_of_all_partition_alias_stmt }},
                        target_file_size,
                        sum_file_size as subpartition_size,
                        should_sort,
                        should_binpack
                    from
                        {{ summary_before_view_name }}
                ),
                -- Running cumulative partition size ordered by age
                running_partition_size as (
                    select
                        partition_age,
                        {{ list_of_all_partition_alias_stmt }},
                        target_file_size,
                        subpartition_size,
                        sum(subpartition_size) over (
                            partition by partition_age, target_file_size
                            order by partition_age asc, target_file_size desc, subpartition_size desc
                            rows between unbounded preceding and current row
                        ) as running_subpartition_size
                    from
                        summary
                    where
                        {% if is_binpack %}
                        should_binpack = true
                        {% else %}
                        should_sort = true
                        {% endif %}
                ),
                /*
                Computes a cumulative sum of subpartition_size ordered by partition_age (same-age rows stay together), with larger partitions first as tiebreaker
                running_subpartition_size - subpartition_size gives the total before the current row
                floor(... / optimization_grouping_size_threshold) increments the id each time the preceding rows have filled a 16GB bucket
                */
                labeled_partition_groupings as (
                    select
                        partition_age,
                        {{ list_of_all_partition_alias_stmt }},
                        target_file_size,
                        format_string('partition_age:%d target_file_size:%d optimization_group:%d',
                            partition_age,
                            target_file_size,
                            floor((running_subpartition_size - subpartition_size) / {{ optimization_grouping_size_threshold }})
                        ) as dynamic_optimization_grouping_label
                    from
                        running_partition_size
                ),
                final as (
                    select
                        partition_age,
                        target_file_size,
                        dynamic_optimization_grouping_label
                        {% if is_partitioned %}
                            , array_sort(array_distinct(collect_list(struct({{ list_of_all_partition_alias_stmt }})))) as partition_filters
                        {% endif %}
                    from
                        labeled_partition_groupings
                    group by
                        partition_age,
                        target_file_size,
                        dynamic_optimization_grouping_label
                    order by
                        partition_age asc,
                        target_file_size desc,
                        dynamic_optimization_grouping_label
                )
                select * from final
        """)

        # Render the SQL query with all required variables
        sql = sql_template.render(
            is_partitioned=self.spec.is_partitioned,
            is_binpack=self.mnt_props.optimization_spec.is_binpack(),
            list_of_all_partition_alias_stmt=self.spec.make_diagnosis_grouping_stmt(diagnostic_depth),
            summary_before_view_name=summary.summary_before_view_name,
            full_name=self.mnt_props.full_name,
            optimization_grouping_size_threshold=self.mnt_props.optimization_grouping_size_bytes,
        )

        rows = STL.sql_and_log(sql, "Find partitions to optimize using dynamic grouping").collect()
        rows_log_debug(rows, f"Partitions to optimize using dynamic grouping in {self.mnt_props.full_name}")
        return [PartitionDiagnosisResult.from_row(row) for row in rows]

    def _find_partitions_to_optimize_fixed_depth(
        self, summary: PartitionSummary, diagnostic_depth: int
    ) -> list[PartitionDiagnosisResult]:
        """Find partitions to optimize using a fixed partition depth for grouping.

        Produces one result row per distinct combination of partition columns up
        to ``diagnostic_depth``. Each row becomes a single rewrite_data_files call.

        For example, with partitions [ts_day, id_bucket]:
        - depth=1 groups by ts_day only → one row per day (coarser, fewer calls).
        - depth=2 groups by ts_day and id_bucket → one row per (day, bucket).

        For partitioned tables, the result includes a ``partition_filters`` column
        containing an array of structs with the partition column values. For
        unpartitioned tables, only ``partition_age`` and ``target_file_size`` are
        returned.

        Args:
            summary: The partition summary containing the data files metrics view.
            diagnostic_depth: Number of partition levels to include in the SQL
                GROUP BY clause.

        Returns:
            A list of diagnosis results, one per partition group, ordered by
            ascending partition age.
        """
        sql_template = Template("""
                -- Identifying partitions to optimize for table {{ full_name }}
                select
                    partition_age,
                    target_file_size
                    {% if is_partitioned %}
                    , array_sort(array_distinct(collect_list(struct({{ depth_grouping_stmt }})))) as partition_filters
                    {% endif %}
                from
                    {{ summary_before_view_name }}
                where
                    {% if is_binpack %}
                    should_binpack = true
                    {% else %}
                    should_sort = true
                    {% endif %}
                group by
                    partition_age,
                    target_file_size
                    {% if is_partitioned %}
                    , {{ depth_grouping_stmt }}
                    {% endif %}
                order by
                    partition_age asc
                """)
        # Render the SQL query with all required variables
        sql = sql_template.render(
            is_partitioned=self.spec.is_partitioned,
            is_binpack=self.mnt_props.optimization_spec.is_binpack(),
            depth_grouping_stmt=self.spec.make_diagnosis_grouping_stmt(diagnostic_depth),
            summary_before_view_name=summary.summary_before_view_name,
            full_name=self.mnt_props.full_name,
        )

        # Log the SQL query for debugging
        rows = STL.sql_and_log(sql, "Find partitions to optimize").collect()
        rows_log_debug(rows, f"Partitions to optimize in {self.mnt_props.full_name}")
        return [PartitionDiagnosisResult.from_row(row) for row in rows]

    def find_partitions_to_optimize(self, summary: PartitionSummary) -> list[PartitionDiagnosisResult]:
        """Find partitions that need optimization based on the summary metrics.

        Entry point that delegates to either the fixed-depth or dynamic-grouping
        query depending on the diagnostic mode resolved by
        ``_determine_diagnostic_depth_and_mode``.

        The summary view is expected to contain per-partition rows with
        ``should_binpack`` / ``should_sort`` flags. This method selects
        partitions where the relevant flag is ``true`` (binpack or sort,
        depending on ``mnt_props.optimization_spec``).

        Each returned ``PartitionDiagnosisResult`` contains:
        - ``partition_age`` and ``target_file_size`` — grouping keys.
        - ``partition_filters`` — an array of partition column structs
          identifying which partitions to rewrite (empty list for
          unpartitioned tables).

        See ``_find_partitions_to_optimize_fixed_depth`` and
        ``_find_partitions_to_optimize_dynamic_grouping`` for details on
        each strategy.

        Args:
            summary: The partition summary containing data file metrics and
                the ``should_binpack`` / ``should_sort`` flags per partition.

        Returns:
            A list of diagnosis results for partitions marked for optimization,
            ordered by ascending partition age.
        """
        use_dynamic_grouping, diagnostic_depth = self._determine_diagnostic_depth_and_mode()

        if use_dynamic_grouping:
            return self._find_partitions_to_optimize_dynamic_grouping(summary, diagnostic_depth)
        return self._find_partitions_to_optimize_fixed_depth(summary, diagnostic_depth)

    def _determine_diagnostic_depth_and_mode(self) -> tuple[bool, int]:
        """Determine the diagnostic mode (fixed or dynamic) and the partition depth for aggregating the data files summary.

        The depth controls how many partition levels are used in the SQL GROUP BY
        when diagnosing partitions. For example, with partitions [ts_day, id_bucket]:
        - depth=1 groups by ts_day only (coarser, fewer result rows)
        - depth=2 groups by ts_day and id_bucket (finer, more result rows)
        - depth=0 means no partition grouping (unpartitioned tables)

        The mode determines how result rows map to rewrite_data_files procedures:
        - Fixed mode: each result row becomes one rewrite_data_files call.
        - Dynamic mode: partitions are packed into a list per result row and
          OR'd together in the rewrite_data_files WHERE clause, grouped by a
          cumulative size threshold (optimization_grouping_size_bytes).

        Resolution logic:
        - If ``optimize_partition_depth`` is -1 and no widening rule applies,
          dynamic grouping is used at the maximum partition depth.
        - If ``optimize_partition_depth`` is -1 but a widening rule is present,
          fixed mode is used at the maximum depth (dynamic grouping is
          incompatible with widening rules).
        - Otherwise, fixed mode is used at ``min(optimize_partition_depth, max_depth)``.
        - Unpartitioned tables always use fixed mode with depth=0.

        Returns:
            tuple[bool, int]: A tuple of (use_dynamic_grouping, diagnostic_depth).
        """
        max_possible_depth = len(self.spec.partition_list)
        # Ensure the depth does not exceed the maximum partition depth
        # Use user's depth unless it's -1 (dynamic grouping)
        user_requested_depth = self.mnt_props.optimize_partition_depth
        if self.mnt_props.optimize_partition_depth == -1:
            user_requested_depth = max_possible_depth
        else:
            user_requested_depth = min(user_requested_depth, max_possible_depth)

        # Default behaviour is to use the users depth in fixed mode.
        use_dynamic_grouping = False
        diagnostic_depth = user_requested_depth
        if not self.spec.is_partitioned:
            # Unless the table is not partitioned at all, then depth should be zero. No aggregation of data files summary.
            diagnostic_depth = 0
        elif self.mnt_props.optimize_partition_depth == -1 and not self.widening_rule:
            # If the user requested dynamic grouping and the partition we are diagnosing does not have a widening rule
            # then we can use dynamic grouping. Dynamic grouping is incompatible with widening rules.
            # If that is the case, diagnose at the most refined granularity and aggregate list of partitions (dynamic grouping)
            use_dynamic_grouping = True

        logger.debug("Diagnostic use dynamic grouping %s, group-by determined (depth=%s)", use_dynamic_grouping, diagnostic_depth)
        return (use_dynamic_grouping, diagnostic_depth)
