import logging

from jinja2 import Template

from ice_keeper.spec.partition_diagnosis_result import PartitionDiagnosisResult
from ice_keeper.stm import STL
from ice_keeper.table import MaintenanceScheduleEntry
from ice_keeper.task.action.optimization.optimization import WideningRule

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
        # Construct SQL query to retrieve partitions that satisfy the optimization criteria.
        # Aggregate the data file summary into the most granular partitions. Then group together these partitions into groups that are below the optimization_grouping_size_threshold.
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

        rows = STL.sql_and_log(sql, "Find partitions to optimize using dyanmic grouping").collect()
        rows_log_debug(rows, f"Partitions to optimize in using dynamic grouping {self.mnt_props.full_name}")
        return [PartitionDiagnosisResult.from_row(row) for row in rows]

    def _find_partitions_to_optimize_fixed_depth(
        self, summary: PartitionSummary, diagnostic_depth: int
    ) -> list[PartitionDiagnosisResult]:
        # Generate the grouping statement based on the partition structure
        # The optimize_partition_depth determines if sub-partition are used.
        # For example if depth is set to 2, then we will return rows for id_bucket as well
        # Not just for ts_day.
        # Construct SQL query to retrieve partitions that satisfy the optimization criteria
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

        Unpartitioned tables and tables with a fixed depth always use the fixed-depth
        query. Dynamic grouping (depth=-1) is used only for partitioned tables.

        The summary (input) will look like this:
        ┏━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
        ┃ spec_id ┃ partition_time ┃ ts_day     ┃ id_bucket ┃ partition_age ┃ partition_desc                        ┃ n_files ┃ num_files_targetted_for_rewrite ┃ n_records ┃ target_file_size ┃ avg_file_size ┃ min_file_size ┃ max_file_size ┃ sum_file_size ┃ num_files_to_widen ┃ corr ┃ corr_threshold ┃ n_delete_files ┃ n_delete_records ┃ should_sort ┃ should_binpack ┃
        ┡━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
        │ 0       │ 2025-03-03     │ 2025-03-03 │ 2         │ 1             │ {"ts_day":"2025-03-03","id_bucket":2} │ 8       │ 8                               │ 26801     │ 536870912        │ 86378.125     │ 85472         │ 87719         │ 691025        │ 0                  │ 1.0  │ 1.00           │ 0              │ 0                │ False       │ True           │
        │ 0       │ 2025-03-03     │ 2025-03-03 │ 1         │ 1             │ {"ts_day":"2025-03-03","id_bucket":1} │ 8       │ 8                               │ 26305     │ 536870912        │ 84802.25      │ 83224         │ 86171         │ 678418        │ 0                  │ 1.0  │ 1.00           │ 0              │ 0                │ False       │ True           │
        │ 0       │ 2025-03-03     │ 2025-03-03 │ 0         │ 1             │ {"ts_day":"2025-03-03","id_bucket":0} │ 8       │ 8                               │ 26886     │ 536870912        │ 86628.75      │ 83885         │ 88055         │ 693030        │ 0                  │ 1.0  │ 1.00           │ 0              │ 0                │ False       │ True           │
        └─────────┴────────────────┴────────────┴───────────┴───────────────┴───────────────────────────────────────┴─────────┴─────────────────────────────────┴───────────┴──────────────────┴───────────────┴───────────────┴───────────────┴───────────────┴────────────────────┴──────┴────────────────┴────────────────┴──────────────────┴─────────────┴────────────────┘

        The function returns rows like this (fixed depth=1, groups by ts_day only):
        ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃ partition_age ┃ target_file_size ┃ partition_filters     ┃
        ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━┩
        │ 1             │ 536870912        │ [{ts_day:2025-03-03}] │
        └───────────────┴──────────────────┴───────────────────────┘

        If the depth is set to two, then it will output (fixed depth=2):
        ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃ partition_age ┃ target_file_size ┃ partition_filters                   ┃
        ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
        │ 1             │ 536870912        │ [{ts_day:2025-03-03, id_bucket: 2}] │
        │ 1             │ 536870912        │ [{ts_day:2025-03-03, id_bucket: 1}] │
        │ 1             │ 536870912        │ [{ts_day:2025-03-03, id_bucket: 0}] │
        └───────────────┴──────────────────┴─────────────────────────────────────┘

        For unpartitioned tables, partition_filters is absent (defaults to []):
        ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┓
        ┃ partition_age ┃ target_file_size ┃
        ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━┩
        │ 0             │ 536870912        │
        └───────────────┴──────────────────┘

        Args:
            summary (PartitionSummary): An instance that contains summary data about
                the partitions, including metrics to evaluate optimization criteria.

        Returns:
            list[PartitionDiagnosisResult]: A list of PartitionDiagnosisResult, each containing partition information for
                       the partitions marked for optimization.
        """
        use_dynamic_grouping, diagnostic_depth = self._determine_diagnostic_depth_and_mode()

        if use_dynamic_grouping:
            return self._find_partitions_to_optimize_dynamic_grouping(summary, diagnostic_depth)
        return self._find_partitions_to_optimize_fixed_depth(summary, diagnostic_depth)

    def _determine_diagnostic_depth_and_mode(self) -> tuple[bool, int]:
        """This function determines the diagnostic mode (fixed or dynamic) and the depth (num sub-partition levels) at which to aggregate the data files summary.

        A fixed mode means returning one partition per result row, these result rows are then converted into rewrite_data_files procedures. Dynamic means
        partitions are packed together into a list and are then converted into the rewrite_data_files where clause, each of them OR together.
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

        logger.debug("Diagnostic use dynamic grouping %b, group-by determined (depth=%s)", use_dynamic_grouping, diagnostic_depth)
        return (use_dynamic_grouping, diagnostic_depth)
