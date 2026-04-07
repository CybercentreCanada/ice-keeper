import logging

from ice_keeper import IceKeeperTblProperty
from ice_keeper.spec.partition_diagnosis_result import PartitionDiagnosisResult

from .partition_spec import Partition, PartitionSpecification, PartitionSpecifications

logger = logging.getLogger("ice-keeper")


class WideningPartition:
    def __init__(self, partition_spec: PartitionSpecification, partition: Partition) -> None:
        self.partition_spec = partition_spec
        self.partition = partition


class WideningRule:
    """Represents a rule for widening partitions in a table.

    This class manages the process of widening partitions in an Apache Iceberg table from one level to another,
    for example, from a daily partition ("partition.timestamp_day") to a monthly partition ("partition.timestamp_month").

    Attributes:
        src_partition_name (str): The name of the source partition to be widened.
        dst_partition_name (str): The name of the destination (widened) partition.
        required_fixed_columns (list[str]): A list of column names that must not contain NULL values
            before the partition can be widened. This ensures data integrity during the operation.
        filter_expr (str): An expression used to filter rows. Only the rows matching this condition
            will be considered for widening. For example, "partition.category in ('leading', 'lagging')".
        partition_specs (dict[int, "PartitionSpecification"]): A dictionary of partition specifications used
            to find the source and destination partitions for the widening operation.
        partition_depth_required (int): The minimum number of sub-partition levels required in the source
            and destination partition specifications to proceed with widening. This value will be tested against the table's
            optimize_partition_depth, making sure the rewrite_data_files includes these required sub partitions.
        src_widening (Optional[WideningPartition]): Represents the source partition to widen.
            Found by matching `src_partition_name` in the `partition_specs`.
        dst_widening (Optional[WideningPartition]): Represents the destination partition after widening.
            Found by matching `dst_partition_name` in the `partition_specs`.

    Methods:
        __init__:
            Initializes the WideningRule with all the necessary parameters, validates the required
            columns, and locates matching source and destination partitions in the `partition_specs`.

    Args:
        src_partition_name (str): Name of the source partition (e.g., "partition.timestamp_day").
        dst_partition_name (str): Name of the destination partition (e.g., "partition.timestamp_month").
        required_fixed_columns (list[str]): List of mandatory columns that must be non-NULL
            to ensure the integrity of the widening process.
        filter_expr (str): A filter expression to select rows for processing (e.g.,
            "partition.category in ('leading', 'lagging')").
        partition_specs (dict[int, "PartitionSpecification"]): The data structure containing details of all available
            partition specifications, needed to identify the source and destination partitions.

    Notes:
        - Before performing a widening operation, the class checks that all `required_fixed_columns`
          contain non-NULL values. Failure to do so results in a warning with a suggested SQL statement to
          identify the rows with missing values.
        - Example warning message:
            "Partition about to be widened, however, some required columns in the table have null values.
            All required columns ['partition.category'] should have a value set for the given date range.
            This SQL statement should not return any rows: <SQL_QUERY>."
    """

    def __init__(
        self,
        partition_specs: PartitionSpecifications,
        src_partition_name: str,
        dst_partition_name: str,
        required_fixed_columns: list[str],
        filter_expr: str,
    ) -> None:
        self.partition_specs = partition_specs
        self.required_fixed_columns = [
            self.strip_partition_prefix(required, IceKeeperTblProperty.WIDENING_RULE_REQUIRED_PARTITION_COLUMNS)
            for required in required_fixed_columns
        ]
        self.filter_expr = filter_expr
        self.partition_depth_required = len(self.required_fixed_columns) + 1
        self.src_partition_name = self.strip_partition_prefix(
            src_partition_name, IceKeeperTblProperty.WIDENING_RULE_SRC_PARTITION
        )
        self.dst_partition_name = self.strip_partition_prefix(
            dst_partition_name, IceKeeperTblProperty.WIDENING_RULE_DST_PARTITION
        )
        self.src_widening = self.find_matching_partition(self.src_partition_name)
        self.dst_widening = self.find_matching_partition(self.dst_partition_name)
        self.spec_id = self.dst_widening.partition_spec.spec_id

    def strip_partition_prefix(self, value: str, argument: str) -> str:
        if not value.startswith("partition."):
            msg = f"Expected 'partition.' prefix in {argument}: {value}"
            raise ValueError(msg)
        return value[len("partition.") :]

    def __str__(self) -> str:  # noqa: D105
        return (
            f"WideningRule("
            f"src_partition_name='{self.src_partition_name}', "
            f"dst_partition_name='{self.dst_partition_name}', "
            f"required_fixed_columns={self.required_fixed_columns}, "
            f"filter_expr='{self.filter_expr}', "
            ")"
        )

    def make_diagnosis_widening_expr_stmt(self) -> str:
        srt_widening_partition = self.src_widening.partition
        dst_widening_partition = self.dst_widening.partition
        return dst_widening_partition.transformation.make_diagnosis_widening_expr_stmt(srt_widening_partition.transformation)

    def check_partition_has_all_fixed_columns(self, spec: PartitionSpecification) -> bool:
        """Tests that the provided partition specs has all the required sub partitions."""
        return all(spec.get_partition_field_by_name(fixed_col) for fixed_col in self.required_fixed_columns)

    def find_matching_partition(self, partition_name: str) -> WideningPartition:
        """Find a partition spec which has the required sub partitions and matches the partition name."""
        matching_specs = []
        for spec in self.partition_specs.get_specifications():
            partition = spec.get_partition_field_by_name(partition_name)
            if partition:
                matching_specs.append(spec)
        if len(matching_specs) == 0:
            msg = f"Could not find any partition specification with a partition named [partition.{partition_name}]"
            raise ValueError(msg)
        for spec in matching_specs:
            # Check if this partition spec has a partition matching the given rule.
            if self.check_partition_has_all_fixed_columns(spec):
                partition = spec.get_partition_field_by_name(partition_name)
                if partition:
                    return WideningPartition(spec, partition)
        comma_separated_columns = ", ".join(f"partition.{col}" for col in self.required_fixed_columns)
        msg = (
            f"Found partition specification with partition named [partition.{partition_name}]. "
            f"But none of them had the required partitions [{comma_separated_columns}]"
        )
        raise ValueError(msg)

    def make_widening_validation_filter(self, partition_diagnosis: PartitionDiagnosisResult) -> str:
        """Generate a WHERE clause to validate that required columns do not contain NULL values.

        When performing a widening operation with `rewrite_data_files`, a filter is applied
        to ensure no rows in the specified date range contain NULL values in the required columns
        (e.g., `_lag = 'lagging'`). Rows with NULL values in required columns may lead to undefined
        behavior during the widening process.

        This method returns a SQL WHERE clause to check for NULL values in the required columns
        within the date range of the partition being widened.

        Returns:
        str: A WHERE clause, e.g.,
        "(timestamp >= '2025-02-01' AND timestamp < '2025-02-01' + INTERVAL 1 MONTH) AND (_lag IS NULL)"
        """
        assert self.dst_widening
        assert len(self.dst_widening.partition.partition_field_alias) > 0
        partition_field_alias = self.dst_widening.partition.partition_field_alias
        if len(partition_diagnosis.partition_filters) != 1:
            msg = (
                "Widening validation requires exactly one partition filter, "
                f"but got {len(partition_diagnosis.partition_filters)}. "
                "Multiple partition filters are not supported by this widening rule."
            )
            raise ValueError(msg)
        partition_filter = partition_diagnosis.partition_filters[0]
        partition_field_value = partition_filter[partition_field_alias]
        dst_partition_date_range = self.dst_widening.partition.to_sql_predicate(partition_field_value)
        null_check_stmts = [f"{required_column} is null" for required_column in self.required_fixed_columns]
        required_columns_not_null = " or ".join(null_check_stmts)
        return f"({dst_partition_date_range}) and ({required_columns_not_null})"
