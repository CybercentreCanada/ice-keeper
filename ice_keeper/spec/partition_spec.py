import json
import logging
import re
from typing import Any

from pydantic import BaseModel
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import DateType, IcebergType, TimestampType, TimestamptzType, TimeType

from ice_keeper import escape_identifier
from ice_keeper.catalog import load_table
from ice_keeper.spec.partition_diagnosis_result import PartitionDiagnosisResult

from .transformation import (
    DayTransformation,
    HourTransformation,
    IdentityTransformation,
    MonthTransformation,
    Transformation,
    YearTransformation,
)

logger = logging.getLogger("ice-keeper")


class Partition(BaseModel):
    """This class provides methods to manipulate the `partition` column obtained from queries on the `.data_files` table in Apache Iceberg.

    For reference, see: https://iceberg.apache.org/docs/latest/spark-queries/#files.

    Iceberg tables allow users to specify partitioning schemes using transformations like
    `hours(ts)`, `hour(ts)`, or `date_hour(ts)`. These are normalized within this class to
    the format `hours(ts)`. Similarly, temporal partitions for years, months, and days are
    uniformly handled.

    The `transformation` field in this class can only represent the following partitioning types:
    - `years`
    - `months`
    - `days`
    - `hours`
    - `truncate`
    - `bucket`
    """

    partition_field_alias: str
    """The normalized (special characters replaced) version of the partition field name.
    It is used as the column name of the aggregation of the partition field in the dignostic query
    """
    source_field_type: IcebergType
    # The type of the source column in the table.
    transformation: Transformation
    # The transformation associated with this partition.

    def is_temporal_transformation(self) -> bool:
        return isinstance(self.transformation, YearTransformation | MonthTransformation | DayTransformation | HourTransformation)

    def is_temporal_column(self) -> bool:
        # Use the field (column) type to determine if it is temporal.
        if self.is_temporal_transformation():
            return True
        if isinstance(self.transformation, IdentityTransformation):
            return self.source_field_type in [TimestampType(), TimestamptzType(), TimeType(), DateType()]
        return False

    def applies_to_diagnosis_row(self, partition_filter: dict[str, Any]) -> bool:
        return self.partition_field_alias in partition_filter

    def sanity_check_partition_field_value(self, partition_filter: dict[str, Any]) -> None:
        partition_field_value = partition_filter[self.partition_field_alias]
        if partition_field_value is None:
            msg = f"Cannot perform rewrite_data_files using a filter criteria that is NULL. The partition field {self.partition_field_alias} cannot be NULL."
            raise ValueError(msg)

    def make_rewrite_data_files_partition_filter_stmt(self, partition_field_value: Any) -> str:  # noqa: ANN401
        """Make filters for the rewrite_data_files from the diagnosis result.

        The diagnosis will return a resultset like this one. Note, that this result set does not necessarliy contain
        columns for all the partitions. ice-keeper groups partitions together using a depth level.

        This function will make a filter only if the column matches a partition.
            ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
            ┃ partition_age ┃ ts_hour    ┃
            ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
            │ 6             │ 450000     │
            │ 7             │ 450001     │
            │ 8             │ 450002     │
            │ 9             │ 450003     │
            └───────────────┴────────────┘
        """
        return self.transformation.make_rewrite_data_files_partition_filter_stmt(partition_field_value)


class PartitionSpecification:
    def __init__(self, partition_list: list[Partition], spec_id: int) -> None:
        self.partition_list = partition_list
        self.spec_id = spec_id
        self.is_partitioned = len(partition_list) > 0

    def __str__(self) -> str:  # noqa: D105
        partition_list_str = "[" + ", ".join(part.model_dump_json() for part in self.partition_list) + "]"
        return f"PartitionSpecification(spec_id={self.spec_id}, partition_list={partition_list_str}, is_partitioned={self.is_partitioned})"

    def get_base_partition(self) -> Partition:
        if len(self.partition_list) == 0:
            msg = "Can't get base partition of un-partitioned tables, check if table is partitioned before calling his method."
            raise RuntimeError(msg)
        return self.partition_list[0]

    def make_to_json_stmt(self) -> str:
        """Create an sql statement that transforms the tables partition into a JSON string.

        This is used by the DataFilesSummary to fit any partition into a partition_desc column.
        """
        struct_fields = [f"'{p.partition_field_alias}', {p.partition_field_alias}" for p in self.partition_list]
        return f"to_json(named_struct( {' , '.join(struct_fields)} ))"

    def sanity_check_partition_field_values(self, partition_filter: dict[str, Any]) -> None:
        if self.is_partitioned:
            applicable_partitions = [
                partition for partition in self.partition_list if partition.applies_to_diagnosis_row(partition_filter)
            ]
            for partition in applicable_partitions:
                partition.sanity_check_partition_field_value(partition_filter)

    def convert_to_rewrite_data_files_partition_filter_stmt(self, partition_diagnosis: PartitionDiagnosisResult) -> str:
        if not self.is_partitioned:
            return "(1 = 1)"
        partition_filter_stmts: list[str] = []
        for partition_filter in partition_diagnosis.partition_filters:
            filters = self.make_rewrite_data_files_partition_filters(partition_filter)
            one_partition_filter = " and ".join(filters)
            partition_filter_stmts.append(one_partition_filter)
        if len(partition_filter_stmts) == 1:
            return partition_filter_stmts[0]
        return " or ".join(f"({stmt})" for stmt in partition_filter_stmts)

    def make_rewrite_data_files_partition_filters(self, partition_filter: dict[str, Any]) -> list[str]:
        """Make filters for the rewrite_data_files from the diagnosis result.

        The diagnosis will return a resultset like this one. Note, that this result set does not necessarliy contain
        columns for all the partitions. ice-keeper groups partitions together using a depth level.

        This function will make a filter only if the column matches a partition.
            ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
            ┃ partition_age ┃ ts_hour    ┃
            ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
            │ 6             │ 450000     │
            │ 7             │ 450001     │
            │ 8             │ 450002     │
            │ 9             │ 450003     │
            └───────────────┴────────────┘
        """
        partition_filter_stmts: list[str] = []
        # If the partition field is not found in the resultset then skip it.
        for partition_field_alias, partition_field_value in partition_filter.items():
            partition = self.get_partition_field_by_alias(partition_field_alias)
            if partition:
                filter_stmt = partition.make_rewrite_data_files_partition_filter_stmt(partition_field_value)
                partition_filter_stmts.append(filter_stmt)
            else:
                msg = "should not happen"
                raise RuntimeError(msg)
        return partition_filter_stmts

    def get_partition_field_by_alias(self, partition_alias: str) -> Partition | None:
        for partition in self.partition_list:
            if partition.partition_field_alias == partition_alias:
                return partition
        return None

    def get_partition_field_by_name(self, partition_name: str) -> Partition | None:
        """Retrieve the partition field matching the partition_name in the list of partitions.

        This method is used by the widening rule to find source and destination partitions in a given table.
        """
        partition_name_escaped = escape_identifier(partition_name)
        return next((p for p in self.partition_list if p.transformation.partition_field_escaped == partition_name_escaped), None)

    def make_order_stmt(self) -> str:
        """Generate an SQL statement for ordering partitions.

        The partitions are ordered in descending order.

        Returns:
            str: SQL statement for ordering partitions by descending order.
        """
        if not self.is_partitioned:
            return ""
        return ",".join([f"{partition.partition_field_alias} desc" for partition in self.partition_list])

    def make_grouping_stmt(self) -> str:
        """Generate an SQL statement for grouping partitions.

        Groups the table data by partition specifications.

        Returns:
            str: A SQL 'GROUP BY' expression that includes the `spec_id` and
                 all partition identifiers.
        """
        grouping_stmts = ["spec_id"]
        if self._is_time_partitioned():
            grouping_stmts.append("partition_time")
        if self.is_partitioned:
            grouping_stmts.extend([partition.partition_field_alias for partition in self.partition_list])
        return ", ".join(grouping_stmts)

    def _is_time_partitioned(self) -> bool:
        """Check if the partition specification includes any temporal partitions."""
        if not self.is_partitioned:
            return False
        return self.get_base_partition().is_temporal_transformation() or self.get_base_partition().is_temporal_column()

    def make_partition_time_alias_stmt(self) -> str:
        """Create a partition_time alias statement.

        Use the time based partition alias for example ts_day and compute the corresponding timestamp
        of the partition given that hours, months and year are an integer since epoch.
        """
        if not self._is_time_partitioned():
            return ""

        partition_time_stmt = ""
        partition_field = f"{self.get_base_partition().partition_field_alias}"
        if self.get_base_partition().is_temporal_transformation():
            if isinstance(self.get_base_partition().transformation, YearTransformation):
                partition_time_stmt = f"timestamp '1970-01-01 00:00:00' + ({partition_field} * interval '1' year)"
            elif isinstance(self.get_base_partition().transformation, MonthTransformation):
                partition_time_stmt = f"timestamp '1970-01-01 00:00:00' + ({partition_field} * interval '1' month)"
            elif isinstance(self.get_base_partition().transformation, DayTransformation):
                # Spark represents this as a date already when returning partitions by day.
                partition_time_stmt = partition_field
            elif isinstance(self.get_base_partition().transformation, HourTransformation):
                partition_time_stmt = f"timestamp '1970-01-01 00:00:00' + ({partition_field} * interval '1' hour)"
            else:
                msg = f"Unsupported temporal transformation: {type(self.get_base_partition().transformation)}"
                raise ValueError(msg)
        elif self.get_base_partition().is_temporal_column():
            partition_time_stmt = partition_field

        return f"{partition_time_stmt} as partition_time"

    def make_alias_stmt(self) -> str:
        """Generate an SQL alias statement for partitions.

        The alias assigns a name/alias to the partition columns for easier referencing.

        Returns:
            str: Alias statement representing the partition columns.
        """
        alias_stmts: list[str] = ["spec_id as spec_id"]
        if self.is_partitioned:
            alias_stmts.extend(
                f"partition.{partition.transformation.partition_field_escaped} as {partition.partition_field_alias}"
                for partition in self.partition_list
            )
        return ", ".join(alias_stmts)

    def make_diagnosis_grouping_stmt(self, optimize_partition_depth: int) -> str:
        """Generate an SQL expression for grouping partitions for optimization.

        Determines the group-by clause for optimizing partitions with a coarser
        granularity, based on `optimize_partition_depth` property.

        Example:
            A table has 3 partitions: `hours(ts), department, action`. If the
            `optimize_partition_depth` is set to 1, grouping occurs by `hours(ts)`.
            This results in larger rewrite operations with fewer commits, while still
            maintaining the desired optimization level.

        Returns:
            str: An SQL grouping statement representing the partitions' group-by keys.
        """
        max_possible_depth = len(self.partition_list)

        # Determine the optimization depth (defaults to 1 if not specified or invalid)
        depth = optimize_partition_depth
        if not depth or depth <= 0:
            depth = 1

        # Ensure the depth does not exceed the maximum partition depth
        depth = min(depth, max_possible_depth)

        # Generate column statements for grouping based on the calculated depth
        group_by_columns = ",".join(self.partition_list[idx].partition_field_alias for idx in range(depth))
        logger.debug("Optimization partition group-by determined (depth=%s): %s", depth, group_by_columns)
        return group_by_columns

    @classmethod
    def _sanitize_alias(cls, input_string: str) -> str:
        """Replaces any character not matching [a-zA-Z0-9_] with an underscore.

        Args:
            input_string (str): The string to sanitize.

        Returns:
            str: The sanitized string.
        """
        return re.sub(r"[^a-zA-Z0-9_]", "_", input_string)

    # Alternative: Using the parent index (more direct)
    @classmethod
    def _build_path_with_escaping(cls, schema: Schema, field_id: int) -> str:
        """Build escaped path using the parent index."""
        path_parts = []
        current_id: int | None = field_id

        # Build path bottom-up
        while current_id is not None:
            field = schema.find_field(current_id)
            path_parts.append(escape_identifier(field.name))
            current_id = schema._lazy_id_to_parent.get(current_id)  # noqa: SLF001

        # Reverse since we built bottom-up
        path_parts.reverse()
        return ".".join(path_parts)

    @classmethod
    def from_pyiceberg(cls, catalog: str, spec: PartitionSpec, schema: Schema) -> "PartitionSpecification":
        """Create a PartitionSpecification from a PyIceberg PartitionSpec.

        Args:
            catalog (str): The catalog to identify the Iceberg table.
            spec (PartitionSpec): PyIceberg PartitionSpec object containing partition metadata.
            schema (Schema): The schema of the table, used for source field lookup.

        Returns:
            PartitionSpecification: The partition specification for the Iceberg table.
        """
        if spec.is_unpartitioned():
            return cls._create_unpartitioned_spec(spec)

        return cls._create_partitioned_spec(catalog, spec, schema)

    @classmethod
    def _create_unpartitioned_spec(cls, spec: PartitionSpec) -> "PartitionSpecification":
        """Create a PartitionSpecification for an unpartitioned table.

        Args:
            spec (PartitionSpec): PyIceberg PartitionSpec object.

        Returns:
            PartitionSpecification: A specification indicating unpartitioned data.
        """
        partition_list: list[Partition] = []

        return PartitionSpecification(partition_list, spec.spec_id)

    @classmethod
    def _create_partitioned_spec(cls, catalog: str, spec: PartitionSpec, schema: Schema) -> "PartitionSpecification":
        """Create a PartitionSpecification for a partitioned table.

        Args:
            catalog (str): The catalog for the table.
            spec (PartitionSpec): PyIceberg PartitionSpec object containing partition information.
            schema (Schema): The schema of the table, used for source field lookup.

        Returns:
            PartitionSpecification: A specification indicating partitioned data.
        """
        partition_list = []
        for partition_field in spec.fields:
            source_field = schema.find_field(partition_field.source_id)
            partition_field_alias = cls._sanitize_alias(partition_field.name)
            source_field_type: IcebergType = source_field.field_type
            source_field_path_escaped = cls._build_path_with_escaping(schema, partition_field.source_id)
            ice_keeper_transform = Transformation.from_pyiceberg(source_field_path_escaped, partition_field, catalog)
            partition = Partition(
                partition_field_alias=partition_field_alias,
                source_field_type=source_field_type,
                transformation=ice_keeper_transform,
            )
            partition_list.append(partition)

        return PartitionSpecification(partition_list, spec.spec_id)


class PartitionSpecifications:
    def __init__(self, default_spec_id: int, partition_specs: dict[int, PartitionSpecification]) -> None:
        self.default_spec_id = default_spec_id
        self.partition_specs = partition_specs

    def __getitem__(self, spec_id: int) -> PartitionSpecification:
        """Return the PartitionSpecification for the given spec_id."""
        return self.partition_specs[spec_id]

    def get_specifications(self) -> list[PartitionSpecification]:
        return list(self.partition_specs.values())

    def get_num_specs(self) -> int:
        return len(self.partition_specs.values())

    @classmethod
    def serialize_partition_by(cls, table: Table) -> str:
        serialized_partition_specs = [spec.model_dump_json() for spec in table.metadata.partition_specs]
        # Build a map of field_id to field_type.
        all_partition_field_struct = table.metadata.specs_struct()
        field_types = {f.field_id: str(f.field_type) for f in all_partition_field_struct.fields}
        data = {
            "default_spec_id": table.metadata.default_spec_id,
            "specs": serialized_partition_specs,
            "field_types": field_types,
        }
        return json.dumps(data, indent=2)

    @classmethod
    def deserialize_partition_by(cls, catalog: str, schema: str, table_name: str) -> "PartitionSpecifications":
        """Parses a serialized JSON of partition specs into individual partition specifications.

        Returns:
            tuple[int, dict[int, PartitionSpecification]]: Default spec ID and a dictionary of PartitionSpecification objects.
        """
        table = load_table(catalog, schema, table_name)
        default_spec_id = table.spec().spec_id
        partition_spec_list = [
            PartitionSpecification.from_pyiceberg(catalog, spec, table.schema()) for spec in table.specs().values()
        ]
        partition_specs = {s.spec_id: s for s in partition_spec_list}
        return PartitionSpecifications(default_spec_id, partition_specs)
