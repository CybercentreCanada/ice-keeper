import json
import logging
import re

from pydantic import BaseModel
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import VoidTransform
from pyiceberg.types import DateType, IcebergType, StringType, TimestampType, TimestamptzType, TimeType
from pyspark.sql.types import Row

from ice_keeper import escape_identifier
from ice_keeper.catalog import load_table

from .transformation import (
    DayTransformation,
    HourTransformation,
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
        return self.source_field_type in [TimestampType(), TimestamptzType(), TimeType(), DateType()]

    def applies_to_diagnosis_row(self, row: Row) -> bool:
        return self.partition_field_alias in row

    def sanity_check_partition_field_value(self, row: Row) -> None:
        partition_field_value = row[self.partition_field_alias]
        if partition_field_value is None:
            msg = f"Cannot perform rewrite_data_files using a filter criteria that is NULL. The partition field {self.partition_field_alias} cannot be NULL."
            raise ValueError(msg)

    def make_rewrite_data_files_partition_filter_stmt(self, row: Row) -> str:
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
        return self.transformation.make_rewrite_data_files_partition_filter_stmt(row[self.partition_field_alias])


class PartitionSpecification:
    def __init__(self, partition_list: list[Partition], spec_id: int, *, is_partitioned: bool) -> None:
        self.partition_list = partition_list
        self.spec_id = spec_id
        self.is_partitioned = is_partitioned

    def __str__(self) -> str:  # noqa: D105
        partition_list_str = "[" + ", ".join(str(part) for part in self.partition_list) + "]"
        return f"PartitionSpecification(spec_id={self.spec_id}, partition_list={partition_list_str}, is_partitioned={self.is_partitioned})"

    def get_base_partition(self) -> Partition:
        return self.partition_list[0]

    def make_to_json_stmt(self) -> str:
        """Create an sql statement that transforms the tables partition into a JSON string.

        This is used by the DataFilesSummary to fit any partition into a partition_desc column.
        """
        struct_fields = [f"'{p.partition_field_alias}', {p.partition_field_alias}" for p in self.partition_list]
        return f"to_json(named_struct( {' , '.join(struct_fields)} ))"

    def sanity_check_partition_field_values(self, row: Row) -> None:
        if self.is_partitioned:
            applicable_partitions = [partition for partition in self.partition_list if partition.applies_to_diagnosis_row(row)]
            for partition in applicable_partitions:
                partition.sanity_check_partition_field_value(row)

    def convert_to_rewrite_data_files_partition_filter_stmt(self, row: Row) -> str:
        filters = self.make_rewrite_data_files_partition_filters(row)
        return " and ".join(filters)

    def make_rewrite_data_files_partition_filters(self, row: Row) -> list[str]:
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
        if not self.is_partitioned:
            return ["(1 = 1)"]
        # If the partition is not found in the resultset then skip it.
        applicable_partitions = [partition for partition in self.partition_list if partition.applies_to_diagnosis_row(row)]
        return [partition.make_rewrite_data_files_partition_filter_stmt(row) for partition in applicable_partitions]

    def get_partition_field_by_name(self, partition_name: str) -> Partition | None:
        """Retrieve the partition field matching the partition_name in the list of partitions.

        This method is used by the widening rule to find source and destination partitions in a given table.
        """
        partition_name_escaped = escape_identifier(partition_name)
        return next((p for p in self.partition_list if p.transformation.partition_field_escaped == partition_name_escaped), None)

    # @classmethod
    # def make_not_partitioned_spec(cls, spec_id: int = 0) -> "PartitionSpecification":
    #     PartitionSpecification.from_pyiceberg()
    #     return PartitionSpecification([Partition(NotPartitionedTransformation())], spec_id, is_partitioned=False)

    def make_order_stmt(self) -> str:
        """Generate an SQL statement for ordering partitions.

        The partitions are ordered in descending order.

        Returns:
            str: SQL statement for ordering partitions by descending order.
        """
        return ",".join([f"{partition.partition_field_alias} desc" for partition in self.partition_list])

    def make_grouping_stmt(self) -> str:
        """Generate an SQL statement for grouping partitions.

        Groups the table data by partition specifications.

        Returns:
            str: A SQL 'GROUP BY' expression that includes the `spec_id` and
                 all partition identifiers.
        """
        partition_stmts = ",".join([partition.partition_field_alias for partition in self.partition_list])
        return f"spec_id, {partition_stmts}"

    def make_alias_stmt(self) -> str:
        """Generate an SQL alias statement for partitions.

        The alias assigns a name/alias to the partition columns for easier referencing.
        If the table is not partitioned, all rows are treated as belonging to a single
        virtual partition using a fixed value.

        Returns:
            str: Alias statement representing the partition columns.
        """
        if self.is_partitioned:
            stmt = ",".join(
                f"partition.{partition.transformation.partition_field_escaped} as {partition.partition_field_alias}"
                for partition in self.partition_list
            )
        else:
            partition = self.get_base_partition()
            stmt = f"'fix_val' as {partition.partition_field_alias}"
        return stmt

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
            return cls._create_unpartitioned_spec(catalog, spec)

        return cls._create_partitioned_spec(catalog, spec, schema)

    @classmethod
    def _create_unpartitioned_spec(cls, catalog: str, spec: PartitionSpec) -> "PartitionSpecification":
        """Create a PartitionSpecification for an unpartitioned table.

        Args:
            catalog (str): The catalog for the table.
            spec (PartitionSpec): PyIceberg PartitionSpec object.

        Returns:
            PartitionSpecification: A specification indicating unpartitioned data.
        """
        partition_field_alias = "not_partitioned"
        source_field_type: IcebergType = StringType()
        source_field_path_escaped = "not_partitioned"
        partition_field = PartitionField(source_id=1, field_id=1000, name="not_partitioned", transform=VoidTransform())
        ice_keeper_transform = Transformation.from_pyiceberg(source_field_path_escaped, partition_field, catalog)
        partition_list = [
            Partition(
                partition_field_alias=partition_field_alias,
                source_field_type=source_field_type,
                transformation=ice_keeper_transform,
            )
        ]

        return PartitionSpecification(partition_list, spec.spec_id, is_partitioned=False)

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

        return PartitionSpecification(partition_list, spec.spec_id, is_partitioned=True)


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
