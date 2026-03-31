from datetime import datetime, timezone
from typing import Any

import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, DayTransform, HourTransform, IdentityTransform, MonthTransform, Transform
from pyiceberg.types import (
    IcebergType,
    NestedField,
    StringType,
    TimestamptzType,
)
from pyspark.sql.types import Row

from ice_keeper import quote_literal_value, should_escape
from ice_keeper.spec.partition_spec import PartitionSpecification


def test_quote_literal_value() -> None:
    """Test the `quote_literal_value` function to ensure it correctly escapes and quotes string literals."""
    # Test cases
    assert quote_literal_value("simple") == "'simple'"
    assert quote_literal_value("with 'single' quotes") == "'with \\'single\\' quotes'"
    assert quote_literal_value("with \\ backslash") == "'with \\\\ backslash'"
    assert quote_literal_value("complex 'mix' of \\ characters") == "'complex \\'mix\\' of \\\\ characters'"
    assert quote_literal_value("") == "''"  # Empty string
    assert quote_literal_value("'") == "'\\''"  # Single quote only
    assert quote_literal_value("\\") == "'\\\\'"  # Backslash only


@pytest.mark.parametrize(
    ("identifier", "expected"),
    [
        ("valid_identifier", False),  # No special characters, no quoting needed
        ("invalid-identifier", True),  # Contains a special character, should be quoted
        ("`quoted_name`", False),  # Already quoted (starts and ends with backticks)
        ("needs`quoting", True),  # Special character and not properly quoted
        ("normalName123", False),  # Alphanumeric and valid
        ("123_with_number", False),  # Starts with number, but valid identifier
        ("spaces in name", True),  # Contains spaces, needs quoting
        ("", False),  # Empty string, should not trigger quoting
        ("`name_with_underscores`", False),  # Quoted with underscores, valid
        ("name_with_symbols@!", True),  # Contains symbols, should be quoted
    ],
)
def test_should_escape(identifier: str, expected: bool) -> None:  # noqa: FBT001
    assert should_escape(identifier) == expected


def test_identity_partition_spec() -> None:
    field_name = "id"
    partition_name = "id"
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=StringType(), required=False),
        schema_id=1,
    )
    iceberg_partition_spec = PartitionSpec(PartitionField(1, 1001, IdentityTransform(), partition_name))
    catalog = "test"
    ps = PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)
    assert ps.is_partitioned
    par_spec = ps.get_base_partition()
    assert not par_spec.is_temporal_column()

    # In the select data_files the column name will be:
    assert par_spec.partition_field_alias == "id"

    # The diagnosis uses these statements
    assert ps.make_alias_stmt() == "spec_id as spec_id, partition.id as id"
    assert ps.make_grouping_stmt() == "spec_id, id"
    assert ps.make_order_stmt() == "id desc"

    # Once we find partitions to optimize we will feed those values to a function to make a filter expression for the rewrite_data_files procedure:
    # Note we have to apply the bucket transform to the original column in order to get the equality to work.
    partition_to_optimize = Row(id="fullvalue")
    assert par_spec.make_rewrite_data_files_partition_filter_stmt(partition_to_optimize) == "( id = 'fullvalue' )"


def test_identity_partition_spec_need_quotes() -> None:
    field_name = "col+plus|pipe"
    partition_name = "col+plus|pipe"
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=StringType(), required=False),
        schema_id=1,
    )
    iceberg_partition_spec = PartitionSpec(PartitionField(1, 1001, IdentityTransform(), partition_name))
    catalog = "test"
    ps = PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)
    assert ps.is_partitioned
    par_spec = ps.get_base_partition()
    assert not par_spec.is_temporal_column()

    # In the select data_files the column name will be:
    assert par_spec.partition_field_alias == "col_plus_pipe"

    # The diagnosis uses these statements
    assert ps.make_alias_stmt() == "spec_id as spec_id, partition.`col+plus|pipe` as col_plus_pipe"
    assert ps.make_grouping_stmt() == "spec_id, col_plus_pipe"
    assert ps.make_order_stmt() == "col_plus_pipe desc"

    # Once we find partitions to optimize we will feed those values to a function to make a filter expression for the rewrite_data_files procedure:
    # Note we have to apply the bucket transform to the original column in order to get the equality to work.
    partition_to_optimize = Row(col_plus_pipe="fullvalue")
    assert par_spec.make_rewrite_data_files_partition_filter_stmt(partition_to_optimize) == "( `col+plus|pipe` = 'fullvalue' )"


def test_bucket_partition_spec() -> None:
    field_name = "id"
    partition_name = "id_bucket"
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=StringType(), required=False),
        schema_id=1,
    )
    iceberg_partition_spec = PartitionSpec(PartitionField(1, 1001, BucketTransform(4), partition_name))
    catalog = "test"
    ps = PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)
    assert ps.is_partitioned
    par_spec = ps.get_base_partition()
    assert not par_spec.is_temporal_column()

    # In the select data_files the column name will be:
    assert par_spec.partition_field_alias == "id_bucket"

    # The diagnosis uses these statements
    assert ps.make_alias_stmt() == "spec_id as spec_id, partition.id_bucket as id_bucket"
    assert ps.make_grouping_stmt() == "spec_id, id_bucket"
    assert ps.make_order_stmt() == "id_bucket desc"

    # Once we find partitions to optimize we will feed those values to a function to make a filter expression for the rewrite_data_files procedure:
    # Note we have to apply the bucket transform to the original column in order to get the equality to work.
    partition_to_optimize = Row(id_bucket="A00B")
    assert (
        par_spec.make_rewrite_data_files_partition_filter_stmt(partition_to_optimize) == "( test.system.bucket(4, id) = 'A00B' )"
    )


def test_month_partition_spec() -> None:
    field_name = "ts"
    partition_name = "ts_month"
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=TimestamptzType(), required=False),
        schema_id=1,
    )
    iceberg_partition_spec = PartitionSpec(PartitionField(1, 1001, MonthTransform(), partition_name))
    catalog = "test"
    ps = PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)
    assert ps.is_partitioned
    par_spec = ps.get_base_partition()
    assert par_spec.is_temporal_column()

    # In the select data_files the column name will be:
    assert par_spec.partition_field_alias == "ts_month"

    # The diagnosis uses these statements
    assert ps.make_alias_stmt() == "spec_id as spec_id, partition.ts_month as ts_month"
    assert ps.make_grouping_stmt() == "spec_id, partition_time, ts_month"
    assert (
        ps.make_partition_time_alias_stmt()
        == "timestamp '1970-01-01 00:00:00' + (ts_month * interval '1' month) as partition_time"
    )
    assert ps.make_order_stmt() == "ts_month desc"

    # Running the diagnosis and finding the partitions to optimize will return us a resulset like this one:
    # ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
    # ┃ partition_age ┃ ts_month    ┃
    # ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
    # │ 6             │ 672     │
    # │ 7             │ 671     │
    # │ 8             │ 670     │
    # │ 9             │ 669     │
    # └───────────────┴────────────┘

    # Once we find partitions to optimize we will feed those values to a function to make a filter expression for the rewrite_data_files procedure:
    partition_to_optimize = Row(partition_age=6, ts_month=672)
    assert (
        par_spec.make_rewrite_data_files_partition_filter_stmt(partition_to_optimize)
        == "( ts >= date('2026-01-01') and ts < date('2026-01-01') + interval 1 month )"
    )


def test_day_partition_spec() -> None:
    field_name = "ts"
    partition_name = "ts_day"
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=TimestamptzType(), required=False),
        schema_id=1,
    )
    iceberg_partition_spec = PartitionSpec(PartitionField(1, 1001, DayTransform(), partition_name))
    catalog = "test"
    ps = PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)
    assert ps.is_partitioned
    par_spec = ps.get_base_partition()
    assert par_spec.is_temporal_column()

    # In the select data_files the column name will be:
    assert par_spec.partition_field_alias == "ts_day"

    # The diagnosis uses these statements
    assert ps.make_alias_stmt() == "spec_id as spec_id, partition.ts_day as ts_day"
    assert ps.make_grouping_stmt() == "spec_id, partition_time, ts_day"
    assert ps.make_partition_time_alias_stmt() == "ts_day as partition_time"
    assert ps.make_order_stmt() == "ts_day desc"

    # Once we find partitions to optimize we will feed those values to a function to make a filter expression for the rewrite_data_files procedure:
    partition_to_optimize = Row(ts_day=datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc))
    assert (
        par_spec.make_rewrite_data_files_partition_filter_stmt(partition_to_optimize)
        == "( ts >= date('2026-01-10 00:00:00+00:00') and ts < date('2026-01-10 00:00:00+00:00') + interval 1 day )"
    )


def test_hour_partition_spec() -> None:
    field_name = "ts"
    partition_name = "ts_hour"
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=TimestamptzType(), required=False),
        schema_id=1,
    )
    iceberg_partition_spec = PartitionSpec(PartitionField(1, 1001, HourTransform(), partition_name))
    catalog = "test"
    ps = PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)

    assert ps.is_partitioned
    par_spec = ps.get_base_partition()
    assert par_spec.is_temporal_column()

    # In the select data_files the column name will be:
    assert par_spec.partition_field_alias == "ts_hour"

    # The diagnosis uses these statements
    assert ps.make_alias_stmt() == "spec_id as spec_id, partition.ts_hour as ts_hour"
    assert ps.make_grouping_stmt() == "spec_id, partition_time, ts_hour"
    assert (
        ps.make_partition_time_alias_stmt() == "timestamp '1970-01-01 00:00:00' + (ts_hour * interval '1' hour) as partition_time"
    )
    assert ps.make_order_stmt() == "ts_hour desc"

    # Running the diagnosis and finding the partitions to optimize will return us a resulset like this one:
    # ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
    # ┃ partition_age ┃ ts_hour    ┃
    # ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
    # │ 6             │ 450000     │
    # │ 7             │ 450001     │
    # │ 8             │ 450002     │
    # │ 9             │ 450003     │
    # └───────────────┴────────────┘

    # Once we find partitions to optimize we will feed those values to a function to make a filter expression for the rewrite_data_files procedure:
    partition_to_optimize = Row(partition_age=6, ts_hour=450000)
    assert (
        par_spec.make_rewrite_data_files_partition_filter_stmt(partition_to_optimize)
        == "( ts >= timestamp('2021-05-03 00:00:00') and ts < timestamp('2021-05-03 00:00:00') + interval 1 hour )"
    )


def test_unpartition_spec() -> None:
    field_name = "ts"
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=StringType(), required=False),
        schema_id=1,
    )
    iceberg_partition_spec = PartitionSpec()
    catalog = "test"
    ps = PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)

    assert not ps.is_partitioned
    # The diagnosis uses these statements
    assert ps.make_alias_stmt() == "spec_id as spec_id"
    assert ps.make_grouping_stmt() == "spec_id"
    assert ps.make_order_stmt() == ""


@pytest.mark.parametrize(
    argnames=("field_name", "field_type", "transform", "partition_field_value"),
    argvalues=[
        ("ts", TimestamptzType(), HourTransform(), "a string"),
        ("ts", TimestamptzType(), HourTransform(), None),
        (
            "ts",
            TimestamptzType(),
            HourTransform(),
            datetime(2026, 1, 13, tzinfo=timezone.utc),
        ),
    ],
)
def test_invalid_partition_value(
    field_name: str,
    field_type: IcebergType,
    transform: Transform[Any, Any],
    partition_field_value: Any,  # noqa: ANN401
) -> None:
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=field_type, required=False),
        schema_id=1,
    )
    partition_field_name = field_name
    if transform.root:
        partition_field_name = f"{field_name}_{transform.root}"
    iceberg_partition_spec = PartitionSpec(PartitionField(1, 1001, transform, partition_field_name))
    catalog = "test"
    ps = PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)
    partition_field_alias = ps.get_base_partition().partition_field_alias
    data = {partition_field_alias: partition_field_value}
    partition_to_optimize = Row(**data)
    with pytest.raises(TypeError):
        ps.get_base_partition().make_rewrite_data_files_partition_filter_stmt(partition_to_optimize)


@pytest.mark.parametrize(
    argnames=(
        "field_name",
        "field_type",
        "transform",
        "partition_field_name",
        "partition_field_alias",
        "partition_field_value",
        "filter_stmt",
    ),
    argvalues=[
        (
            "ts",
            TimestamptzType(),
            HourTransform(),
            "ts_hour",
            "ts_hour",
            500000,
            "( ts >= timestamp('2027-01-15 08:00:00') and ts < timestamp('2027-01-15 08:00:00') + interval 1 hour )",
        ),
        (
            "col.with.dots",
            TimestamptzType(),
            HourTransform(),
            "col.with.dots_hour",
            "col_with_dots_hour",
            500000,
            "( `col.with.dots` >= timestamp('2027-01-15 08:00:00') and `col.with.dots` < timestamp('2027-01-15 08:00:00') + interval 1 hour )",
        ),
        (
            "ts",
            TimestamptzType(),
            DayTransform(),
            "ts_day",
            "ts_day",
            datetime(2026, 1, 13, tzinfo=timezone.utc),
            "( ts >= date('2026-01-13 00:00:00+00:00') and ts < date('2026-01-13 00:00:00+00:00') + interval 1 day )",
        ),
    ],
)
def test_partition_spec(
    field_name: str,
    field_type: IcebergType,
    partition_field_name: str,
    transform: Transform[Any, Any],
    partition_field_alias: str,
    partition_field_value: Any,  # noqa: ANN401
    filter_stmt: str,
) -> None:
    schema = Schema(
        NestedField(field_id=1, name=field_name, field_type=field_type, required=False),
        schema_id=1,
    )
    iceberg_partition_spec = PartitionSpec(PartitionField(1, 1001, transform, partition_field_name))
    catalog = "test"
    ps = PartitionSpecification.from_pyiceberg(catalog, iceberg_partition_spec, schema)

    assert ps.is_partitioned
    par_spec = ps.get_base_partition()

    data = {partition_field_alias: partition_field_value}
    partition_to_optimize = Row(**data)
    assert par_spec.make_rewrite_data_files_partition_filter_stmt(partition_to_optimize) == filter_stmt
