import datetime
from datetime import timezone

import pyspark.sql.functions as F  # noqa:N812
import pytest
from pyspark.sql import Row

from ice_keeper.ice_keeper import MaintenanceScheduleRecord
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL
from ice_keeper.table import MaintenanceScheduleEntry
from ice_keeper.task.action.optimization.datafile_summary import DataFilesSummary
from tests.utils import create_generic_test_table, get_updated_mnt_props


def set_mnt_props_age(mnt_props: MaintenanceScheduleEntry, min_age: int, max_age: int) -> MaintenanceScheduleEntry:
    record = mnt_props.record
    # Make a copy of the record before mutating
    record_copy = record.model_copy()
    record_copy.min_age_to_optimize = min_age
    record_copy.max_age_to_optimize = max_age
    row = Row(**record_copy.model_dump(by_alias=True))
    return MaintenanceScheduleRecord.from_row(row).to_entry()


def set_mnt_partition_to_optimize(mnt_props: MaintenanceScheduleEntry, min_p: str, max_p: str) -> MaintenanceScheduleEntry:
    record = mnt_props.record
    # Make a copy of the record before mutating
    record_copy = record.model_copy()
    record_copy.min_partition_to_optimize = min_p
    record_copy.max_partition_to_optimize = max_p
    row = Row(**record_copy.model_dump(by_alias=True))
    return MaintenanceScheduleRecord.from_row(row).to_entry()


def get_partition_time_from_summary(mnt_props: MaintenanceScheduleEntry) -> Row | None:
    spec_id = 0
    spec = mnt_props.partition_specs[spec_id]
    widening_rule = None
    datafiles_summary = DataFilesSummary(mnt_props, spec, spec_id, widening_rule)
    sql = datafiles_summary.create_summary_stmt()
    df = STL.sql_and_log(sql, "Retrieve rows from partition summary")
    rows = df.select(F.expr("min(partition_time)").alias("oldest"), F.expr("max(partition_time)").alias("most_recent")).take(1)
    if len(rows) == 0:
        return None
    return rows[0]


dt_first_utc = datetime.datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
dt_second_utc = datetime.datetime(2025, 12, 2, 0, 0, 0, tzinfo=timezone.utc)
dt_third_utc = datetime.datetime(2025, 12, 3, 0, 0, 0, tzinfo=timezone.utc)

dt_first_18h_utc = datetime.datetime(2025, 12, 1, 18, 0, 0, tzinfo=timezone.utc)
dt_first_19h_utc = datetime.datetime(2025, 12, 1, 19, 0, 0, tzinfo=timezone.utc)
dt_first_20h_utc = datetime.datetime(2025, 12, 1, 20, 0, 0, tzinfo=timezone.utc)


d_first = datetime.date(2025, 12, 1)
d_second = datetime.date(2025, 12, 2)
d_third = datetime.date(2025, 12, 3)

dt_first = datetime.datetime(2025, 12, 1, 0, 0, 0)  # noqa: DTZ001
dt_second = datetime.datetime(2025, 12, 2, 0, 0, 0)  # noqa: DTZ001
dt_third = datetime.datetime(2025, 12, 3, 0, 0, 0)  # noqa: DTZ001

dt_first_18h = datetime.datetime(2025, 12, 1, 18, 0, 0)  # noqa: DTZ001
dt_first_19h = datetime.datetime(2025, 12, 1, 19, 0, 0)  # noqa: DTZ001
dt_first_20h = datetime.datetime(2025, 12, 1, 20, 0, 0)  # noqa: DTZ001


@pytest.mark.integration
def test_summary_age_day(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "binpack"
    properties: dict[str, str] = {}

    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[dt_first_utc, dt_second_utc, dt_third_utc],
        partitioned_by=partitioned_by,
        optimization_strategy=optimization_strategy,
        properties=properties,
    )

    mnt_props = get_updated_mnt_props()

    # Consider all partitions (age 1 is current partition)
    mnt_props = set_mnt_props_age(mnt_props, 1, 2000)
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_third

    # Do not process most recent partition
    mnt_props = set_mnt_props_age(mnt_props, 2, 2000)
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_second

    # Only partition age 2 to 5
    mnt_props = set_mnt_props_age(mnt_props, 2, 2)
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_second
    assert row.most_recent == d_second


@pytest.mark.integration
def test_summary_identity_day(executor: TaskExecutor) -> None:
    partitioned_by = "ts"
    optimization_strategy = "binpack"
    properties: dict[str, str] = {}

    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[dt_first_utc, dt_second_utc, dt_third_utc],
        partitioned_by=partitioned_by,
        optimization_strategy=optimization_strategy,
        properties=properties,
    )

    mnt_props = get_updated_mnt_props()

    # Consider all partitions
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0d", "2000d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first
    assert row.most_recent == dt_third

    # Do not process most recent partition
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "2000d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first
    assert row.most_recent == dt_second

    # skip current partition and up to 2d
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "2d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first
    assert row.most_recent == dt_second

    # skip current partition and up to 1d
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "1d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_second
    assert row.most_recent == dt_second

    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0d", "0d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_third
    assert row.most_recent == dt_third

    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0M", "0M")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first
    assert row.most_recent == dt_third


@pytest.mark.integration
def test_summary_partition_day(executor: TaskExecutor) -> None:
    partitioned_by = "days(ts)"
    optimization_strategy = "binpack"
    properties: dict[str, str] = {}
    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[dt_first_utc, dt_second_utc, dt_third_utc],
        partitioned_by=partitioned_by,
        optimization_strategy=optimization_strategy,
        properties=properties,
    )

    mnt_props = get_updated_mnt_props()

    # Consider all partitions
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0d", "2000d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_third

    # Do not process most recent partition
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "2000d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_second

    # skip current partition and up to 2d
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "2d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_second

    # skip current partition and up to 1d
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "1d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_second
    assert row.most_recent == d_second

    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0d", "0d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_third
    assert row.most_recent == d_third

    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0M", "0M")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_third


@pytest.mark.integration
def test_summary_partition_hour(executor: TaskExecutor) -> None:
    partitioned_by = "hours(ts)"
    optimization_strategy = "binpack"
    properties: dict[str, str] = {}
    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[dt_first_18h_utc, dt_first_19h_utc, dt_first_20h_utc],
        partitioned_by=partitioned_by,
        optimization_strategy=optimization_strategy,
        properties=properties,
    )

    mnt_props = get_updated_mnt_props()

    # Consider all partitions
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0h", "2000h")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first_18h
    assert row.most_recent == dt_first_20h

    # Do not process most recent partition
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1h", "2000h")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first_18h
    assert row.most_recent == dt_first_19h

    # skip current partition and up to 2h
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1h", "2h")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first_18h
    assert row.most_recent == dt_first_19h

    # skip current partition and up to 1h
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1h", "1h")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first_19h
    assert row.most_recent == dt_first_19h

    # Assert grabbing all partitions
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0d", "2d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first_18h
    assert row.most_recent == dt_first_20h

    # Assert grabbing all partitions
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0d", "0d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == dt_first_18h
    assert row.most_recent == dt_first_20h

    # Assert no partitions to optimize 1 day ago
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "1d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest is None
    assert row.most_recent is None

    # Assert no partitions to optimize 1 day ago
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "10d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest is None
    assert row.most_recent is None


@pytest.mark.integration
def test_summary_partition_month(executor: TaskExecutor) -> None:
    """Test partition filtering with month (M) intervals on a month-partitioned table."""
    partitioned_by = "month(ts)"
    optimization_strategy = "binpack"
    properties: dict[str, str] = {}
    # Insert data into 3 months: Oct, Nov, Dec 2025
    oct_utc = datetime.datetime(2025, 10, 15, 0, 0, 0, tzinfo=timezone.utc)
    nov_utc = datetime.datetime(2025, 11, 15, 0, 0, 0, tzinfo=timezone.utc)
    dec_utc = datetime.datetime(2025, 12, 15, 0, 0, 0, tzinfo=timezone.utc)
    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[oct_utc, nov_utc, dec_utc],
        partitioned_by=partitioned_by,
        optimization_strategy=optimization_strategy,
        properties=properties,
    )

    mnt_props = get_updated_mnt_props()

    # All months: 0M to 100M
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0M", "100M")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.most_recent is not None
    assert row.oldest is not None

    # Skip most recent month
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1M", "100M")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    # most_recent should be Nov (skipped Dec)
    assert row.most_recent == datetime.datetime(2025, 11, 1, 0, 0, 0)  # noqa: DTZ001
    assert row.oldest == datetime.datetime(2025, 10, 1, 0, 0, 0)  # noqa: DTZ001

    # Only current month
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0M", "0M")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == row.most_recent


@pytest.mark.integration
def test_summary_partition_year(executor: TaskExecutor) -> None:
    """Test partition filtering with year (Y) intervals on a year-partitioned table."""
    partitioned_by = "year(ts)"
    optimization_strategy = "binpack"
    properties: dict[str, str] = {}
    # Insert data into 3 years: 2023, 2024, 2025
    y2023_utc = datetime.datetime(2023, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
    y2024_utc = datetime.datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
    y2025_utc = datetime.datetime(2025, 6, 15, 0, 0, 0, tzinfo=timezone.utc)

    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[y2023_utc, y2024_utc, y2025_utc],
        partitioned_by=partitioned_by,
        optimization_strategy=optimization_strategy,
        properties=properties,
    )

    mnt_props = get_updated_mnt_props()

    # All years
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0Y", "100Y")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest is not None
    assert row.most_recent is not None

    # Skip most recent year
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1Y", "100Y")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.most_recent is not None
    assert row.oldest is not None

    # Only current year
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "0Y", "0Y")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == row.most_recent


@pytest.mark.integration
def test_summary_partition_negative_offset(executor: TaskExecutor) -> None:
    """Test that negative offsets extend the window beyond the most recent partition."""
    partitioned_by = "days(ts)"
    optimization_strategy = "binpack"
    properties: dict[str, str] = {}
    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[dt_first_utc, dt_second_utc, dt_third_utc],
        partitioned_by=partitioned_by,
        optimization_strategy=optimization_strategy,
        properties=properties,
    )

    mnt_props = get_updated_mnt_props()

    # Negative min offset: -1d means include 1 day into the future from the reference point.
    # Since there are no future partitions, this should still include the most recent one.
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "-1d", "2000d")
    row = get_partition_time_from_summary(mnt_props)
    assert row
    assert row.oldest == d_first
    assert row.most_recent == d_third


@pytest.mark.integration
def test_summary_partition_mismatched_units_rejected(executor: TaskExecutor) -> None:
    """Test that mismatched units between min and max partition raise a ValueError."""
    partitioned_by = "days(ts)"
    optimization_strategy = "binpack"
    properties: dict[str, str] = {}
    create_generic_test_table(
        executor=executor,
        partitions_to_insert_into=[dt_first_utc],
        partitioned_by=partitioned_by,
        optimization_strategy=optimization_strategy,
        properties=properties,
    )

    mnt_props = get_updated_mnt_props()

    # Mismatched units: min in days, max in months
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1d", "3M")
    with pytest.raises(ValueError, match="must use the same unit"):
        get_partition_time_from_summary(mnt_props)

    # Mismatched units: min in hours, max in years
    mnt_props = set_mnt_partition_to_optimize(mnt_props, "1h", "2Y")
    with pytest.raises(ValueError, match="must use the same unit"):
        get_partition_time_from_summary(mnt_props)
