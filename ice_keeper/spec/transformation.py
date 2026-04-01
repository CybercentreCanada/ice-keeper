import logging
from abc import abstractmethod
from datetime import date, datetime, timezone
from typing import Any

from pydantic import BaseModel
from pyiceberg.partitioning import PartitionField
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    UnknownTransform,
    VoidTransform,
    YearTransform,
)

from ice_keeper import escape_identifier

logger = logging.getLogger("ice-keeper")


class Transformation(BaseModel):
    """Base class for defining transformations applied to Iceberg table partitions."""

    source_field_path_escaped: str
    """Full path of the source column to which the partition transformation applies."""
    partition_field_escaped: str
    """The name of the partition field, i.e.: the name found in catalog.schema.table_name.data_files's
       partition.this_is_the_partition_field_name.
    """

    @abstractmethod
    def make_rewrite_data_files_partition_filter_stmt(self, partition_value: Any) -> str:  # noqa: ANN401
        """Abstract method to build a filter condition for the `rewrite_data_files` operation in SQL.

        This is used when rewriting Iceberg tables based on partition transformations.

        Args:
            partition_value (Any): The specific partition value being filtered for.

        Returns:
            str: SQL filter condition for the specified partition value.

        Examples:
            For a year partition filter we get a partition_value=55 and we return:
                `( ts >= date('2024-01-01') and ts < date('2024-01-01') + interval 1 year)`
            For a day partition filter we get a partition_value='2024-12-19' and we return:
                `( ts >= date('2024-12-19') and ts < date('2024-12-19') + interval 1 day)`
            For an hour partition filter we get a partition_value=450001 and we return:
                `( ts >= timestamp('2024-10-19 00:00:00') and ts < timestamp('2024-10-19 00:00:00') + interval 1 hour)`
        """

    def make_diagnosis_widening_expr_stmt(self, src_transformation: "Transformation") -> str:
        """Builds a SQL expression to handle widening partition scopes.

        A WideningRule specifies that some rows in one partition (e.g., a day partition)
        need to be rewritten into a wider partition (e.g., a year partition). This method
        constructs a SQL expression to transform the source partition value into the corresponding
        value of the destination partition.

        This is used when running diagnostic queries. It effectively integrates rows from
        a narrower partition into the diagnostics for a wider partition.

        Args:
            src_transformation (Transformation): The transformation being widened from
                                                  (e.g., HourTransformation, DayTransformation).

        Raises:
            NotImplementedError: If the subclass does not support widening from the given source transformation.

        Returns:
            str: SQL expression representing the widening transformation.

        Example:
            For widening from a day to a year:
            The source partition values hold dates, and the method must return a value
            representing the number of years since the epoch (1970-01-01).

            SQL example:
                `year({column_holding_date}) - year('1970-01-01')`
        """
        msg = f"Widening from {src_transformation.__class__.__name__} to {self.__class__.__name__} is not supported."
        raise NotImplementedError(msg)

    @classmethod
    def from_pyiceberg(  # noqa: PLR0911
        cls,
        source_field_path_escaped: str,
        partition_field: PartitionField,
        catalog: str,
    ) -> "Transformation":
        partition_field_escaped = escape_identifier(partition_field.name)
        if isinstance(partition_field.transform, HourTransform):
            return HourTransformation(
                source_field_path_escaped=source_field_path_escaped, partition_field_escaped=partition_field_escaped
            )
        if isinstance(partition_field.transform, DayTransform):
            return DayTransformation(
                source_field_path_escaped=source_field_path_escaped, partition_field_escaped=partition_field_escaped
            )
        if isinstance(partition_field.transform, MonthTransform):
            return MonthTransformation(
                source_field_path_escaped=source_field_path_escaped, partition_field_escaped=partition_field_escaped
            )
        if isinstance(partition_field.transform, YearTransform):
            return YearTransformation(
                source_field_path_escaped=source_field_path_escaped, partition_field_escaped=partition_field_escaped
            )
        if isinstance(partition_field.transform, IdentityTransform):
            return IdentityTransformation(
                source_field_path_escaped=source_field_path_escaped, partition_field_escaped=partition_field_escaped
            )
        if isinstance(partition_field.transform, TruncateTransform):
            return TruncateTransformation(
                source_field_path_escaped=source_field_path_escaped,
                partition_field_escaped=partition_field_escaped,
                width=partition_field.transform.width,
                catalog=catalog,
            )
        if isinstance(partition_field.transform, BucketTransform):
            return BucketTransformation(
                source_field_path_escaped=source_field_path_escaped,
                partition_field_escaped=partition_field_escaped,
                num_buckets=partition_field.transform.num_buckets,
                catalog=catalog,
            )
        if isinstance(partition_field.transform, (VoidTransform, UnknownTransform)):
            return NotPartitionedTransformation(source_field_path_escaped="", partition_field_escaped="")

        msg = f"Unsupported partition transform {type(partition_field.transform).__name__!s} for field '{partition_field.name}'"
        raise ValueError(msg)


class IdentityTransformation(Transformation):
    def make_rewrite_data_files_partition_filter_stmt(self, partition_value: Any) -> str:  # noqa: ANN401
        """Make an sql expression selecting the specific value provided.

        Example:
        >>> transform = IdentityTransformation(source_field_path_escaped="col", partition_field_escaped="col")
        >>> transform.make_rewrite_data_files_partition_filter_stmt("category1")
        "( col = 'category1' )"
        >>> transform = IdentityTransformation(source_field_path_escaped="col", partition_field_escaped="col")
        >>> transform.make_rewrite_data_files_partition_filter_stmt(123)
        "( col = 123 )"
        """
        if isinstance(partition_value, int | float):
            # Handle numeric types (no quotes needed)
            return f"( {self.source_field_path_escaped} = {partition_value} )"
        if isinstance(partition_value, str | datetime | date):
            # Handle string type (add quotes)
            return f"( {self.source_field_path_escaped} = '{partition_value}' )"
        # Raise an error for unsupported types
        msg = f"Unsupported partition value type: {type(partition_value)}"
        raise TypeError(msg)


class NotPartitionedTransformation(Transformation):
    def make_rewrite_data_files_partition_filter_stmt(self, partition_value: Any) -> str:  # noqa: ANN401, ARG002
        msg = "Should never be called."
        raise Exception(msg)


class YearTransformation(Transformation):
    def make_rewrite_data_files_partition_filter_stmt(self, partition_value: Any) -> str:  # noqa: ANN401
        """Make an sql expression selecting the specific value provided.

        Example:
        >>> transform = YearTransformation(source_field_path_escaped="ts", partition_field_escaped="ts_year")
        >>> transform.make_rewrite_data_files_partition_filter_stmt(57)
        "( ts >= date('2027-01-01') and ts < date('2027-01-01') + interval 1 year )"
        """
        if not isinstance(partition_value, int):
            # Raise an error for unsupported types
            msg = f"Unsupported partition value type: {type(partition_value)}"
            raise TypeError(msg)
        years_since_epoch = partition_value
        year = 1970 + years_since_epoch
        # Create the datetime object for January 1st of that year
        dt = datetime(year, 1, 1, tzinfo=timezone.utc)
        formatted_date = dt.strftime("%Y-%m-%d")
        return f"( {self.source_field_path_escaped} >= date('{formatted_date}') and {self.source_field_path_escaped} < date('{formatted_date}') + interval 1 year )"

    def make_diagnosis_widening_expr_stmt(self, src_transformation: Transformation) -> str:
        if not isinstance(src_transformation, HourTransformation | DayTransformation | MonthTransformation):
            msg = "Only month, day, hour are allowed when widening to a year."
            raise ValueError(msg)
        if isinstance(src_transformation, HourTransformation):
            hours_since_epoch = src_transformation.partition_field_escaped
            year_stmt = f"year(from_unixtime(partition.{hours_since_epoch} * 3600))"
        elif isinstance(src_transformation, DayTransformation):
            date = src_transformation.partition_field_escaped
            year_stmt = f"year(partition.{date})"
        else:  # Month
            months_since_epoch = src_transformation.partition_field_escaped
            year_stmt = f"(cast((partition.{months_since_epoch} / 12) as int) + 1970)"
        return f"({year_stmt} - year('1970-01-01'))"


class MonthTransformation(Transformation):
    def make_rewrite_data_files_partition_filter_stmt(self, partition_value: Any) -> str:  # noqa: ANN401
        """Make an sql expression selecting the specific value provided.

        Example:
        >>> transform = MonthTransformation(source_field_path_escaped="ts", partition_field_escaped="ts_month")
        >>> transform.make_rewrite_data_files_partition_filter_stmt(649)
        "( ts >= date('2024-02-01') and ts < date('2024-02-01') + interval 1 month )"
        """
        if not isinstance(partition_value, int):
            # Raise an error for unsupported types
            msg = f"Unsupported partition value type: {type(partition_value)}"
            raise TypeError(msg)
        months_since_epoch = partition_value
        year = 1970 + (months_since_epoch // 12)  # Get the year
        month = (months_since_epoch % 12) + 1  # Get the month (adding 1 because months are 1-indexed)
        # Create a datetime object (default day 1, time 00:00:00)
        dt = datetime(year, month, 1, tzinfo=timezone.utc)
        formatted_date = dt.strftime("%Y-%m-%d")
        return f"( {self.source_field_path_escaped} >= date('{formatted_date}') and {self.source_field_path_escaped} < date('{formatted_date}') + interval 1 month )"

    def make_diagnosis_widening_expr_stmt(self, src_transformation: Transformation) -> str:
        if not isinstance(src_transformation, HourTransformation | DayTransformation):
            msg = "Only day, hour are allowed when widening to a month."
            raise ValueError(msg)

        if isinstance(src_transformation, HourTransformation):
            hours_since_epoch = src_transformation.partition_field_escaped
            month_stmt = f"date_trunc('month', timestamp(from_unixtime(3600 * partition.{hours_since_epoch})))"
        else:
            date = src_transformation.partition_field_escaped
            month_stmt = f"date_trunc('month', partition.{date})"
        return f"floor(months_between({month_stmt}, date('1970-01-01')))"


class DayTransformation(Transformation):
    def make_rewrite_data_files_partition_filter_stmt(self, partition_value: Any) -> str:  # noqa: ANN401
        """Make an sql expression selecting the specific value provided.

        Example:
        >>> transform = DayTransformation(source_field_path_escaped="ts", partition_field_escaped="ts_day")
        >>> transform.make_rewrite_data_files_partition_filter_stmt(datetime(2025,1,28))
        "( ts >= date('2025-01-28 00:00:00') and ts < date('2025-01-28 00:00:00') + interval 1 day )"
        """
        if not isinstance(partition_value, datetime | date):
            # Raise an error for unsupported types
            msg = f"Unsupported partition value type: {type(partition_value)}"
            raise TypeError(msg)
        date_str = partition_value
        # The value is already a date time format, no conversion required.
        return f"( {self.source_field_path_escaped} >= date('{date_str}') and {self.source_field_path_escaped} < date('{date_str}') + interval 1 day )"

    def make_diagnosis_widening_expr_stmt(self, src_transformation: Transformation) -> str:
        if isinstance(src_transformation, HourTransformation):
            hours_since_epoch = src_transformation.partition_field_escaped
            return f"date(timestamp(from_unixtime(3600 * partition.{hours_since_epoch})))"
        msg = "Only hour is allowed when widening to a day."
        raise ValueError(msg)


class HourTransformation(Transformation):
    def make_rewrite_data_files_partition_filter_stmt(self, partition_value: Any) -> str:  # noqa: ANN401
        """Make an sql expression selecting the specific value provided.

        Example:
        >>> transform = HourTransformation(source_field_path_escaped="ts", partition_field_escaped="ts_hour")
        >>> transform.make_rewrite_data_files_partition_filter_stmt(466992)
        "( ts >= timestamp('2023-04-11 00:00:00') and ts < timestamp('2023-04-11 00:00:00') + interval 1 hour )"
        """
        if not isinstance(partition_value, int):
            # Raise an error for unsupported types
            msg = f"Unsupported partition value type: {type(partition_value)}"
            raise TypeError(msg)
        hours_since_epoch = partition_value
        # Convert hours since epoch to seconds and create a UTC datetime object
        timestamp = hours_since_epoch * 3600  # Convert hours into seconds
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)  # Convert to UTC datetime
        formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")
        return f"( {self.source_field_path_escaped} >= timestamp('{formatted_date}') and {self.source_field_path_escaped} < timestamp('{formatted_date}') + interval 1 hour )"


class TruncateTransformation(Transformation):
    catalog: str
    width: int

    # To filter rewrite_data_files we use Iceberg's truncate function on the column.
    def make_rewrite_data_files_partition_filter_stmt(self, partition_value: Any) -> str:  # noqa: ANN401
        """Make an sql expression selecting the specific value provided.

        Example:
        >>> transform = TruncateTransformation(source_field_path_escaped="col", partition_field_escaped="col_trunc", catalog="cat", width=4)
        >>> transform.make_rewrite_data_files_partition_filter_stmt(123)
        '( col_trunc.system.truncate(4, col) = 123 )'
        >>> transform.make_rewrite_data_files_partition_filter_stmt("abcd")
        "( col_trunc.system.truncate(4, col) = 'abcd' )"
        """
        if isinstance(partition_value, int | float):
            # Handle numeric types (no quotes needed)
            return f"( {self.catalog}.system.truncate({self.width}, {self.source_field_path_escaped}) = {partition_value} )"
        if isinstance(partition_value, str | datetime | date):
            # Handle string type (add quotes)
            return f"( {self.catalog}.system.truncate({self.width}, {self.source_field_path_escaped}) = '{partition_value}' )"
        # Raise an error for unsupported types
        msg = f"Unsupported partition value type: {type(partition_value)}"
        raise TypeError(msg)


class BucketTransformation(Transformation):
    catalog: str
    num_buckets: int

    # To filter rewrite_data_files we use Iceberg's bucket function on the column.
    def make_rewrite_data_files_partition_filter_stmt(self, partition_value: Any) -> str:  # noqa: ANN401
        """Make an sql expression selecting the specific value provided.

        Example:
        >>> transform = BucketTransformation(source_field_path_escaped="col", partition_field_escaped="col_bucket", catalog="cat", num_buckets=4)
        >>> transform.make_rewrite_data_files_partition_filter_stmt(123)
        '( cat.system.bucket(4, col) = 123 )'
        >>> transform.make_rewrite_data_files_partition_filter_stmt("abcd")
        "( cat.system.bucket(4, col) = 'abcd' )"
        """
        if isinstance(partition_value, int | float):
            # Handle numeric types (no quotes needed)
            return f"( {self.catalog}.system.bucket({self.num_buckets}, {self.source_field_path_escaped}) = {partition_value} )"
        if isinstance(partition_value, str | datetime | date):
            # Handle string type (add quotes)
            return f"( {self.catalog}.system.bucket({self.num_buckets}, {self.source_field_path_escaped}) = '{partition_value}' )"
        # Raise an error for unsupported types
        msg = f"Unsupported partition value type: {type(partition_value)}"
        raise TypeError(msg)
