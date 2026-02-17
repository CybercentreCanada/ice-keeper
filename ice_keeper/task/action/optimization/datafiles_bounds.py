import logging
from abc import ABC, abstractmethod
from enum import Enum

from typing_extensions import override

from ice_keeper import escape_identifier
from ice_keeper.stm import STL
from ice_keeper.table import MaintenanceScheduleEntry

logger = logging.getLogger("ice-keeper")


class Bound(Enum):
    """Enumeration to represent the bounds of a column (lower or upper)."""

    lower = "lower"
    upper = "upper"


class Bounds(ABC):
    def __init__(self, mnt_props: MaintenanceScheduleEntry) -> None:
        self.mnt_props = mnt_props

    @abstractmethod
    def make_corr_threshold_expr_stmt(self) -> str:
        """Abstract method to generate a correlation threshold expression."""

    def make_lower_bounds_expr_stmt(self) -> str:
        """Constructs and returns the SQL expression for the lower bounds."""
        return self._make_bounds_expr_stmt(Bound.lower)

    def make_upper_bounds_expr_stmt(self) -> str:
        """Constructs and returns the SQL expression for the upper bounds."""
        return self._make_bounds_expr_stmt(Bound.upper)

    @abstractmethod
    def _make_bounds_expr_stmt(self, bound: Bound) -> str:
        """Abstract method to compute the SQL expression for either lower or upper bounds.

        Args:
            bound (Bound): Indicates whether to compute lower or upper bounds.

        Returns:
            str: The SQL expression for the specified bounds.
        """

    def _get_metric_column_type(self, column_name: str) -> str:
        """Retrieves the data type of the specified column's lower bound.

        Args:
            column_name (str): The name of the column.

        Returns:
            str: The data type of the column's lower bound.
        """
        schema = STL.sql(f"select * from {self.mnt_props.full_name}.files", "Get metric column type").schema
        return schema["readable_metrics"].dataType[column_name].dataType["lower_bound"].dataType.simpleString()  # type: ignore[index]

    def _make_column_expr(self, column_name: str, bound: Bound) -> str:
        """Generates the SQL expression to reference the column's bound.

        Args:
            column_name (str): Name of the column.
            bound (Bound): Specifies whether the lower or upper bound is required.

        Returns:
            str: The SQL expression for referencing the column's bound.
        """
        return f" readable_metrics.{escape_identifier(column_name)}.{bound.value}_bound "


class BoundsBinpack(Bounds):
    @override
    def make_corr_threshold_expr_stmt(self) -> str:
        """Returns a fixed correlation threshold expression for binpack optimization.

        Binpack strategy does not rely on calculating correlation of bounds, return 1.
        """
        return "1.00"

    @override
    def _make_bounds_expr_stmt(self, bound: Bound) -> str:
        """Stub for bounds expression with a static value for binpack optimization.

        Binpack strategy does not rely on calculating correlation of bounds.
        Return bounds of zero for upper and lower bounds.
        """
        return "0"


class BoundsSort(Bounds):
    @override
    def make_corr_threshold_expr_stmt(self) -> str:
        """Returns a fixed correlation threshold expression for sort optimization."""
        return "0.97"

    @override
    def _make_bounds_expr_stmt(self, bound: Bound) -> str:
        """Constructs the bounds expression using the first sorted column.

        Args:
            bound (Bound): Specifies whether the lower or upper bound is required.

        Returns:
            str: The SQL expression for sorted column bounds.
        """
        spec = self.mnt_props.optimization_spec
        # For normal sorting: use the first sorted column as it suffices to evaluate the correlation.
        sorted_column_name = spec.sorted_column_names[0]
        return self._make_column_expr(sorted_column_name, bound)


class BoundsZorderSort(Bounds):
    @override
    def make_corr_threshold_expr_stmt(self) -> str:
        """Computes a correlation threshold for z-order sorting.

        The correlation factor depends on
        the sorting strategy (e.g., z-order vs single column sorting). For z-order sorting, the
        min/max values of each contributing column are not directly influenced by the sorting
        applied to their interleaved result, leading to a lower correlation factor. The method uses
        a predefined step pattern curve derived from analyzed data to approximate the correlation factor.

        Returns:
            str: The SQL expression representing the correlation threshold.
        """
        return """
            case
                when n_files between 0 and 20 then 0.37857142857142856
                when n_files between 20 and 40 then 0.6812252964426878
                when n_files between 40 and 60 then 0.7815196998123827
                when n_files between 60 and 80 then 0.8363264499089385
                when n_files between 80 and 100 then 0.8572433753597433
                when n_files between 100 and 120 then 0.8789533750633772
                when n_files between 120 and 140 then 0.8863692819647544
                when n_files between 140 and 160 then 0.8909153528241235
                else 0.8909153528241235
            end
            """

    @override
    def _make_bounds_expr_stmt(self, bound: Bound) -> str:
        """Constructs the bounds expression using z-order sorting strategy.

        Args:
            bound (Bound): Specifies whether the lower or upper bound is required.

        Returns:
            str: The SQL expression for z-order sorted column bounds.
        """
        spec = self.mnt_props.optimization_spec
        sorted_column_names_and_type = []
        for column_name in spec.sorted_column_names:
            column_type = self._get_metric_column_type(column_name)
            sorted_column_names_and_type.append((column_name, column_type))
        return self._make_zorder_column_expr(sorted_column_names_and_type, bound)

    def _make_zorder_column_expr(self, column_names_and_type: list[tuple[str, str]], bound: Bound) -> str:
        """Constructs a custom z-order SQL expression for the provided columns and bounds.

        Args:
            column_names_and_type (list): A list of tuples, each containing a column name and its data type.
            bound (Bound): Specifies whether the lower or upper bound is required.

        Returns:
            str: The z-order SQL expression.
        """
        column_expressions = []
        for column_name, column_type in column_names_and_type:
            col_expr = self._make_column_expr(column_name, bound)
            if column_type == "string":
                # Convert the first 16 bytes of string columns to a numeric representation.
                # This is achieved using a combination of hex and conv methods in Spark SQL.
                # The Iceberg z-order function would be better, but it's currently not exposed
                # and can only be used within Iceberg's Java library.
                column_expressions.append(
                    f"""(
                            shiftleft(
                            bigint(
                                conv(
                                    hex(left({col_expr}, 8)),
                                    16,
                                    10)
                                ),
                                32)
                            |
                            bigint(
                                conv(
                                    hex(substring({col_expr}, 9, 8)),
                                    16,
                                    10
                                )
                            )
                        )
                    """
                )
            else:
                column_expressions.append(col_expr)
        # Apply custom z-order User Defined Function (UDF).
        return f" zorder2Tuple( {', '.join(column_expressions)} ) "
