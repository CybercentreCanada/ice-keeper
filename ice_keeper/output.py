import logging
from collections.abc import Callable
from typing import Any

from pyspark.sql import DataFrame, Row
from rich.console import Console
from rich.table import Table
from rich.text import Text

logger = logging.getLogger("ice-keeper")


def print_df_to_console_vertical(df: DataFrame, n: int, truncate: int) -> None:
    """Print dataframe vertically.

    This function is used by the journal and maintenance schedule to show
    the user the activity or configuration of ice-keeper thus we don't have to
    log these outputs to a logger.
    """
    df.show(vertical=True, n=n, truncate=truncate)


def rows_log_info(rows: list[Row], title: str, style_function: Callable[[list[str], list[Any]], list[Any]] | None = None) -> None:
    table = rows_to_rich_table(rows, title, style_function)
    text = table_to_text(table)
    logger.info(text)


def rows_log_debug(
    rows: list[Row], title: str, style_function: Callable[[list[str], list[Any]], list[Any]] | None = None
) -> None:
    table = rows_to_rich_table(rows, title, style_function)
    text = table_to_text(table)
    logger.debug(text)


def table_to_text(table: Table) -> str:
    # Explicitly pass a width to the Console to simulate a wider console
    console = Console(width=800)  # Adjust width as appropriate
    with console.capture() as capture:
        console.print(table)
    return str(Text.from_ansi(capture.get()))


def rows_to_rich_table(
    rows: list[Row], title: str, style_function: Callable[[list[str], list[Any]], list[Any]] | None = None
) -> Table:
    """Convert a PySpark DataFrame to a Rich Table with optional row styling.

    Args:
        rows (Row): The PySpark Rows to transform into a Rich table.
        title (str): The title of the Rich Table.
        style_function (Callable[[list[str], list[Any]], list[Any]] | None):
            An optional function that takes a list of column names and
            a row (list of cell values) as input and returns a list of styled cells.

    Returns:
        Table: A Rich Table object with customized styling.
    """
    # Create a Rich Table with a title
    rc_table = Table(title=title)

    if len(rows) > 0:
        # Add columns to the table using the schema from the DataFrame
        column_names = rows[0].__fields__
        for column in column_names:
            rc_table.add_column(column)

        for row in rows:
            # If no style function is provided, simply convert all cells to strings
            styled_row = style_function(column_names, list(row)) if style_function else [str(cell) for cell in row]
            rc_table.add_row(*styled_row)

    return rc_table
