import logging
import threading
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from sqlparse import format as sql_format

logger = logging.getLogger("ice-keeper")


class STL:
    """Spark Thread Local is a wrapper class for thread local variable holding the current spark session."""

    # Create an instance of threading.local for managing thread-local data
    _thread_local_data = threading.local()

    @classmethod
    def get(cls) -> SparkSession:
        return cls._thread_local_data.spark

    @classmethod
    def set(cls, spark: SparkSession, thread_name: str) -> None:
        # Set spark session in a thread-local variable
        cls._thread_local_data.spark = spark
        cls._set_thread_name(thread_name)

    @classmethod
    def set_job_description(cls, title: str, details: str = "") -> None:
        cls._thread_local_data.spark.sparkContext.setJobDescription(title)
        cls._thread_local_data.spark.sparkContext.setLocalProperty("callSite.short", title)
        cls._thread_local_data.spark.sparkContext.setLocalProperty("callSite.long", details)

    @classmethod
    def sql(cls, sql: str, description: str = "") -> DataFrame:
        """Create dataframe using provided sql statement and optinal job description."""
        cls.set_job_description(description, sql)
        return cls._thread_local_data.spark.sql(sql)

    @classmethod
    def sql_and_log(cls, sql: str, description: str = "") -> DataFrame:
        """Create dataframe using provided sql statement and optinal job description."""
        if logger.level == logging.DEBUG:
            print_stm(sql)
        cls.set_job_description(description, sql)
        return cls._thread_local_data.spark.sql(sql)

    @classmethod
    def set_config(cls, key: str, value: str) -> None:
        """Set spark configuration."""
        desc = f"Setting {key}={value}"
        cls.set_job_description(desc)
        cls._thread_local_data.spark.conf.set(key, value)

    @classmethod
    def _set_thread_name(cls, name: str) -> None:
        # truncate to last 25 chars to keep log line prefix short
        threading.current_thread().name = name[-25:]


def print_stm(sql: str) -> None:
    pretty_sql = sql_format(sql, indent_after_first=True, reindent_aligned=True, keyword_case="lower", compact=False)
    sql = "\n" + pretty_sql
    logger.debug(sql)


@dataclass(frozen=True)
class Scope:
    catalog: None | str = None
    schema: None | str = None
    table_name: None | str = None
    where: None | str = None

    def make_scoping_stmt(self) -> str:
        filters = []
        if self.catalog:
            filters.append(f"(catalog like '{self.catalog}')")
        if self.schema:
            filters.append(f"(schema like '{self.schema}')")
        if self.table_name:
            filters.append(f"(table_name like '{self.table_name}')")
        if self.where:
            filters.append(f"({self.where})")
        if len(filters) > 0:
            return " and ".join(filters)
        return "(1=1)"
