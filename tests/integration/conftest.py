import os
import re
import shutil
import time
from collections.abc import Generator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.io import load_file_io
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table
from pyiceberg.typedef import Identifier
from pyspark.sql import SparkSession

from ice_keeper.catalog import CATALOGS
from ice_keeper.pool import TaskExecutor
from ice_keeper.stm import STL
from ice_keeper.table import Journal, MaintenanceSchedule, PartitionHealth


class PyIcebergHadoopCatalog(NoopCatalog):
    """Basic Hadoop Catalog to test with."""

    def __init__(self, name: str, warehouse_dir: str, **properties: str) -> None:
        super().__init__(name, **properties)
        self.warehouse_dir = warehouse_dir
        self.version_pattern = re.compile(r"v(\d+)\.metadata.json")

    def load_table(self, identifier: str | Identifier) -> Table:
        namespace_tuple = Catalog.namespace_from(identifier)
        namespace = Catalog.namespace_to_string(namespace_tuple)
        table_name = Catalog.table_name_from(identifier)
        # Convert the file URL to a local path (remove 'file:///' prefix)
        local_path = f"{self.warehouse_dir}/{namespace}/{table_name}/metadata/".replace("file://", "")
        files = Path(local_path).glob("v*.metadata.json")
        # Use a regular expression to extract the numeric part of the filename

        # Find the file with the largest version number
        metadata_location = ""
        max_version = -1
        for f in files:
            # print(f"processing {file}")
            match = self.version_pattern.search(Path(f).name)
            if match:
                version = int(match.group(1))  # Extract the numerical version
                if version > max_version:
                    max_version = version
                    metadata_location = str(f)
        io = load_file_io(properties=self.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)
        return Table(
            identifier=(*Catalog.identifier_to_tuple(namespace), table_name),
            metadata=metadata,
            metadata_location=metadata_location,
            io=self._load_file_io(metadata.properties, metadata_location),
            catalog=self,
        )


@pytest.fixture(name="executor", autouse=True)
def fixture_for_every_test_function(reset_config: Any) -> Generator[TaskExecutor, None, None]:  # noqa: ANN401, ARG001
    # For every test we reset everything
    print("Setup: Before test")  # Runs before the test
    warehouse_dir = STL.get().conf.get("spark.sql.catalog.local.warehouse")
    assert warehouse_dir
    warehouse_dir = warehouse_dir.replace("file://", "")
    try:  # noqa: SIM105
        shutil.rmtree(warehouse_dir)
    except FileNotFoundError:
        pass
    MaintenanceSchedule.reset()
    PartitionHealth.reset()
    Journal.reset()
    journal = Journal(maxsize=1000, flush_wait_seconds=1, poll_interval_seconds=1)
    executor = TaskExecutor(journal, 1, 1, 1, 1)
    executor.start()
    yield executor
    print("Teardown: After test")
    executor.shutdown()


@pytest.fixture(scope="session", autouse=True)
def spark_fixture(tmp_path_factory: pytest.TempPathFactory) -> None:
    # 1. Set the TZ environment variable to UTC
    os.environ["TZ"] = "UTC"

    # 2. Call tzset to apply the change (only works on Unix/Ubuntu)
    time.tzset()

    warehouse_dir = f"file://{tmp_path_factory.mktemp('iceberg-')}"  # Creates a session-scoped temporary directory
    CATALOGS["local"] = PyIcebergHadoopCatalog("local", warehouse_dir)

    spark = (
        SparkSession.builder.appName("integration tests")
        # Make spark is running in UTC timezone.
        .config("spark.sql.session.timeZone", "UTC")
        # Use a local catalog to speed up integration testing.
        .config("spark.driver.extraClassPath", "./jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_dir)
        .config("spark.sql.catalog.local.cache-enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.cores", "1")
        .getOrCreate()
    )

    STL.set(spark, "main-thread")

    # Sanity checks for timezone UTC

    # Get system local timezone name and offset
    local_tz = datetime.now(timezone.utc).astimezone().tzinfo
    print(f"Local Timezone: {local_tz}")
    assert str(local_tz) == "UTC", f"Local timezone is not UTC, but {local_tz}"

    # This runs inside the Spark SQL engine
    row = STL.sql("SELECT current_timezone() as tz").take(1)[0]
    assert row.tz == "UTC", f"Local timezone is not UTC, but {row.tz}"
