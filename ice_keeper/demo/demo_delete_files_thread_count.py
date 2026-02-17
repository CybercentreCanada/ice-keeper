import logging
import time

import requests
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# The configuration 'iceberg.hadoop.delete-file-parallelism' controls the number of delete threads
# used by the HadoopFileIO class. The catalog creates an instance of the HadoopFileIO and will use this
# configuration. However, if you change this configuration after the file IO is created, it will not
# take effect—it will not modify the number of threads used in the HadoopFileIO instance's thread pool.
# This program illustrates the behavior: only the original configuration is utilized. Any subsequent
# changes made using spark.conf.set are ignored. In IceKeeper, this configuration must be set during
# the creation of the initial Spark session.

# Note in the logs you might see this warning
# Warning: Ignoring non-Spark config property: iceberg.hadoop.delete-file-parallelism
# Event though this waring is issued the configuration is still being used.

spark_driver_port = 4099

catalog_name = "local"


def create_spark_session() -> SparkSession:
    num_machines = 50
    cpu_per_machine = 5
    return (
        SparkSession.builder.appName("expire snapshots thread count test")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.cores", "8")
        .config("spark.executor.memory", "5g")
        .config("spark.executor.cores", str(cpu_per_machine))
        .config("spark.cores.max", str(cpu_per_machine * num_machines))
        .config("spark.ui.port", str(spark_driver_port))
        .config("iceberg.hadoop.delete-file-parallelism", "7")
        # The following configs have no effect
        # .config("spark.iceberg.hadoop.delete-file-parallelism", "1")
        # .config("spark.hadoop.iceberg.hadoop.delete-file-parallelism", "1")
        # .config("spark.sql.iceberg.hadoop.delete-file-parallelism", "11")
        .getOrCreate()
    )


def create_test_table(spark: SparkSession) -> None:
    spark.sql(f"drop table if exists {catalog_name}.jcc.table_lots_of_files")

    spark.sql(f"""
      create table {catalog_name}.jcc.table_lots_of_files (id int) using iceberg tblproperties (
        'write.target-file-size-bytes' = '1000'
      )
    """)

    step = 1
    num_spark_tasks = 50
    num_rows = 100000

    for _ in range(3):
        start = time.time()
        spark.sql(
            f"insert into {catalog_name}.jcc.table_lots_of_files select id from range(0, {num_rows}, {step}, {num_spark_tasks})"
        )
        end = time.time()
        msg = f"took: {end - start} to insert data"
        logger.info(msg)

        num_files = spark.table(f"{catalog_name}.jcc.table_lots_of_files.data_files").count()
        msg = f"num_files:{num_files}"
        logger.info(msg)


def delete_data(spark: SparkSession) -> None:
    spark.sql(f"delete from {catalog_name}.jcc.table_lots_of_files where 1=1")
    remaining_files = spark.table(f"{catalog_name}.jcc.table_lots_of_files.data_files").count()
    msg = f"remaining_files:{remaining_files}"
    logger.info(msg)


def expire_snapshots(spark: SparkSession) -> None:
    num_snapshots = spark.table(f"{catalog_name}.jcc.table_lots_of_files.snapshots").count()
    msg = f"num_snapshots:{num_snapshots}"
    logger.info(msg)

    # All these are ignored by this point.
    spark.conf.set("spark.iceberg.hadoop.delete-file-parallelism", "5")
    spark.sql("set `iceberg.hadoop.delete-file-parallelism`=2")
    spark.sparkContext._conf.set("spark.iceberg.hadoop.delete-file-parallelism", "9")  # noqa: SLF001

    # Expire snapshots keeping only last one (the delete operation)
    start = time.time()
    spark.sql(f"""
      call {catalog_name}.system.expire_snapshots (
      table=> 'jcc.table_lots_of_files',
      older_than => timestamp '2100-01-01',
      retain_last => 1
    )
    """).show()

    end = time.time()
    msg = f"took: {end - start} to expire snapshots (physically delete data files)"
    logger.info(msg)

    num_snapshots = spark.table(f"{catalog_name}.jcc.table_lots_of_files.snapshots").count()
    msg = f"num_snapshots:{num_snapshots}"
    logger.info(msg)


def count_num_delete_threads_created() -> None:
    # Step 1: Define Spark Driver Web UI URL
    thread_dump_url = f"http://localhost:{spark_driver_port}/executors/threadDump/?executorId=driver"

    # Step 2: Make an HTTP GET request to retrieve the thread dump
    response = requests.get(thread_dump_url, timeout=10)

    # Step 3: Check the response and display the thread information
    status_code_ok = 200
    if response.status_code == status_code_ok:
        logger.info("Thread Dump:")
        # logger.info(response.text)

        # Target thread name to search for
        thread_name = "iceberg-hadoopfileio-delete-"

        # Initialize the counter
        count = 0

        # Loop through each line in the response
        for line in response.text.splitlines():
            if thread_name in line:  # Check if the thread name exists in this line
                count += 1

        msg = f"There are {count} delete threads named '{thread_name}'-N in the thread dump."
        logger.info(msg)
    else:
        msg = f"Failed to fetch thread dump. Status Code: {response.status_code}"
        logger.info(msg)
        msg = f"Details: {response.content.decode()}"
        logger.info(msg)


if __name__ == "__main__":
    spark = create_spark_session()
    create_test_table(spark)
    delete_data(spark)
    expire_snapshots(spark)
    count_num_delete_threads_created()
    time.sleep(10)
    spark.stop()
