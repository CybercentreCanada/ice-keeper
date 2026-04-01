import logging
import os
import re
import sys
import time
from datetime import datetime, timezone

import click
from pyspark.sql import Row, SparkSession

from ice_keeper import Action, Command, configure_logger
from ice_keeper.table.schedule_entry import MaintenanceScheduleRecord
from ice_keeper.task.action.optimization.optimization import OptimizationStrategy

from .config import ICEKEEPER_CONFIG, Config
from .pool import TaskExecutor
from .stm import STL, Scope
from .table import Journal, MaintenanceSchedule, PartitionHealth
from .task import (
    ActionTaskFactory,
    DiscoveryTask,
    Emailer,
    SequentialTask,
    Task,
    get_ordered_tasks_by_execution_time,
)

logger = logging.getLogger("ice-keeper")


def _get_catalog_schema_table_name(ctx: click.Context) -> tuple[str, str, str]:
    """Small helper to get catalog, schema and table_name from Click context."""
    return ctx.obj["catalog"], ctx.obj["schema"], ctx.obj["table_name"]


def create_spark_session(
    app_name: str,
    spark_master: str,
    driver_cores: int,
    driver_memory: str,
    num_machines: int,
    cpu_per_machine: int,
    executor_memory: str,
) -> None:
    # 1. Set the TZ environment variable to UTC
    os.environ["TZ"] = "UTC"

    # 2. Call tzset to apply the change (only works on Unix/Ubuntu)
    time.tzset()

    builder = SparkSession.builder.appName(app_name)
    if num_machines > 0:
        builder = (
            builder.config("spark.master", spark_master)
            # Make spark is running in UTC timezone.
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.executor.memory", executor_memory)
            .config("spark.executor.cores", str(cpu_per_machine))
            .config("spark.cores.max", str(cpu_per_machine * num_machines))
            .config("spark.dynamicAllocation.executorAllocationRatio", "0.25")
            .config("spark.dynamicAllocation.maxExecutors", str(num_machines))
            .config("spark.dynamicAllocation.minExecutors", "0")
            .config("spark.dynamicAllocation.executorIdleTimeout", "600s")
            .config("spark.sql.broadcastTimeout", "600")
            .config(
                "spark.memory.fraction", "0.80"
            )  # give more RAM to execution, default is 0.60 which reserves 40% of RAM to sparse records.
        )

    user_name = os.getenv("USER", default="unknown_user")
    spark = (
        builder.config("spark.driver.memory", driver_memory)
        .config("spark.driver.cores", str(driver_cores))
        # The configuration 'iceberg.hadoop.delete-file-parallelism' controls the number of delete threads
        # used by the HadoopFileIO class. The catalog creates an instance of the HadoopFileIO and will use this
        # configuration. However, if you change this configuration after the file IO is created, it will not
        # take effect—it will not modify the number of threads used in the HadoopFileIO instance's thread pool.
        # The max_concurrent_deletes is not used by the HadoopFileIO since it supports bulk deletes and thus
        # parameter will be ignored.
        #
        # To control the HadoopFileIO delete concurrency you have to set iceberg.hadoop.delete-file-parallelism
        # See in codebase https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/hadoop/HadoopFileIO.java#L51
        # We can confirm this option is taking affect my dumping the thread stack of the running Spark driver process
        # First find the PID of the Spark driver process.
        # Then use jstack to dump the threads: jstack -l -e <pid> > threads.txt
        # The threads.txt should contain 1024 thread dumps named "iceberg-hadoopfileio-delete-N"
        # Count the number of threads: cat threads.txt | grep iceberg-hadoopfileio-delete | wc -l
        #
        # Note in the logs you might see this warning
        # Warning: Ignoring non-Spark config property: iceberg.hadoop.delete-file-parallelism
        # Event though this waring is issued the configuration is still being used.
        # The 'iceberg.hadoop.delete-file-parallelism' configuration determines the number of delete threads
        # used by the HadoopFileIO class. The catalog creates an instance of HadoopFileIO and uses this setting.
        # However, changing this configuration after the FileIO instance is created will not take effect, as it
        # will not affect the number of threads in the HadoopFileIO instance's thread pool.
        #
        # Additionally, the 'max_concurrent_deletes' parameter is not used by HadoopFileIO because it supports
        # bulk deletes. Therefore, this parameter is ignored.
        #
        # To control the delete concurrency of HadoopFileIO, you must configure 'iceberg.hadoop.delete-file-parallelism'.
        # You can verify this by dumping the thread stack of the running Spark driver process:
        # 1. Find the PID of the Spark driver process.
        # 2. Use 'jstack' to dump the threads: jstack -l -e <pid> > threads.txt
        # 3. The threads.txt file should contain 1024 thread dumps named "iceberg-hadoopfileio-delete-N".
        # 4. Count the number of threads: cat threads.txt | grep iceberg-hadoopfileio-delete | wc -l
        #
        # Note: You might encounter the following warning in the logs: "Warning: Ignoring non-Spark config property:
        # iceberg.hadoop.delete-file-parallelism". Despite this warning, the configuration is still applied.
        .config("iceberg.hadoop.delete-file-parallelism", "1024")
        .config("spark.driver.maxResultSize", "16g")
        .config("spark.driver.extraJavaOptions", f"-Duser.name={user_name} -Dlog4j.configuration=file:./spark.log4j2.properties")
        .getOrCreate()
    )

    # Sanity checks for timezone UTC

    # Get system local timezone name and offset
    local_tz = datetime.now(timezone.utc).astimezone().tzinfo
    assert str(local_tz) == "UTC", f"Local timezone is not UTC, but {local_tz}"

    # This runs inside the Spark SQL engine
    row = spark.sql("SELECT current_timezone() as tz").take(1)[0]
    assert row.tz == "UTC", f"Local timezone is not UTC, but {row.tz}"

    # Store spark session on the main thread local.
    STL.set(spark, "main-thread")


def make_app_name(subcommand: str | None, scope: Scope) -> str:
    display_cmd = subcommand or ""

    app_name = f"ice-keeper: [{display_cmd}]"

    if scope.catalog:
        app_name += f" catalog: [{scope.catalog}]"

    if scope.schema:
        app_name += f" schema: [{scope.schema}]"

    if scope.table_name:
        app_name += f" table_name: [{scope.table_name}]"

    if scope.where:
        cleaned_where = re.sub(r"[^ ,a-zA-Z0-9=.\'\(\)]+", "_", scope.where)
        cleaned_where = scope.where.replace("\n", "").replace("    ", " ")
        app_name += f" where: [{cleaned_where}]"

    return app_name


@click.group()
@click.option("--spark_master", type=str, default="local", help="The spark master url (default: local).")
@click.option("--spark_executors", type=int, default=0, help="Number of Spark executors (default: 0).")
@click.option("--spark_executor_cores", type=int, default=5, help="Number of cores per Spark executor (default: 5).")
@click.option("--spark_executor_memory", type=str, default="5g", help="Memory per Spark executor (default: 5g).")
@click.option("--spark_driver_cores", type=int, default=4, help="Number of cores for the Spark driver (default: 4).")
@click.option("--spark_driver_memory", type=str, default="4g", help="Memory for the Spark driver (default: 4g).")
@click.option("--concurrency", type=int, default=1, help="Number of concurrent threads or processes to use (default: 1).")
@click.option("--catalog", type=str, required=False, help="Specify the catalog for the operation.")
@click.option("--schema", type=str, required=False, help="Specify the schema for the operation.")
@click.option("--table_name", type=str, required=False, help="Specify the table name for the operation.")
@click.option("--where", type=str, required=False, help="Specify a WHERE clause to filter rows for the operation.")
@click.option(
    "--config_file",
    type=click.Path(exists=True),
    envvar=ICEKEEPER_CONFIG,
    required=True,
    help="Path to the YAML config file. Also looks at ICEKEEPER_CONFIG env var.",
)
@click.pass_context
def cli(
    ctx: click.Context,
    spark_master: str,
    spark_executors: int,
    spark_executor_cores: int,
    spark_executor_memory: str,
    spark_driver_cores: int,
    spark_driver_memory: str,
    concurrency: int,
    catalog: str | None,
    schema: str | None,
    table_name: str | None,
    where: str | None,
    config_file: str,
) -> int:
    """Tool for Iceberg table maintenance tasks."""
    # If '--help' is invoked, short-circuit and avoid creating a useless spark session and journal thread.
    if "--help" in sys.argv:
        return 0

    Config.load_config(config_file)
    configure_logger()

    logger.info("Using %s", Config.instance().maintenance_schedule_table_name)
    logger.info("Using %s", Config.instance().partition_health_table_name)
    logger.info("Using %s", Config.instance().journal_table_name)

    ctx.ensure_object(dict)

    start = time.time()
    scope = Scope(catalog, schema, table_name, where)
    app_name = make_app_name(ctx.invoked_subcommand, scope)
    create_spark_session(
        app_name,
        spark_master,
        spark_driver_cores,
        spark_driver_memory,
        spark_executors,
        spark_executor_cores,
        spark_executor_memory,
    )

    journal = Journal()
    executor = TaskExecutor(journal, concurrency, max_subtask_workers=concurrency)
    executor.start()
    ctx.obj["executor"] = executor
    ctx.obj["app_name"] = app_name
    ctx.obj["start"] = start

    ctx.obj["catalog"] = catalog
    ctx.obj["schema"] = schema
    ctx.obj["table_name"] = table_name
    ctx.obj["where"] = where

    @ctx.call_on_close
    def stop_journal_and_spark() -> None:
        executor.shutdown()
        if STL.get():
            STL.get().stop()
        end = time.time()
        logger.info("||||||||  %s TOOK A TOTAL OF : %s SECONDS TO RUN. |||||||||||", app_name, end - start)

    return 0


@cli.command(
    short_help="Drop ice-keeper admin tables and recreate them (deletes history/state).",
)
@click.option(
    "--force",
    is_flag=True,
    help="Confirm resetting admin tables without prompting.",
)
def reset(force: bool) -> None:  # noqa: FBT001
    """Resets the admin tables."""
    if force or click.confirm(
        "This will DROP and RECREATE the MaintenanceSchedule table and permanently delete all "
        "configuration state. Are you sure you want to continue?",
    ):
        MaintenanceSchedule.reset()
        click.echo("MaintenanceSchedule table reset.")
    else:
        click.echo("Skipping MaintenanceSchedule table.")

    if force or click.confirm(
        "This will DROP and RECREATE the Journal table and permanently delete all history of executions. Are you sure you want to continue?",
    ):
        Journal.reset()
        click.echo("Journal table reset.")
    else:
        click.echo("Skipping Journal table.")

    if force or click.confirm(
        "This will DROP and RECREATE the PartitionHealth table and permanently delete all history/state. Are you sure you want to continue?",
    ):
        PartitionHealth.reset()
        click.echo("PartitionHealth table reset.")
    else:
        click.echo("Skipping PartitionHealth table.")


@cli.command(
    short_help="Diagnose table health by analyzing its partitions.",
)
@click.option("--full_name", required=True, help="Fully qualified name of table to diagnose.")
@click.option("--min_age_to_diagnose", default=None, help="Minimum snapshot age (in partition rank) to diagnose (default: 1).")
@click.option("--max_age_to_diagnose", default=None, help="Maximum snapshot age (in partition rank) to diagnose (default: 72).")
@click.option(
    "--min_partition_to_diagnose",
    default=None,
    help="Minimum partition offset to diagnose (e.g., '1d', '1M'). Overrides min_age_to_diagnose.",
)
@click.option(
    "--max_partition_to_diagnose",
    default=None,
    help="Maximum partition offset to diagnose (e.g., '7d', '3M'). Overrides max_age_to_diagnose.",
)
@click.option("--optimization_strategy", help="Optional optimization strategy to use during diagnosis.")
@click.option(
    "--target_file_size_bytes",
    type=int,
    default=None,
    help=(
        "Optional target data file size in bytes. If omitted or set to a value <= 0, "
        "ice-keeper automatically chooses an appropriate target size based on table "
        "characteristics."
    ),
)
@click.option(
    "--mode",
    type=click.Choice(["simulate", "dry_run"], case_sensitive=False),
    default="simulate",
    show_default=True,
    help=(
        "Execution mode. 'simulate' runs the optimizer logic and prints a detailed "
        "decision summary without applying any changes. 'dry_run' prints only a "
        "high-level estimate of potential changes, also without applying them."
    ),
)
def diagnose(
    full_name: str,
    min_age_to_diagnose: int,
    max_age_to_diagnose: int,
    min_partition_to_diagnose: str | None,
    max_partition_to_diagnose: str | None,
    mode: str,
    optimization_strategy: str | None,
    target_file_size_bytes: int | None,
) -> int:
    """Diagnose table health by analyzing its partitions.

    This command processes partitions of the table to identify opportunities
    for optimization. It uses a specified optimization strategy to calculate and display
    a detailed summary of the partition state before any intervention.
    """
    maintenance_schedule = MaintenanceSchedule(Scope())
    entry = maintenance_schedule.get_maintenance_entry(full_name)
    if entry:
        record = entry.record
        # Make a copy of the record before mutating
        record_copy = record.model_copy()
        if optimization_strategy:
            record_copy.optimization_strategy = optimization_strategy
        if target_file_size_bytes is not None:
            record_copy.target_file_size_bytes = target_file_size_bytes
        if min_partition_to_diagnose is not None or max_partition_to_diagnose is not None:
            # Partition-based range: set age fields to -1 so the partition path is used
            record_copy.min_age_to_optimize = -1
            record_copy.max_age_to_optimize = -1
            if min_partition_to_diagnose is not None:
                record_copy.min_partition_to_optimize = min_partition_to_diagnose
            if max_partition_to_diagnose is not None:
                record_copy.max_partition_to_optimize = max_partition_to_diagnose
        else:
            record_copy.min_age_to_optimize = min_age_to_diagnose
            record_copy.max_age_to_optimize = max_age_to_diagnose
        row = Row(**record_copy.model_dump(by_alias=True))
        entry = MaintenanceScheduleRecord.from_row(row).to_entry()
        strategy = OptimizationStrategy(entry)
        try:
            if mode == "dry_run":
                strategy.diagnose_partition_specs()
            else:
                strategy.estimate_optimization_results_partition_specs()
        except Exception as e:  # noqa: BLE001
            msg = f"An error occurred while diagnosing table '{full_name}': {e}"
            raise click.ClickException(msg)  # noqa: B904

    else:
        # Preserve expected CLI behavior: emit an error and non-zero exit
        # when the table is not present in the maintenance schedule.
        msg = f"Table '{full_name}' not found in maintenance schedule."
        raise click.ClickException(msg)
    return 0


def optimize_maintenance_schedule(executor: TaskExecutor) -> None:
    full_name = Config.instance().maintenance_schedule_table_name
    maintenance_schedule = MaintenanceSchedule(Scope(where=f" full_name = '{full_name}' "))
    tasks = ActionTaskFactory.make_tasks(Action.REWRITE_DATA_FILES, maintenance_schedule)
    executor.submit_tasks_and_wait(tasks)


def commands_to_actions(commands: tuple[Command]) -> set[Action]:
    return {Action.from_command(command) for command in commands}


def initialize_tasks(maintenance_schedule: MaintenanceSchedule) -> dict[str, SequentialTask]:
    # Create a dictionary with empty SequentialTask for each maintenance entry
    return {entry.full_name: SequentialTask() for entry in maintenance_schedule.entries()}


def make_tasks_for_actions(actions: set[Action], maintenance_schedule: MaintenanceSchedule) -> list[list[Task]]:
    return [ActionTaskFactory.make_tasks(action, maintenance_schedule) for action in actions]


def make_sequences(actions: set[Action], maintenance_schedule: MaintenanceSchedule) -> list[SequentialTask]:
    all_tasks = make_tasks_for_actions(actions, maintenance_schedule)
    sequential_tasks = initialize_tasks(maintenance_schedule)
    for child_tasks in all_tasks:
        for child_task in child_tasks:
            sequential_tasks[child_task.task_name()].add_task(child_task)
    return list(sequential_tasks.values())


@cli.command(short_help="Run multiple maintenance commands across multiple tables.")
@click.option(
    "--command",
    "commands",
    type=click.Choice([command.value for command in Command], case_sensitive=False),
    multiple=True,
    required=True,
    help="List of commands to run.  Pass the option multiple times, e.g. --command expire --command orphan",
)
@click.pass_context
def multi(ctx: click.Context, commands: tuple[str]) -> None:
    """Run multiple maintenance commands across multiple tables.

    Use this command to execute a combination of tasks like expiring snapshots,
    optimizing tables, rewriting manifests, or removing orphan files.

    This command supports running tasks across multiple tables concurrently.
    """
    catalog, schema, table_name = _get_catalog_schema_table_name(ctx)
    where = ctx.obj["where"]
    scope = Scope(catalog, schema, table_name, where)
    maintenance_schedule = MaintenanceSchedule(scope)
    action_set = {Action.from_command(command) for command in Command.from_strings(commands)}
    tasks: list[SequentialTask] = make_sequences(action_set, maintenance_schedule)

    executor = ctx.obj["executor"]
    rows = executor.get_journal().get_historical_total_execution_times(action_set, scope)
    ordered_tasks = get_ordered_tasks_by_execution_time(tasks, rows)

    executor.submit_tasks_and_wait(ordered_tasks)
    optimize_maintenance_schedule(executor)


@cli.command(short_help="View or update the maintenance schedule for Iceberg tables.")
@click.option("--set", "set_clause", type=str, required=False, help="Update the schedule by setting these column values.")
@click.pass_context
def schedule(ctx: click.Context, set_clause: str | None) -> None:
    """View or update the maintenance schedule for Iceberg tables.

    Use this command to display or modify the maintenance schedule of tables
    configured in the system. The `--set` option allows for updating specific
    column values in the schedule.
    """
    catalog, schema, table_name = _get_catalog_schema_table_name(ctx)
    where = ctx.obj["where"]

    scope = Scope(catalog, schema, table_name, where=where)
    maintenance_schedule = MaintenanceSchedule(scope)
    if set_clause:
        maintenance_schedule.update(scope, set_clause=set_clause)
    else:
        maintenance_schedule.show()


@cli.command(short_help="Discover and schedule maintenance tasks for tables in a catalog or schema.")
@click.pass_context
def discover(ctx: click.Context) -> None:
    """Discover and schedule maintenance tasks for tables in a catalog or schema.

    This command identifies Iceberg tables under a specified catalog or schema
    and ensures they are properly registered in the maintenance schedule.
    It can be used to onboard new tables for maintenance.
    """
    catalog, schema, _ = _get_catalog_schema_table_name(ctx)

    all_tasks = []
    catalogs = catalog.split(",")
    for raw_catalog in catalogs:
        catalog = raw_catalog.strip()
        scope = Scope(catalog, schema)
        maintenance = MaintenanceSchedule(scope)
        tasks = DiscoveryTask.make_tasks(maintenance, scope)
        all_tasks.extend(tasks)
    executor = ctx.obj["executor"]
    executor.submit_tasks_and_wait(all_tasks)
    optimize_maintenance_schedule(executor)


@cli.command(short_help="Show details of maintenance task executions.")
@click.pass_context
def journal(ctx: click.Context) -> None:
    """Show details of maintenance task executions.

    This command displays a log of executed maintenance tasks for Iceberg tables.
    It includes details like task success, failures, and runtime durations. Use
    filters to narrow down results by catalog, schema, table, or others.
    """
    catalog, schema, table_name = _get_catalog_schema_table_name(ctx)
    where = ctx.obj["where"]
    scope = Scope(catalog, schema, table_name, where=where)
    Journal().show(scope)


@cli.command(name=Command.OPTIMIZE.value, short_help="Optimize table sorting and file packing for efficient querying.")
@click.pass_context
def optimize(
    ctx: click.Context,
) -> None:
    """Optimize table sorting and file packing for efficient querying.

    This command performs optimization tasks on specified Iceberg tables. It
    ensures partitions and data layouts are in an ideal state for performance
    benefits during read or write operations.
    """
    catalog, schema, table_name = _get_catalog_schema_table_name(ctx)
    where = ctx.obj["where"]
    scope = Scope(catalog, schema, table_name, where=where)

    maintenance_schedule = MaintenanceSchedule(scope)
    action = Action.from_command(Command.OPTIMIZE)
    tasks = ActionTaskFactory.make_tasks(action, maintenance_schedule)

    executor = ctx.obj["executor"]
    rows = executor.get_journal().get_historical_total_execution_times({action}, scope)
    ordered_tasks = get_ordered_tasks_by_execution_time(tasks, rows)
    executor.submit_tasks_and_wait(ordered_tasks)
    optimize_maintenance_schedule(executor)


@cli.command(name=Command.EXPIRE.value, short_help="Clean up old snapshots from Iceberg tables.")
@click.pass_context
def expire(ctx: click.Context) -> None:
    """Clean up old snapshots from Iceberg tables.

    This command removes historical snapshots beyond a certain age, freeing up
    storage space while maintaining table consistency. Use this command to manage
    storage growth effectively.
    """
    catalog, schema, table_name = _get_catalog_schema_table_name(ctx)
    where = ctx.obj["where"]
    scope = Scope(catalog, schema, table_name, where=where)

    maintenance_schedule = MaintenanceSchedule(scope)
    action = Action.from_command(Command.EXPIRE)
    tasks = ActionTaskFactory.make_tasks(action, maintenance_schedule)

    executor = ctx.obj["executor"]
    rows = executor.get_journal().get_historical_total_execution_times({action}, scope)
    ordered_tasks = get_ordered_tasks_by_execution_time(tasks, rows)

    executor.submit_tasks_and_wait(ordered_tasks)
    optimize_maintenance_schedule(executor)


@cli.command(name=Command.REWRITE_MANIFESTS.value, short_help="Optimize metadata by rewriting manifest files.")
@click.pass_context
def rewrite_manifests(ctx: click.Context) -> None:
    """Optimize metadata by rewriting manifest files.

    This command rewrites manifest files in Iceberg tables for more efficient
    metadata management. It is useful when tables have accumulated a large
    number of small manifests.
    """
    catalog, schema, table_name = _get_catalog_schema_table_name(ctx)
    where = ctx.obj["where"]
    scope = Scope(catalog, schema, table_name, where=where)

    maintenance_schedule = MaintenanceSchedule(scope)
    action = Action.from_command(Command.REWRITE_MANIFESTS)
    tasks = ActionTaskFactory.make_tasks(action, maintenance_schedule)
    executor = ctx.obj["executor"]

    rows = executor.get_journal().get_historical_total_execution_times({action}, scope)
    ordered_tasks = get_ordered_tasks_by_execution_time(tasks, rows)

    executor.submit_tasks_and_wait(ordered_tasks)
    optimize_maintenance_schedule(executor)


@cli.command(name=Command.LIFECYCLE_DATA.value, short_help="Delete table data exceeding the retention period.")
@click.pass_context
def lifecycle(ctx: click.Context) -> None:
    """Lifecycle the data in tables.

    This command issues delete statements to remove data from tables
    that is older than the configured retention period, as defined
    by the `ice-keeper.lifecycle-max-days` property.
    """
    catalog, schema, table_name = _get_catalog_schema_table_name(ctx)
    where = ctx.obj["where"]
    scope = Scope(catalog, schema, table_name, where=where)

    maintenance = MaintenanceSchedule(scope)
    action = Action.from_command(Command.LIFECYCLE_DATA)
    tasks = ActionTaskFactory.make_tasks(action, maintenance)
    executor = ctx.obj["executor"]
    executor.submit_tasks_and_wait(tasks)
    optimize_maintenance_schedule(executor)


@cli.command(name=Command.ORPHAN.value, short_help="Remove orphaned files left behind in storage.")
@click.pass_context
def orphan(ctx: click.Context) -> None:
    """Remove orphaned files left behind in storage.

    This command scans Iceberg table storage to identify files that are no
    longer referenced in the metadata. These files are then deleted to free
    up storage space.
    """
    catalog, schema, table_name = _get_catalog_schema_table_name(ctx)
    where = ctx.obj["where"]
    scope = Scope(catalog, schema, table_name, where=where)

    maintenance_schedule = MaintenanceSchedule(scope)
    action = Action.from_command(Command.ORPHAN)
    tasks = ActionTaskFactory.make_tasks(action, maintenance_schedule)
    executor = ctx.obj["executor"]

    rows = executor.get_journal().get_historical_total_execution_times({action}, scope)
    ordered_tasks = get_ordered_tasks_by_execution_time(tasks, rows)

    executor.submit_tasks_and_wait(ordered_tasks)
    optimize_maintenance_schedule(executor)


@cli.command(name=Command.AUDIT_CONFIG.value, short_help="Look for typos in tblproperties.")
@click.pass_context
def audit_config(ctx: click.Context) -> None:
    """Look for typos in tblproperties."""
    catalog, schema, table_name = _get_catalog_schema_table_name(ctx)
    where = ctx.obj["where"]
    scope = Scope(catalog, schema, table_name, where=where)

    maintenance_schedule = MaintenanceSchedule(scope)
    action = Action.from_command(Command.AUDIT_CONFIG)
    tasks = ActionTaskFactory.make_tasks(action, maintenance_schedule)
    executor = ctx.obj["executor"]
    executor.submit_tasks_and_wait(tasks)


@cli.command(short_help="Notify users of tasks fail every day for the past number of days")
@click.option("--last_num_days", "last_num_days_arg", type=int, required=True, help="Number of days of consecutive failures")
@click.pass_context
def notify(ctx: click.Context, last_num_days_arg: int = 1) -> None:
    """Notify users of failed tasks in the last 24 hours.

    This command scans for maintenance tasks that failed within the past
    24 hours and sends email notifications to the relevant users or teams.
    Ensure proper email configuration is in place before using this command.
    """
    catalog, schema, table_name = _get_catalog_schema_table_name(ctx)

    maintenance = MaintenanceSchedule(Scope())
    emailer = Emailer(maintenance, should_send_emails=True)
    scope = Scope(catalog, schema, table_name)
    emailer.send_notifications(scope, last_num_days_arg)


def main() -> int:
    return cli()


if __name__ == "__main__":
    main()
