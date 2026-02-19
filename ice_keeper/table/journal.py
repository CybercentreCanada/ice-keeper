import logging
import queue
import threading

from pyspark.sql import DataFrame, Row, SparkSession

from ice_keeper import Action
from ice_keeper.config import Config, TemplateName
from ice_keeper.output import print_df_to_console_vertical
from ice_keeper.stm import STL, Scope

from .journal_entry import JournalEntry

logger = logging.getLogger("ice-keeper")


class Journal:
    """Handles writing and managing journal entries for tracking Iceberg table operations.

    The Journal class handles the creation, queuing, and writing of journal entries
    to an Iceberg-backed journal table. It uses a separate consumer thread to batch and
    asynchronously write log data to the table. This ensures that the journal does
    not block the main execution pipeline.

    Usage:
      - Call `journal.start()` to start the consumer thread.
      - Use `journal.write_journal_entry(journal_entry)` to enqueue entries for writing.
      - After operations, use `journal.flush()` to ensure all data has been written,
        and call `journal.stop()` to terminate the consumer thread.
      - It's recommended to call `flush()` and `stop()` in a `finally` block to avoid leaving threads active.
    """

    def __init__(self, maxsize: int = 10000, flush_wait_seconds: int = 60, poll_interval_seconds: int = 5) -> None:
        """Initialize the Journal with parameters for queue size, flush wait, and poll intervals.

        Args:
            maxsize (int): Maximum size of the event queue.
            flush_wait_seconds (int): Time to wait for batching entries before writing to the table.
            poll_interval_seconds (int): Polling interval to periodically check the queue and thread state.
        """
        self.event_queue: queue.Queue[JournalEntry] = queue.Queue(maxsize=maxsize)
        self.asked_to_stop = threading.Event()  # Event to signal consumer thread to stop
        self.total_queued = 0  # Number of entries queued
        self.total_written = 0  # Number of entries successfully written
        self.total_failed_write = 0  # Number of failed writes
        self.flush_wait_seconds = flush_wait_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.flush_timeout = 300

    def flush(self) -> "Journal":
        """Flushes all queued journal entries to the database, waiting until all are written.

        Returns:
            Journal: The current instance for chaining.
        """
        if self.asked_to_stop.is_set():
            return self
        assert self.consumer_thread.is_alive(), (
            "Journal thread should be running; make sure to call journal.start() before this method."
        )

        # Wait for the writer to flush the queue
        total_wait_time_seconds = 0
        while (self.total_written + self.total_failed_write) < self.total_queued and total_wait_time_seconds < self.flush_timeout:
            if self.asked_to_stop.wait(timeout=self.poll_interval_seconds):
                break
            total_wait_time_seconds += self.poll_interval_seconds
            logger.debug(
                "flush -> waited: %s seconds queued: %s written: %s failed: %s for journal writer",
                total_wait_time_seconds,
                self.total_queued,
                self.total_written,
                self.total_failed_write,
            )
        flush_state = "okay"
        if (self.total_written + self.total_failed_write) < self.total_queued:
            flush_state = "incomplete"
        logger.debug(
            "Flush %s -> waited: %s seconds queued: %s written: %s failed: %s for journal writer",
            flush_state,
            total_wait_time_seconds,
            self.total_queued,
            self.total_written,
            self.total_failed_write,
        )
        return self

    def write_journal_entry(self, journal_entry: JournalEntry) -> "Journal":
        """Adds a journal entry to the queue for processing.

        Args:
            journal_entry (JournalEntry): The journal entry to queue.

        Returns:
            Journal: The current instance for chaining.
        """
        self.event_queue.put(journal_entry, block=True)
        self.total_queued += 1  # Increment the counter for the total queued entries
        return self

    def stop(self) -> "Journal":
        """Stops the journal by signaling the consumer thread to terminate.

        Safely waits for the consumer thread to finish.

        Returns:
            Journal: The current instance for chaining.
        """
        self.asked_to_stop.set()  # Signal the thread to stop
        self.consumer_thread.join()  # Wait for the thread to terminate
        return self

    def start(self) -> "Journal":
        """Starts the consumer thread.

        This sets up a new Spark session and begins processing the event queue for writing logs.

        Returns:
            Journal: The current instance for chaining.
        """
        current_spark_session = STL.get()  # Get the current user's session
        self.consumer_thread = threading.Thread(target=self._consumer, args=(current_spark_session,))
        self.consumer_thread.start()
        return self

    def _drain_queue(self) -> list[JournalEntry]:
        """Drains all entries from the queue.

        Returns:
            list[JournalEntry]: A list of journal entries from the queue.
        """
        entries = []
        try:
            while True:
                journal_entry = self.event_queue.get(block=True, timeout=1)
                entries.append(journal_entry)
        except queue.Empty:
            pass
        return entries

    def _wait_between_saving(self) -> None:
        """Waits and logs the journal state, allowing queue entries to accumulate."""
        slept = 0
        while slept < self.flush_wait_seconds and not self.asked_to_stop.wait(timeout=self.poll_interval_seconds):
            slept += self.poll_interval_seconds
        logger.debug(
            "Journal consumer thread ===> queued: %s written: %s failed: %s. Sleeping for %s seconds",
            self.total_queued,
            self.total_written,
            self.total_failed_write,
            self.flush_wait_seconds,
        )

    def _store_entries(self, entries: list[JournalEntry]) -> None:
        """Stores journal entries into the Iceberg table.

        Args:
            entries (list[JournalEntry]): The list of journal entries to write.
        """
        try:
            rows = [entry.to_row() for entry in entries]
            journal_df = STL.get().createDataFrame(rows, JournalEntry.get_ddl())
            journal_df.writeTo(Config.instance().journal_table_name).append()
            self.total_written += len(entries)  # Update the written counter after success
        except Exception:
            self.total_failed_write += len(entries)  # Log failures
            logger.exception("An error occurred while writing to the journal table.")

    def _consumer(self, spark: SparkSession) -> None:
        """Consumes journal entries from the queue and writes them in batches.

        Args:
            spark (SparkSession): A Spark session used for writing to the Iceberg table.
        """
        STL.set(spark.newSession(), "journal-writer-thread")  # Assign a new session for the consumer thread
        STL.get().sparkContext.setJobGroup("Journal Writer", "Saving journal entries")

        while not self.asked_to_stop.is_set():
            entries = self._drain_queue()
            if len(entries) > 0:
                self._store_entries(entries)
                self._wait_between_saving()

    @classmethod
    def reset(cls) -> None:
        """Resets the journal table, dropping it if it exists, and recreating it."""
        full_name = Config.instance().journal_table_name
        STL.sql_and_log(f"drop table if exists {full_name}", "Dropping the journal table")
        ddl = JournalEntry.get_ddl()
        admin_table_notification_email = Config.instance().admin_table_notification_email
        # Ensures the base ends with a slash so the name is appended correctly
        table_location = Config.instance().get_table_location(full_name)
        template = Config.instance().load_template(TemplateName.JOURNAL)
        sql = template.render(
            full_name=full_name,
            ddl=ddl,
            notification_email=admin_table_notification_email,
            location=table_location,
        )
        STL.sql_and_log(sql)

    def read(self, scope: Scope) -> DataFrame:
        """Reads journal entries from the table and returns them as a DataFrame."""
        sql = self.read_sql(scope)
        return STL.sql(sql, "Reading Journal")

    def read_journal_entries(self, scope: Scope) -> list[JournalEntry]:
        """Reads journal entries from the table and returns them as a DataFrame."""
        sql = self.read_sql(scope)
        rows = STL.sql(sql, "Reading Journal").collect()
        return [JournalEntry.from_row(row) for row in rows]

    def read_sql(self, scope: Scope) -> str:
        """Builds the SQL query for reading journal entries with filters."""
        sql_filter = scope.make_scoping_stmt()
        return f"""
            select
                *
            from
                {Config.instance().journal_table_name}
            where
                {sql_filter}
        """

    def show(self, scope: Scope) -> None:
        """Displays the journal entries in a vertical format for better readability."""
        drop_df = self.read(scope).drop("catalog").drop("schema").drop("table_name")
        print_df_to_console_vertical(drop_df, n=10000, truncate=150)

    def get_historical_total_execution_times(
        self,
        actions: set[Action],
        scope: Scope,
        num_days: int = 14,
    ) -> list[Row]:
        """Retrieves historical total execution times for specified actions over a given period."""
        actions_quoted = [f"'{action.value}'" for action in actions]
        all_actions_sql = "(" + ",".join(actions_quoted) + ")"
        sql = f"""
            select
                sum(exec_time_seconds) as total_execution_time,
                full_name
            from
                ({self.read_sql(scope)})
            where
                start_time >= now() - interval '{num_days}' day
                and action in {all_actions_sql}
            group by
                full_name
            """
        return STL.sql_and_log(sql, "Get historical journal entries by total execution time.").collect()
