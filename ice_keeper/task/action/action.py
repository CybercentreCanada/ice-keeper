import logging
from abc import abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any

from typing_extensions import override

from ice_keeper import Action, ActionWarning, Status, dedent
from ice_keeper.stm import STL
from ice_keeper.table import JournalEntry, MaintenanceSchedule, MaintenanceScheduleEntry
from ice_keeper.task import Task, TaskResult
from ice_keeper.task.task import SubTaskExecutor

logger = logging.getLogger("ice-keeper")


class ActionStrategy:
    """Base class for defining strategies for specific Iceberg table actions.

    Subclasses must override the abstract methods to implement action-specific
    logic. This class provides common utilities to check for table modifications
    and handle maintenance properties.
    """

    def __init__(self, mnt_props: MaintenanceScheduleEntry) -> None:
        """Initialize the action strategy.

        Args:
            mnt_props (MaintenanceScheduleEntry): Configuration properties for the maintenance operation.
        """
        self._should_journal = True  # Flag to determine whether to log actions
        self.mnt_props = mnt_props

    def should_journal(self) -> bool:
        """Indicates whether the action should create a journal entry.

        Returns:
            bool: True if journaling is enabled, False otherwise.
        """
        return self._should_journal

    def disable_journaling(self) -> None:
        """Disables the creation of journal entries for this action."""
        self._should_journal = False

    def update_mnt_props(self, mnt_props: MaintenanceScheduleEntry) -> None:
        """Update the MaintenanceScheduleEntry configuration.

        The strategy retrieves and applies the most up-to-date configuration.

        Args:
            mnt_props (MaintenanceScheduleEntry): Updated maintenance properties to apply.
        """
        self.mnt_props = mnt_props

    @classmethod
    @abstractmethod
    def get_action(cls) -> Action:
        """Returns the specific action that the strategy represents.

        Must be implemented in derived classes.
        """

    @abstractmethod
    def task_description(self, full_name: str) -> str:
        """Returns a description for the task to be performed.

        Must be implemented in derived classes.

        Args:
            full_name (str): Fully qualified name of the table.

        Returns:
            str: A human-readable description of the task.
        """

    @abstractmethod
    def check_should_execute_action(self) -> bool:
        """Determine whether the specific action should be executed.

        Subclasses must implement this method to define their own
        preconditions for execution based on the table and its configuration.

        Raises:
            Exception: If the configuration is invalid or a required column is missing.
            ActionWarning: If there is a warning that does not prevent execution but needs attention.

        Returns:
            bool: True if the action should proceed, False otherwise.
        """

    @abstractmethod
    def prepare_statement_to_execute(self) -> str:
        """Prepare the SQL statement for the specific action.

        Subclasses must implement this method to generate the SQL query.

        Returns:
            str: The prepared SQL statement for execution.
        """
        return ""

    @abstractmethod
    def execute_statement(self, sub_executor: SubTaskExecutor, sql_stm: str) -> dict[str, Any]:
        """Run the given SQL statement and return execution results.

        Args:
            sub_executor (SubTaskExecutor): The execution context for the task.
            sql_stm (str): The SQL statement to execute.

        Returns:
            dict[str, Any]: A dictionary containing the execution results.
        """

    def _is_table_recently_modified(self) -> bool:
        """Check if the table was recently modified in the metadata log.

        Queries the metadata log for the table's latest modification timestamp
        and determines if it lies within a 5-day retention period.

        Returns:
            bool: True if the table was modified within the last 5 days, False otherwise.
        """
        full_name = self.mnt_props.full_name
        sql = f"""
            select
                max(timestamp) as latest_metadata_log
            from
                {full_name}.metadata_log_entries
            """
        row = STL.sql(sql, "Checking if table was recently modified").take(1)[0]
        latest_metadata_log = row.latest_metadata_log

        # Ensure the timestamp is timezone-aware
        if latest_metadata_log.tzinfo is None:
            latest_metadata_log = latest_metadata_log.replace(tzinfo=timezone.utc)

        metadata_log_age = datetime.now(tz=timezone.utc) - latest_metadata_log
        return metadata_log_age < timedelta(days=5)


class ActionTask(Task):
    """Encapsulates an action as a task for scheduled execution.

    An ActionTask coordinates the lifecycle of an action, including:
    - Updating maintenance properties
    - Logging a journal entry
    - Running the prepared SQL statement
    - Managing exception handling and journaling status
    """

    def __init__(
        self,
        strategy: ActionStrategy,
        mnt_props: MaintenanceScheduleEntry,
        maintenance_schedule: None | MaintenanceSchedule = None,
    ) -> None:
        """Initialize the task with its strategy and maintenance configuration.

        Args:
            strategy (ActionStrategy): Strategy instance for executing the action.
            mnt_props (MaintenanceScheduleEntry): Configuration associated with the table.
            maintenance_schedule (MaintenanceSchedule | None): Optional parent maintenance schedule, if applicable.
        """
        self.strategy = strategy
        self.mnt_props = mnt_props
        self.maintenance_schedule = maintenance_schedule

        # Base Task initialization
        super().__init__(mnt_props.full_name)

    @override
    def task_description(self) -> str:
        """Retrieve the task description from the strategy."""
        return self.strategy.task_description(self.mnt_props.full_name)

    def execute(self, sub_executor: SubTaskExecutor) -> TaskResult:
        """Execute the configured action strategy.

        Orchestrates the full lifecycle of an action, including:
        - Logging a journal entry.
        - Checking if the action should execute.
        - Preparing and running the SQL procedure.
        - Handling any exceptions and updating the journal.

        Args:
            sub_executor (SubTaskExecutor): The execution context for the task.

        Returns:
            TaskResult: The result of the execution, including any error details or journal entry.
        """
        journal_entry = JournalEntry.make_journal_entry(self.strategy.get_action(), self.mnt_props)
        journal_entry.start_time = datetime.now(timezone.utc)
        task_exception: None | Exception = None

        try:
            # Refresh maintenance properties if required
            if self.maintenance_schedule:
                self.mnt_props = self.maintenance_schedule.update_maintenance_schedule_entry(self.mnt_props)
            self.strategy.update_mnt_props(self.mnt_props)

            # Validate whether the action should execute
            if self.strategy.check_should_execute_action():
                # Prepare the SQL statement
                sql_stm = self.strategy.prepare_statement_to_execute()
                journal_entry.sql_stm = dedent(sql_stm).strip()

                # Execute the SQL statement and retrieve the results
                results = self.strategy.execute_statement(sub_executor, journal_entry.sql_stm)
                journal_entry.apply_dict(**results)
        except ActionWarning as w:
            journal_entry.set_status(Status.WARNING, str(w))
            task_exception = w
        except Exception as e:  # noqa: BLE001
            journal_entry.set_status(Status.FAILED, str(e))
            task_exception = e

        # Finalize the journal entry with execution time
        journal_entry.end_time = datetime.now(timezone.utc)
        execution_duration = journal_entry.end_time - journal_entry.start_time
        journal_entry.exec_time_seconds = execution_duration.total_seconds()
        logger.info(
            "Execution time: %s seconds to %s table %s",
            journal_entry.exec_time_seconds,
            self.strategy.get_action(),
            self.mnt_props.full_name,
        )

        # Save the journal entry to the database if journaling is enabled
        if self.strategy.should_journal():
            return TaskResult(journal_entry)
        # Return any exception to be logged.
        return TaskResult(exception=task_exception)
