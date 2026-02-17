import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence

from pyspark.sql import Row

from ice_keeper.stm import STL
from ice_keeper.table import JournalEntry

logger = logging.getLogger("ice-keeper")


class TaskResult:
    """Represents the result of a task execution.

    Attributes:
        exception (Exception | None): Exception that occurred during the task execution, if any.
        journal_entry (JournalEntry | None): Journal entry providing information about the task status.
    """

    exception: None | Exception = None
    journal_entry: None | JournalEntry = None

    def __init__(self, journal_entry: None | JournalEntry = None, exception: None | Exception = None) -> None:
        """Initialize a TaskResult object.

        Args:
            journal_entry (JournalEntry | None): Journal entry to store task details. Defaults to None.
            exception (Exception | None): Exception encountered during task execution. Defaults to None.
        """
        self.journal_entry = journal_entry
        if exception and self.journal_entry:
            assert not self.journal_entry.is_success(), "Exception should only occur if the task failed."
        self.exception = exception


class Task(ABC):
    """Abstract base class for tasks.

    A task represents a single unit of work that operates on a table.
    """

    def __init__(self, task_name: str = "") -> None:
        """Initialize a Task instance.

        Args:
            task_name (str): Name of the task, generally associated with the name of the table it works on.
                             Defaults to an empty string.
        """
        self._task_name = task_name

    @abstractmethod
    def execute(self, sub_executor: "SubTaskExecutor") -> TaskResult:
        """Execute the task in the given context.

        Args:
            sub_executor (SubTaskExecutor): Context containing necessary resources for sub-task execution.

        Returns:
            TaskResult: The result of the task execution.
        """

    def task_name(self) -> str:
        """Retrieve the name of the task.

        The name typically corresponds to the full name of the table on which the task operates.

        Returns:
            str: The name of the task.
        """
        return self._task_name

    @abstractmethod
    def task_description(self) -> str:
        """Retrieve a description of the task.

        This usually includes details about the task's operation, e.g., "Expiring snapshots of <table_name>."

        Returns:
            str: A description of the task.
        """


class SubTaskExecutor(ABC):
    """Interface used by TaskContext. A subset of the methods of TaskExecutor used by Task to submit sub-tasks."""

    @abstractmethod
    def execute_task(self, task: "Task") -> None:
        """Execute the given task.

        It will submit and wait for task to complete. Used by the SequentialTask.
        Executes on the current thread.

        Args:
            task (Task): The task to execute.
        """

    @abstractmethod
    def submit_subtasks_and_wait(self, sub_tasks: list[Task]) -> None:
        """Submit a list of subtasks for execution and wait for their completion.

        Used by the OptimizationTask to scheculd partition optimizations.

        Args:
            sub_tasks (list[Task]): List of tasks to be executed.
        """


class SparkTask(Task):
    """A wrapper for a task that ensures it executes in a new Spark session.

    This task creates a new Spark session in the current local thread
    for execution and cleans up after the task is completed.
    """

    def __init__(self, task: Task) -> None:
        """Initialize the SparkTask wrapper.

        Args:
            task (Task): The task to be executed within the new Spark session.
        """
        self.delegate = task
        self.parent_session = STL.get()

    def task_name(self) -> str:
        """Retrieve the name of the wrapped task."""
        return self.delegate.task_name()

    def task_description(self) -> str:
        """Retrieve the description of the wrapped task."""
        return self.delegate.task_description()

    def execute(self, sub_executor: SubTaskExecutor) -> TaskResult:
        """Execute the task, initializing a new Spark session in the current thread.

        This method checks whether the current Spark session is active, creates
        a new session if necessary, and assigns it to the thread-local Spark Thread Local (STL).

        During execution, the job group is set for the Spark session for better management.

        Args:
            sub_executor (SubTaskExecutor): The execution context for the task.

        Returns:
            TaskResult: The result of the task execution.

        Raises:
            Exception: If the Spark session is stopped before execution starts.
        """
        # Check if the parent Spark session is stopped
        if not self.parent_session.sparkContext._jsc:  # noqa: SLF001
            msg = "Spark session is stopped"
            raise Exception(msg)

        # Create a new session for the task and assign it to the thread-local context
        STL.set(self.parent_session.newSession(), self.task_name())
        STL.get().sparkContext.setJobGroup(self.task_description(), self.task_description())

        # Delegate the task execution to the wrapped task
        return self.delegate.execute(sub_executor)


def get_ordered_tasks_by_execution_time(tasks: Sequence[Task], rows: list[Row]) -> list[Task]:
    """Sort and return a list of tasks by their execution time in descending order.

    This function maps each task to its execution time derived from a list of
    `Row` objects containing `full_name` and `total_execution_time`, sorts tasks
    by execution time, and returns the ordered list. If a task's execution time
    is not found in the rows (e.g., if it's the first time the task is being
    executed), it is given a default execution time of 0.

    Args:
        tasks (Sequence[Task]): List of Task objects to be ordered.
        rows (list[Row]): List of Row objects containing `full_name` and
                          `total_execution_time` of previously executed tasks.

    Returns:
        list[Task]: Tasks sorted by their execution time in descending order.
    """
    # Create a mapping of full_name to the total execution time from the rows
    task_time_map = {row.full_name: row.total_execution_time for row in rows}

    # Map each task to its execution time from the map, defaulting to 0 if not found
    tasks_with_time = [(task_time_map.get(task.task_name(), 0), task) for task in tasks]

    # Sort tasks based on execution time in descending order
    tasks_with_time.sort(key=lambda x: x[0], reverse=True)

    # Extract and return the ordered list of tasks
    return [task for _, task in tasks_with_time]
