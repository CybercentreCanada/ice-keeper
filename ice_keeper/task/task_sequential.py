import logging

from .task import SubTaskExecutor, Task, TaskResult

logger = logging.getLogger("ice-keeper")


class SequentialTask(Task):
    """A SequentialTask class that executes a series of tasks sequentially for the same table.

    This class manages a sequence of tasks that are performed consecutively on the same table.
    Each task in the sequence is executed one after the other in the current thread.
    """

    def __init__(self) -> None:
        """Initialize the SequentialTask.

        Attributes:
            sequence (list[Task]): A list to store the sequential tasks to be executed.
            full_name (str): The name of the table associated with all tasks in the sequence.
            current_step (int): The index of the task currently being executed in the sequence.
        """
        super().__init__()
        self.sequence: list[Task] = []
        self.full_name = ""
        self.current_step = 0

    def task_name(self) -> str:
        """Get the name of the SequentialTask.

        This corresponds to the name of the
        table (full_name) associated with all the tasks in the sequence.

        Returns:
            str: The name of the table.
        """
        return self.full_name

    def task_description(self) -> str:
        """Get a description of the SequentialTask.

        Returns:
            str: A description of the task, indicating the table's name and the
                 fact that it runs multiple commands.
        """
        return f"Multi commands for [{self.task_name()}]"

    def add_task(self, task: Task) -> None:
        """Add a task to the sequence for execution.

        A `SequentialTask` executes commands like "snapshot", "expire", and "optimize" in sequence,
        but only for the same table. This method ensures that all tasks in the sequence belong to
        the same `full_name` table. The name of the first task added to the sequence is used as
        the name of the entire sequence.

        Args:
            task (Task): The task to be added to the sequence.

        Raises:
            AssertionError: If the task being added does not belong to the same table as the first task.
        """
        if len(self.sequence) == 0:
            # Use the task name of the first task as the full_name for the sequence.
            self.full_name = task.task_name()
        else:
            # Ensure all tasks in the sequence refer to the same table.
            assert task.task_name() == self.full_name, (
                f"Task name mismatch: Expected '{self.full_name}', but got '{task.task_name()}'. "
                "All tasks in the sequence must belong to the same table."
            )
        self.sequence.append(task)

    def execute(self, sub_executor: SubTaskExecutor) -> TaskResult:
        """Execute the sequence of tasks sequentially.

        Each task in the sequence is executed one by one within the current thread.
        Execution is halted if a task encounters an exception, and no further tasks are run.

        Args:
            sub_executor (SubTaskExecutor): The context in which tasks are executed.

        Returns:
            TaskResult: The result of executing the sequence of tasks.
        """
        for task in self.sequence:
            # Use the TaskContext to submit and wait for task to complete. This call also takes care of journaling any results or issues
            sub_executor.execute_task(task)

        # Return an empty result. We do not journal a Sequence task.
        return TaskResult()
