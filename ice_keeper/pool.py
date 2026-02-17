import logging
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any, TypeVar

from .table.journal import Journal
from .task import SubTaskExecutor, Task, TaskResult

logger = logging.getLogger("ice-keeper")

T = TypeVar("T")  # Generic type variable for the return value of a function


class BoundedThreadPoolExecutor:
    """Thread pool with bounded queue that blocks when full.

    The semaphore limits the number of tasks that can be queued.
    It blocks a client trying to submit more tasks when the queue
    is full, ensuring that no single client can monopolize
    the thread pool. Once a task finishes, the semaphore is released,
    allowing other clients to submit their tasks in a fair, round-robin manner.
    """

    def __init__(self, max_workers: int, max_queue_size: int) -> None:
        # Define a thread pool with limited workers and a semaphore for queue size
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.semaphore = threading.Semaphore(max_queue_size)

    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        """Submit a task to the executor. Blocks if the queue is full.

        The semaphore ensures that no more than max_queue_size tasks can be in the queue at any time.
        The semaphore is released when a task completes (handled in the _wrapper method),
        which allows new tasks to be submitted. This enforces a bounded thread pool behavior.
        """
        self.semaphore.acquire()  # Wait if queue is full
        return self.executor.submit(self._wrapper, fn, *args, **kwargs)

    def _wrapper(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute function and release semaphore after completion."""
        try:
            return fn(*args, **kwargs)
        finally:
            self.semaphore.release()

    def shutdown(self, wait: bool = True) -> None:  # noqa: FBT001, FBT002
        """Shutdown the thread pool."""
        self.executor.shutdown(wait=wait)


class TaskExecutor(SubTaskExecutor):
    """Manages task execution with journaling and progress tracking."""

    def __init__(
        self,
        journal: Journal,
        max_workers: int = 4,
        batch_wait_time: float = 60.0,
        min_batch_size: int = 10,
        max_subtask_workers: int = 4,
    ) -> None:
        self.journal = journal  # journal for logging results
        self.batch_wait_time = batch_wait_time
        self.min_batch_size = min_batch_size
        self.max_subtask_workers = max_subtask_workers

        # Main executor for primary tasks
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        # Bounded executor for handling subtasks
        self.subtask_executor = BoundedThreadPoolExecutor(max_workers=max_subtask_workers, max_queue_size=max_subtask_workers)

        # Track task progress (top-level tasks only)
        self.lock = threading.Lock()
        self.submitted_count = 0
        self.completed_count = 0
        self.failed_count = 0

    def start(self) -> None:
        """Start journal writer."""
        self.journal.start()

    def shutdown(self, wait: bool = True) -> None:  # noqa: FBT001, FBT002
        """Shutdown thread pool and journal."""
        self.executor.shutdown(wait=wait)
        self.subtask_executor.shutdown(wait=wait)
        self.journal.flush().stop()

    def submit_subtasks_and_wait(self, sub_tasks: list[Task]) -> None:
        """Submit sub-tasks and wait for completion."""
        futures = [self._submit_subtask(task) for task in sub_tasks]
        wait(futures)  # Wait for all submitted tasks

    def _submit_subtask(self, task: Task) -> Future[None]:
        """Submit a sub-task to the subtask executor."""
        return self.subtask_executor.submit(self._execute_task, task, update_counters=False)

    def submit_tasks_and_wait(self, tasks: list[Task]) -> None:
        """Submit tasks and wait for all tasks to complete."""
        futures = []
        with self.lock:
            for task in tasks:
                self.submitted_count += 1
                future = self._submit_task(task)
                futures.append(future)

        wait(futures)  # Wait for all tasks to complete

        # Report progress after all tasks are processed
        with self.lock:
            self._report_progress()

    def _submit_task(self, task: Task) -> Future[None]:
        """Submit a single task to the main executor."""
        return self.executor.submit(self._execute_task, task, update_counters=True)

    def _report_progress(self) -> None:
        """Log task progress."""
        msg = f"Progress: {self.completed_count}/{self.submitted_count} completed, {self.failed_count} failed"
        logger.info(msg)

    def _execute_task(self, task: Task, update_counters: bool = True) -> None:  # noqa: FBT001, FBT002
        """Execute a task while managing journaling and progress tracking."""
        success = True
        try:
            # Execute the task
            result = task.execute(self)
            if result.exception:  # Handle execution errors
                msg = f"Error executing task {task.task_description}: {result.exception}"
                logger.error(msg)
                success = False
            elif result.journal_entry:  # Check if journal entry indicates success
                if not result.journal_entry.is_success():
                    success = False
            # Queue the result for journaling
            self.queue_result(result)
        except Exception as e:  # noqa: BLE001
            msg = f"Unexpected error executing task {task.task_description}: {e}"
            logger.error(msg)
            success = False
        if update_counters:
            with self.lock:  # Update task counters safely
                if success:
                    self.completed_count += 1
                else:
                    self.failed_count += 1
                self._report_progress()

    def queue_result(self, result: TaskResult) -> None:
        """Add task result to the journal."""
        if result.journal_entry:
            self.journal.write_journal_entry(result.journal_entry)

    def execute_task(self, task: Task) -> None:
        """Execute a single standalone task."""
        self._execute_task(task, update_counters=False)

    def get_journal(self) -> Journal:
        """Get the associated journal instance."""
        return self.journal
