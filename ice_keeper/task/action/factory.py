import logging

from ice_keeper import Action
from ice_keeper.table import MaintenanceSchedule
from ice_keeper.task import SparkTask, Task

from .action import ActionStrategy, ActionTask

logger = logging.getLogger("ice-keeper")


class ActionTaskFactory:
    action_tasks: dict[Action, type[ActionStrategy]] = {}

    @classmethod
    def register(cls, action_task: type[ActionStrategy]) -> None:
        cls.action_tasks[action_task.get_action()] = action_task

    @classmethod
    def make_tasks(cls, action: Action, maintenance_schedule: MaintenanceSchedule) -> list[Task]:
        aclz: type[ActionStrategy] = cls.action_tasks[action]
        action_tasks = [
            ActionTask(aclz(mnt_props), mnt_props, maintenance_schedule) for mnt_props in maintenance_schedule.entries()
        ]
        return [SparkTask(action_task) for action_task in action_tasks]
