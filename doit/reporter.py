from enum import Enum
from typing import Set


class TaskEvent(Enum):
    SKIP = 1
    IGNORE = 2
    EXECUTE = 3


class DagEvent(Enum):
    START = 1
    DONE = 2


class ExecutionReporter:
    # ------------------------------------------------------------------------------------------------------------------
    def filter_events(self, keep_task_events=()):
        return FilteredExecutionReporter(
            self, keep_task_events=keep_task_events
        )

    # ------------------------------------------------------------------------------------------------------------------
    def task(self, event: TaskEvent, task_name: str, reason: str):
        raise NotImplementedError()

    # ------------------------------------------------------------------------------------------------------------------
    def dag(self, event: DagEvent, dag_name: str):
        raise NotImplementedError()


class FilteredExecutionReporter(ExecutionReporter):
    def __init__(self, rep: ExecutionReporter, keep_task_events):
        self.rep = rep
        self.keep_task_events = keep_task_events

    def task(self, event: TaskEvent, task_name: str, reason: str):
        if event in self.keep_task_events:
            self.rep.task(event, task_name, reason)

    def dag(self, event: DagEvent, dag_name: str):
        self.rep.dag(event, dag_name)


class LogExecutionReporter(ExecutionReporter):
    def __init__(self, logger=None):
        if logger is None:
            import loguru
            self.logger = loguru.logger
        else:
            self.logger = logger

        super(LogExecutionReporter, self).__init__()

    def _copy(self):
        res = LogExecutionReporter(self.logger)
        return res

    def task(self, event: TaskEvent, task_name: str, reason: str):
        self.logger.info(
            f"{event.name: >7}: {task_name}: {reason}"
        )

    def dag(self, event: DagEvent, dag_name: str):
        self.logger.info(f"{event.name: >5}: {dag_name}")

