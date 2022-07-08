from enum import Enum
from typing import Set


class TaskEvent(Enum):
    SKIP = 1
    IGNORE = 2
    EXECUTE = 3


class DagEvent(Enum):
    START = 1
    DONE = 1


class ExecutionReporter:
    def __init__(self):
        self._filter_out_task_events = set()

    # ------------------------------------------------------------------------------------------------------------------
    def filtered_out_events(self, task_events: Set[TaskEvent]):
        cp = self._copy()  # type: ExecutionReporter
        cp._filter_out_task_events = task_events

    def _copy(self):
        raise NotImplementedError()

    # ------------------------------------------------------------------------------------------------------------------
    def on_task_event(self, event: TaskEvent, task_name: str, reason: str):
        if event not in self._filter_out_task_events:
            self.task_event(event, task_name, reason)

    def task_event(self, event: TaskEvent, task_name: str, reason: str):
        raise NotImplementedError()

    # ------------------------------------------------------------------------------------------------------------------
    def on_dag_event(self, event: DagEvent, dag_name: str):
        self.dag_event(event, dag_name)

    def dag_event(self, event: DagEvent, dag_name: str):
        raise NotImplementedError()


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

    def task_event(self, event: TaskEvent, task_name: str, reason: str):
        self.logger.info(
            f"{task_name}: {event.name}: {reason}"
        )

    def dag_event(self, event: DagEvent, dag_name: str):
        self.logger.info(f"{dag_name}: {event.name}")

