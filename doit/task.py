"""Tasks are the main abstractions managed by doit"""
import dataclasses
import datetime
from itertools import chain
from typing import List, Sequence

from .action import AbstractAction

from .artifact import ArtifactLabel
from .backend import Backend
from .reporter import ExecutionReporter, TaskEvent


class InconsistentBackend(Exception):
    pass


@dataclasses.dataclass
class Task:
    name: str
    action: AbstractAction
    implicit_dependencies: List[ArtifactLabel]
    implicit_targets: List[ArtifactLabel]
    implicit_task_dependencies: list  # task dependencies without artifacts
    always_execute: bool
    execute_ones: bool
    ignore: bool
    execution_reporter: ExecutionReporter

    def __repr__(self):
        return f"<Task: {self.name}>"

    def label(self):
        return self.name

    def dependencies(self) -> chain[ArtifactLabel]:
        return chain(self.implicit_dependencies, self.action.get_all_dependencies())

    def targets(self):
        return chain(self.implicit_targets, self.action.get_all_targets())

    def execute(self, backend: Backend):
        # 1. check if it is needed to execute
        # 2. execute

        if not self.need_execute(backend):
            return

        self.action.execute()
        self.update_fingerprints_in_backend(backend)

    def update_fingerprints_in_backend(self, backend: Backend):
        run_with = {
                       a.label(): a.fingerprint()
                       for a in self.dependencies()
                   } | {
                       other.name: backend.get_task_fingerprint(other.name)
                       for other in self.implicit_task_dependencies
                   }

        backend.set_task_run_with(self.name, run_with)

        try:
            prev_run = backend.get_task_fingerprint(self.name)
            ix = int(prev_run.split(" ")[0]) + 1
        except KeyError:
            ix = 0

        backend.set_task_fingerprint(self.name, f"{ix} @ {datetime.datetime.utcnow()}")

    def need_execute(self, backend: Backend):
        if self.ignore:
            self.execution_reporter.on_task_event(TaskEvent.SKIP, str(self), "it is ignored")
            return False

        if self.always_execute:
            self.execution_reporter.on_task_event(TaskEvent.EXECUTE, str(self), "it is always executed")
            return True

        if not list(self.dependencies()):
            # no dependencies => always execute
            self.execution_reporter.on_task_event(TaskEvent.EXECUTE, str(self), "it has no dependencies")
            return True

        for other in self.implicit_task_dependencies:  # type: Task
            try:
                task_fp = backend.get_task_fingerprint(other.name)
            except KeyError:
                raise InconsistentBackend(f"{other} fingerprint is missing from backend")

            try:
                task_rw_fp = backend.get_task_run_with(self.name, other.name)
            except KeyError:
                self.execution_reporter.on_task_event(TaskEvent.EXECUTE, str(self),
                                                      f"depends now on {other}")
                return True

            if task_fp != task_rw_fp:
                self.execution_reporter.on_task_event(TaskEvent.EXECUTE, str(self),
                                                      f"{other} has been updated")
                return True

        for dep in self.dependencies():
            try:
                if backend.get_task_run_with(self.name, dep.label()) != dep.fingerprint():
                    self.execution_reporter.on_task_event(TaskEvent.EXECUTE, str(self),
                                                          f"{dep} was updated")
                    return True
            except KeyError:
                self.execution_reporter.on_task_event(TaskEvent.EXECUTE, str(self),
                                                      f"{dep} fingerprint is missing from backend")
                return True

        for tar in self.targets():
            if not tar.exists():
                self.execution_reporter.on_task_event(TaskEvent.EXECUTE, str(self),
                                                      f"{tar} does not exist")
                return True

        self.execution_reporter.on_task_event(TaskEvent.SKIP, str(self),
                                              f"it is up-to-date")
        return False
