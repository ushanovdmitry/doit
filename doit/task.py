"""Tasks are the main abstractions managed by doit"""
import dataclasses
from itertools import chain
from typing import List

from .action import AbstractAction

from .artifact import ArtifactLabel
from .backend import Backend


@dataclasses.dataclass
class Task:
    name: str
    action: AbstractAction
    implicit_dependencies: List[ArtifactLabel]
    implicit_targets: List[ArtifactLabel]
    always_execute: bool
    execute_ones: bool

    def __repr__(self):
        return f"<Task: {self.name}>"

    def dependencies(self):
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
        backend.set_task_run_with(
            self.name,
            {
                a.label(): a.fingerprint()
                for a in self.dependencies()
            }
        )

    def need_execute(self, backend: Backend):
        if self.always_execute:
            return True

        if not self.dependencies():
            # no dependencies => always execute
            return True

        for dep in self.dependencies():
            try:
                if dep.fingerprint() != backend.get_task_run_with(self.name, dep.label()):
                    return True
            except KeyError:
                return True

        for tar in self.targets():
            if not tar.exists():
                return True

        return False
