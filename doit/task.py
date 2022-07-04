"""Tasks are the main abstractions managed by doit"""
import dataclasses
from pathlib import Path
from typing import List

from . import AbstractDependency, AbstractTarget
from .action import AbstractAction, AbstractGraphNode, CanRepresentGraphNode


@dataclasses.dataclass
class TaskGraphNode(AbstractGraphNode):
    name: str


@dataclasses.dataclass
class Task(CanRepresentGraphNode):
    name: str
    action: AbstractAction
    implicit_dependencies: List[AbstractDependency]
    implicit_targets: List[AbstractTarget]
    always_execute: bool
    continue_on_failure: bool
    execute_ones: bool

    def __repr__(self):
        return f"<Task: {self.name}>"

    def dependencies(self):
        return self.implicit_dependencies + self.action.get_all_dependencies()

    def targets(self):
        return self.implicit_targets + self.action.get_all_targets()

    def as_graph_node(self) -> AbstractGraphNode:
        return TaskGraphNode(self.name)

    def execute(self, backend):
        self.action.execute(backend, self.name)


@dataclasses.dataclass
class OutputOf(AbstractDependency):
    # task depends on output of other task
    task: Task

    def is_up_to_date(self, this: Task, backend=None):
        pass


@dataclasses.dataclass
class FileDep(AbstractDependency):
    # task depends on file
    path: Path

    def is_up_to_date(self, this: Task, backend=None):
        pass


@dataclasses.dataclass
class TaskDep(AbstractDependency):
    # task depends on other task (implicitly)
    task: Task

    def is_up_to_date(self, this: Task, backend=None):
        pass
