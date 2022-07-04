"""Tasks are the main abstractions managed by doit"""
import dataclasses
from pathlib import Path
from typing import List, Any

from . import AbstractDependency, AbstractTarget
from .action import AbstractAction, AbstractGraphNode, CanRepresentGraphNode


@dataclasses.dataclass(frozen=True)
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
        # 1. check if you need to execute
        # 2. execute

        if not self.need_execute(backend):
            return

        try:
            self.action.execute(backend)

            for tar in self.targets():
                tar.store(backend)
        except:
            if self.continue_on_failure:
                return

            backend.flush()
            raise

    def need_execute(self, backend):
        if self.always_execute:
            return True

        if not self.dependencies():
            # no dependencies = always execute
            return True

        for dep in self.dependencies():
            if not dep.is_up_to_date(backend):
                return True

        for tar in self.targets():
            if not tar.exists(backend):
                return True

        return False


@dataclasses.dataclass
class OutputOf(AbstractDependency):
    # task depends on output of other task
    task: Task

    def is_up_to_date(self, this: Task, backend=None):
        pass


@dataclasses.dataclass(frozen=True)
class FileGraphNode(AbstractGraphNode):
    path: Path


@dataclasses.dataclass
class FileDep(AbstractDependency):
    # task depends on file
    path: Path

    def is_up_to_date(self, backend):
        # check md5
        pass

    def value(self, backend) -> Any:
        return self.path

    def as_graph_node(self) -> AbstractGraphNode:
        return FileGraphNode(self.path)


@dataclasses.dataclass
class TaskDep(AbstractDependency):
    # task depends on other task (implicitly)
    task: Task

    def is_up_to_date(self, backend):
        pass

    def value(self, backend) -> Any:
        raise NotImplementedError()

    def as_graph_node(self) -> AbstractGraphNode:
        return TaskGraphNode(self.task.name)


@dataclasses.dataclass
class FileTar(AbstractTarget):
    path: Path

    def exists(self, backend):
        return self.path.exists() and self.path.is_file()

    def value(self, backend) -> Any:
        return self.path

    def as_graph_node(self) -> AbstractGraphNode:
        return FileGraphNode(self.path)

    def store(self, backend):
        # write md5 to backend
        pass


@dataclasses.dataclass
class OutVal(AbstractTarget):
    val: Any

    def exists(self, backend):
        pass

    def value(self, backend) -> Any:
        return self

    def as_graph_node(self) -> AbstractGraphNode:
        pass

    def store(self, backend):
        pass
