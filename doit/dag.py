from typing import Any, Union, List, Dict

from graphlib import TopologicalSorter
from collections import defaultdict

from .action import PythonAction
from .task import Task
from .artifact import ArtifactLabel
from .backend import Backend


class DAG:
    def __init__(self, dag_name: str, always_execute=False):
        self.dag_name = dag_name

        self.always_execute = always_execute

        self.name2task = {}  # type: Dict[str, Task]

    def py_task(self, name, py_callable, args=None, kwargs=None,
                targets: List[ArtifactLabel] = (), depends_on: List[ArtifactLabel] = (),
                depends_on_tasks: List[Task] = (),
                always_execute=None, execute_ones=None):

        if always_execute is None:
            always_execute = self.always_execute

        assert name not in self.name2task

        t = Task(name, PythonAction(py_callable, args, kwargs),
                 implicit_dependencies=depends_on,
                 implicit_targets=targets,
                 implicit_task_dependencies=depends_on_tasks,
                 always_execute=always_execute,
                 execute_ones=execute_ones, ignore=False)

        self.name2task[t.name] = t

        return t

    def cmd_task(self):
        pass

    def append(self, _task: Task):
        assert _task.name not in self.name2task
        self.name2task[_task.name] = _task

    def _create_dict_graph(self):
        graph = defaultdict(list)

        for task in self.name2task.values():
            for dep in task.dependencies():
                graph[task.name].append(dep.label())
            for tar in task.targets():
                graph[tar.label()].append(task.name)
            for other in task.implicit_task_dependencies:  # type: Task
                graph[task.name].append(other.name)

        return graph

    def run(self, backend: Backend, targets=None):
        graph = self._create_dict_graph()

        ts = TopologicalSorter(graph)
        ts.prepare()

        while ts.is_active():
            nodes = ts.get_ready()

            for node in nodes:
                if node in self.name2task:
                    task = self.name2task[node]
                    task.execute(backend)
                ts.done(node)

        backend.flush()

    def to_graphviz(self) -> str:
        from .graphviz import Digraph

        d = Digraph()

        for task in self.name2task.values():
            for dep in task.dependencies():
                d.edge(dep, task)

            for tar in task.targets():
                d.edge(task, tar)

        return d.source()
