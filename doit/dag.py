from itertools import chain
from typing import Any, Union, List, Dict, Set

from graphlib import TopologicalSorter
from collections import defaultdict

from .action import PythonAction
from .task import Task
from .artifact import ArtifactLabel
from .backend import Backend
from .reporter import LogExecutionReporter, ExecutionReporter, DagEvent


def _get_subgraph(graph, labels: Set[str]):
    front = labels

    res = defaultdict(list)

    while front:
        new_front = set()
        for f in front:
            assert f not in res
            if f in graph:
                res[f] = graph[f]
                new_front.update(graph[f])
        front = new_front

    return res


def _all_nodes(graph):
    res = set()

    for k, v in graph.items():
        res.update([k])
        res.update(v)

    return res


class DAG:
    def __init__(self, dag_name: str, always_execute=False, reporter: ExecutionReporter = LogExecutionReporter()):
        self.dag_name = dag_name

        self.always_execute = always_execute
        self.reporter = reporter

        self.name2task = {}  # type: Dict[str, Task]

    def __str__(self):
        return f"<DAG: {self.dag_name}>"

    def py_task(self, name, action: PythonAction,
                targets: List[ArtifactLabel] = (), depends_on: List[ArtifactLabel] = (),
                depends_on_tasks: List[Task] = (),
                always_execute=None, execute_ones=None,
                reporter: ExecutionReporter = None):

        if always_execute is None:
            always_execute = self.always_execute
        if reporter is None:
            reporter = self.reporter

        assert name not in self.name2task

        t = Task(name, action,
                 implicit_dependencies=depends_on,
                 implicit_targets=targets,
                 implicit_task_dependencies=depends_on_tasks,
                 always_execute=always_execute,
                 execute_ones=execute_ones, ignore=False,
                 execution_reporter=reporter)

        self.name2task[t.name] = t

        return t

    def cmd_task(self):
        pass

    def append(self, _task: Task):
        assert _task.label() not in self.name2task
        self.name2task[_task.label()] = _task

    def _create_dict_graph(self, targets: list):
        graph = defaultdict(list)

        for task in self.name2task.values():
            for dep in task.dependencies():
                graph[task.label()].append(dep.label())
            for tar in task.targets():
                graph[tar.label()].append(task.label())
            for other in task.implicit_task_dependencies:  # type: Task
                graph[task.label()].append(other.label())

        if targets is not None:
            labels = set(t.label() for t in targets)

            return _get_subgraph(graph, labels)

        return graph

    def check_labels(self):
        # check that no artifact has the same label as any of the tasks
        a_names = set()

        for task in self.name2task.values():
            for a in chain(task.dependencies(), task.targets()):
                a_names.add(a.label())

        intersection = a_names.intersection(self.name2task.keys())

        if intersection:
            raise Exception(f"Artifact shares name with task: {intersection!r}")

    def run(self, backend: Backend, targets=None):
        self.check_labels()

        graph = self._create_dict_graph(targets)

        ts = TopologicalSorter(graph)
        ts.prepare()

        self.reporter.dag(DagEvent.START, self.dag_name)

        while ts.is_active():
            nodes = ts.get_ready()

            for node in nodes:
                if node in self.name2task:
                    task = self.name2task[node]
                    task.execute(backend)
                ts.done(node)

        self.reporter.dag(DagEvent.DONE, self.dag_name)

        backend.flush()

    def to_graphviz(self, targets=None) -> str:
        from .graphviz import Digraph

        if targets is not None:
            graph = self._create_dict_graph(targets)
            d = Digraph(
                target_nodes=set(t.label() for t in targets),
                affected_nodes=_all_nodes(graph)
            )
        else:
            d = Digraph()

        for task in self.name2task.values():
            for dep in task.dependencies():
                d.edge(dep, task)

            for tar in task.targets():
                d.edge(task, tar)

            for dep in task.implicit_task_dependencies:
                d.edge(dep, task)

        return d.source()

    def render_online(self, service_url: str, targets=None, open_url=True) -> None:
        import urllib.parse
        import webbrowser

        s = self.to_graphviz(targets)

        url = service_url + urllib.parse.quote(s, safe='')
        print(f'{url}')

        if open_url:
            webbrowser.open(url, )
