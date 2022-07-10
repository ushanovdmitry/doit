from itertools import chain
from typing import Any, Union, List, Dict, Set

from graphlib import TopologicalSorter
from collections import defaultdict

from .action import PythonAction
from .task import Task
from .artifact import ArtifactLabel
from .backend import Backend
from .reporter import LogExecutionReporter, ExecutionReporter, DagEvent
from .node import Node


class DepGraph:
    def __init__(self, dep_graph: Dict[str, List[str]], label2node: Dict[str, Node]):
        """

        :param dep_graph: dictionary from node to a list if its dependencies (as in graphlib.TopologicalSorter)
        """
        self.dep_graph = dep_graph
        self.label2node = label2node

    def subgraph(self, labels: Set[str]):
        front = labels

        res = defaultdict(list)

        while front:
            new_front = set()
            for f in front:
                assert f not in res
                if f in self.dep_graph:
                    res[f] = self.dep_graph[f]
                    new_front.update(self.dep_graph[f])
            front = new_front

        return DepGraph(res)

    def all_nodes(self):
        res = set()

        for k, v in self.dep_graph.items():
            res.update([k])
            res.update(v)

        return res

    def edges(self):
        """
        Yields pairs of the form (head, tail), where head depends on tail.
        head <- tail
        """
        for k, v_ in self.dep_graph.items():
            for v in v_:
                yield k, v


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

    def _create_dep_graph(self) -> DepGraph:
        graph = defaultdict(list)

        for task in self.name2task.values():
            for dep in task.dependencies():
                graph[task.label()].append(dep.label())
            for tar in task.targets():
                graph[tar.label()].append(task.label())
            for other in task.implicit_task_dependencies:  # type: Task
                graph[task.label()].append(other.label())

        return DepGraph(graph)

    def _create_label2obj(self) -> Dict[str, Node]:
        res = dict()  # type: Dict[str, Node]

        for task in self.name2task.values():
            res[task.label()] = task
            for obj in chain(task.dependencies(), task.targets(), task.implicit_task_dependencies):
                res[obj.label()] = obj

        return res

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

        graph = self._create_dep_graph()
        if targets is not None:
            graph = graph.subgraph(set(_.label() for _ in targets))

        ts = TopologicalSorter(graph.dep_graph)
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

    def to_graphviz(self, targets=None, node2group=None):
        from .graphviz import Renderer

        full_graph = self._create_dep_graph()
        label2obj = self._create_label2obj()

        target_labels = set() if targets is None else set(t.label() for t in targets)
        affected_labels = set() if targets is None else full_graph.subgraph(target_labels).all_nodes()

        r = Renderer()
        r.insert_dag(label2obj, full_graph, target_labels, affected_labels, node2group)

        return r
