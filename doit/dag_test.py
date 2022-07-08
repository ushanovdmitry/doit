import unittest

from .dag import DAG
from .backend import DictBackend
from .artifact import InMemoryArtifact, FileDep
from .action import delayed
from .reporter import ExecutionReporter, DagEvent, TaskEvent


class Rep(ExecutionReporter):
    def __init__(self):
        self.t = []
        self.d = []

    def task(self, event: TaskEvent, task_name: str, reason: str):
        self.t.append(event)

    def dag(self, event: DagEvent, dag_name: str):
        self.d.append(event)


class IntegrationTest(unittest.TestCase):
    def test_1(self):
        glval = []

        def foo_1(target: InMemoryArtifact):
            target.put_data('foo 1')
            glval.append('1')

        def foo_2(target: InMemoryArtifact):
            target.put_data('foo 2')
            glval.append('2')

        rep = Rep()
        dag = DAG("main", reporter=rep)

        art1 = InMemoryArtifact("task1res.txt", True)
        art2 = InMemoryArtifact("task2res.txt", True)

        dag.py_task("Task #1", delayed(foo_1)(art1))
        dag.py_task("Task #2", delayed(foo_2)(art2), depends_on=[art1, ])

        back = DictBackend(dag.dag_name, None)

        # first run: all tasks should be executed
        dag.run(back)
        self.assertEqual(['1', '2'], glval)
        self.assertEqual([TaskEvent.EXECUTE, TaskEvent.EXECUTE], rep.t)

        # second run: only first should be executed - not dependencies
        # second task is not executed - md5 of files not changed
        dag.run(back)
        self.assertEqual(['1', '2', '1', ], glval)
        self.assertEqual([TaskEvent.EXECUTE, TaskEvent.EXECUTE,
                          TaskEvent.EXECUTE, TaskEvent.SKIP], rep.t)

        # check number of calls to fingerprint
        self.assertEqual(2, art1._fingerprint_calls)

    def test_2(self):
        glval = []

        def foo_1(target: InMemoryArtifact):
            target.put_data('foo 1')
            glval.append('1')

        def foo_2(target: InMemoryArtifact):
            target.put_data('foo 2')
            glval.append('2')

        rep = Rep()
        dag = DAG("main", reporter=rep)

        art1 = InMemoryArtifact("task1res.txt", True)
        art2 = InMemoryArtifact("task2res.txt", True)

        t1 = dag.py_task("Task #1", delayed(foo_1)(art1))
        dag.py_task("Task #2", delayed(foo_2)(art2), depends_on=[art1],
                    depends_on_tasks=[t1, ])

        back = DictBackend(dag.dag_name, None)

        # Do two runs in a row
        # As task dependency clearly stated in the depends_on_tasks param, both tasks should be executed
        dag.run(back)
        self.assertEqual([DagEvent.START, DagEvent.DONE], rep.d)
        self.assertEqual([TaskEvent.EXECUTE, TaskEvent.EXECUTE], rep.t)

        dag.run(back)
        self.assertEqual([DagEvent.START, DagEvent.DONE, DagEvent.START, DagEvent.DONE], rep.d)
        self.assertEqual([TaskEvent.EXECUTE, TaskEvent.EXECUTE, TaskEvent.EXECUTE, TaskEvent.EXECUTE], rep.t)
        self.assertEqual(['1', '2', '1', '2'], glval)

        # ignore t1 => second task shouldn't get executed too...
        t1.ignore = True
        dag.run(back)
        self.assertEqual(['1', '2', '1', '2'], glval, msg="ignore t1 => second task shouldn't get executed too...")


class DagTest(unittest.TestCase):
    def test_check_labels(self):
        fdep = FileDep(".")

        dag = DAG("main")
        dag.py_task("Task 1", delayed(print)())
        dag.py_task(fdep.label(), delayed(print)(), depends_on=[FileDep(".")])

        with self.assertRaises(Exception) as context:
            dag.check_labels()

    def test__get_subgraph(self):
        from .dag import _get_subgraph

        self.assertEqual(
            {'a': ['b', 'c'], 'b': ['c', 'd']},
            _get_subgraph({'a': ['b', 'c'], 'b': ['c', 'd']}, {'a'})
        )

        self.assertEqual(
            {},
            _get_subgraph({'a': ['b', 'c'], 'b': ['c', 'd']}, {'d'})
        )

        self.assertEqual(
            {'b': ['c', 'd']},
            _get_subgraph({'a': ['b', 'c'], 'b': ['c', 'd']}, {'b'})
        )

        self.assertEqual(
            {},
            _get_subgraph({'a': ['b', 'c'], 'b': ['c', 'd']}, {'x'})
        )

    def test__all_nodes(self):
        from .dag import _all_nodes

        self.assertEqual(
            {'a', 'b', 'c', 'd'},
            _all_nodes({'a': ['b', 'c'], 'b': ['c', 'd']})
        )


if __name__ == '__main__':
    unittest.main()

