import unittest

from .dag import DAG
from .backend import DictBackend
from .artifact import InMemoryArtifact, FileDep


class IntegrationTest(unittest.TestCase):
    def test_1(self):
        glval = []

        def foo_1(target: InMemoryArtifact):
            target.put_data('foo 1')
            glval.append('1')

        def foo_2(target: InMemoryArtifact):
            target.put_data('foo 2')
            glval.append('2')

        dag = DAG("main")

        art1 = InMemoryArtifact("task1res.txt", True)
        art2 = InMemoryArtifact("task2res.txt", True)

        dag.py_task("Task #1", foo_1, args=[art1])
        dag.py_task("Task #2", foo_2, args=[art2], depends_on=[art1])

        back = DictBackend(dag.dag_name, None)

        # first run: all tasks should be executed
        dag.run(back)
        self.assertEqual(['1', '2'], glval)

        # second run: only first should be executed - not dependencies
        # second task is not executed - md5 of files not changed
        dag.run(back)
        self.assertEqual(['1', '2', '1', ], glval)

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

        dag = DAG("main")

        art1 = InMemoryArtifact("task1res.txt", True)
        art2 = InMemoryArtifact("task2res.txt", True)

        t1 = dag.py_task("Task #1", foo_1, args=[art1])
        dag.py_task("Task #2", foo_2, args=[art2], depends_on=[art1],
                    depends_on_tasks=[t1, ])

        back = DictBackend(dag.dag_name, None)

        # Do two runs in a row
        # As task dependency clearly stated in the depends_on_tasks param, both tasks should be executed
        dag.run(back)
        dag.run(back)
        self.assertEqual(['1', '2', '1', '2'], glval)

        # ignore t1 => second task shouldn't get executed too...
        t1.ignore = True
        dag.run(back)
        self.assertEqual(['1', '2', '1', '2'], glval, msg="ignore t1 => second task shouldn't get executed too...")


class DagTest(unittest.TestCase):
    def test_check_labels(self):
        fdep = FileDep(".")

        dag = DAG("main")
        dag.py_task("Task 1", print)
        dag.py_task(fdep.label(), print, depends_on=[FileDep(".")])

        with self.assertRaises(Exception) as context:
            dag.check_labels()


if __name__ == '__main__':
    unittest.main()

