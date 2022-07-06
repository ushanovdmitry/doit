import unittest

from .dag import DAG
from .backend import DictBackend
from .artifact import FileDep, FileTar


class IntegrationTest(unittest.TestCase):
    def test_1(self):
        glval = []

        def foo_1(target: FileTar):
            with open(target.path, 'w') as out:
                out.write('foo 1')
            glval.append('1')

        def foo_2(target: FileTar):
            with open(target.path, 'w') as out:
                out.write('foo 2')
            glval.append('2')

        dag = DAG("main")

        dag.py_task("Task #1", foo_1, args=[FileTar("task1res.txt")])
        dag.py_task("Task #2", foo_2, args=[FileTar("task2res.txt")], depends_on=[FileDep("task1res.txt")])

        back = DictBackend(dag.dag_name, None)

        # first run: all tasks should be executed
        dag.run(back)
        self.assertEqual(['1', '2'], glval)

        # second run: only first should be executed - not dependencies
        # second task is not executed - md5 of files not changed
        dag.run(back)
        self.assertEqual(['1', '2', '1', ], glval)

    def test_2(self):
        glval = []

        def foo_1(target: FileTar):
            with open(target.path, 'w') as out:
                out.write('foo 1')
            glval.append('1')

        def foo_2(target: FileTar):
            with open(target.path, 'w') as out:
                out.write('foo 2')
            glval.append('2')

        dag = DAG("main")

        t1 = dag.py_task("Task #1", foo_1, args=[FileTar("task1res.txt")])
        dag.py_task("Task #2", foo_2, args=[FileTar("task2res.txt")], depends_on=[FileDep("task1res.txt")],
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


if __name__ == '__main__':
    unittest.main()

