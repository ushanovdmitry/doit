import unittest

from doit import Task, DAG, PythonAction
from doit.task import clean_targets
from doit.tools import run_once, result_dep

from pathlib import Path

from doit.action import CmdAction

from unittest.mock import patch, mock_open, call
import main


class IntegrationTest(unittest.TestCase):
    def test_1(self):
        def foo_1(targets):
            with open(targets[0], 'w') as out:
                out.write('foo 1')

        def foo_2(targets):
            with open(targets[0], 'w') as out:
                out.write('foo 2')

        task_1 = Task("task 1", [PythonAction(foo_1)], file_dep=[], targets=["task 1 out.txt"])
        task_2 = Task("task 2", [PythonAction(foo_2)], file_dep=["task 1 out.txt"], targets=["task 2 out.txt"])

        dag = DAG()
        dag.append(task_2)
        dag.append(task_1)

        dag.run()


if __name__ == '__main__':
    unittest.main()

