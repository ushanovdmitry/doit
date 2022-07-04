from doit import Task, DAG, PythonAction
from doit.task import clean_targets, FileDep
from doit.tools import run_once, result_dep

from pathlib import Path

from doit.action import CmdAction


GLOBAL_OBJ = []


def foo(targets: list):
    for target in targets:
        GLOBAL_OBJ.append(target)

        with open(target, 'a') as t:
            t.write("Hello world")

    # raise Exception("")


def bar():
    print("Something!")


if __name__ == '__main__':
    with DAG("main", ) as dag:
        t1 = dag.py_task(
            "task 1",
            print, args=[FileDep("main.py")]
        )

    dag.run(targets=None)

    # assert GLOBAL_OBJ == ['first_task_output.txt', []]
