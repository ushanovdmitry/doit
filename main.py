from doit import Task, DAG
from doit.action import PythonAction
from pathlib import Path
import doit


def foo(targets: list):
    for target in targets:
        with open(target, 'a') as t:
            t.write("Hello world")
            print(f"Written to {target} {t}")


if __name__ == '__main__':
    dag = DAG()

    dag.append(
        Task(
            'first_task', [PythonAction(foo, )],
            targets=[Path('first_task_output.txt')]
        )
    )

    doit.main(dag)
