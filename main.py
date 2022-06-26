from doit import Task, DAG, PythonAction

from pathlib import Path


def foo(targets: list):
    for target in targets:
        with open(target, 'a') as t:
            t.write("Hello world")
            print(f"Written to {target} {t}")


if __name__ == '__main__':
    dag = DAG()

    t1 = Task(
        'first_task', [PythonAction(foo, )], targets=[Path('first_task_output.txt')]
    )

    dag.append(t1)

    dag.cli_main()

