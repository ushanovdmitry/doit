from doit import Task, DAG, PythonAction

from pathlib import Path

from doit.action import CmdAction


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

    t2 = Task(
        'second_task', [CmdAction('type %(dependencies)s > %(targets)s')],
        file_dep=['main.py'],
        targets=[Path('main.models.deps.txt')]
    )

    t3 = Task(
        'last-step',
        file_dep=t1.targets + t2.targets,
        actions=[PythonAction(lambda targets: print(targets, "In here!"))],
        targets=[]
    )

    dag.append(t3)
    dag.append(t1)
    dag.append(t2)

    dag.cli_main()
    # dag.run(targets=['*step', ])

