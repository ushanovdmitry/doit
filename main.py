from doit import DAG
from doit.task import FileDep, FileTar

from pathlib import Path


def foo(src: Path, target: Path):
    with open(target, 'a') as t:
        t.write("Hello world from %s" % src)
    print("Done!")


def bar():
    print("Something!")


if __name__ == '__main__':
    with DAG("main", ) as dag:
        t1 = dag.py_task(
            "task 1",
            foo, kwargs={
                "src": FileDep(Path("main.py")),
                "target": FileTar(Path("task 1 res.txt"))}
        )

    dag.run(targets=None)

