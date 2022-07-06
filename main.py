from doit import DAG, FileDep, FileTar, DictBackend

import pprint


def foo(src: FileDep, target: FileTar):
    with open(target.path, 'a') as t:
        t.write("Hello world from %s" % src.path)
    print("Done foo!")


def bar():
    print("Something bar!")


if __name__ == '__main__':
    dag = DAG("main", )

    t1 = dag.py_task(
        "Task 1",
        foo, kwargs={
            "src": FileDep("main.py"),
            "target": FileTar("task 1 res.txt")}
    )

    dag.py_task(
        "Task 2",
        bar,
        depends_on=[FileDep("task 1 res.txt")]
    )

    dag.py_task(
        "Task 3",
        bar,
        depends_on=[FileDep("task 1 res.txt")],
        depends_on_tasks=[t1, ]
    )

    print(dag.to_graphviz())

    back = DictBackend(dag.dag_name, ".doit.json")
    dag.run(back, targets=None)

    pprint.pprint(back.d)

