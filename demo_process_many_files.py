from doit import DAG, DictBackend, delayed, File
from doit.artifact import InMemoryArtifact
from doit.backend import Backend
from doit.reporter import TaskEvent
from doit.task import Task

from pathlib import Path
import time

from doit.task import AutoUpdate


def process_file(file: File, target: InMemoryArtifact):
    target.put_data(
        file.path.read_text(encoding='utf-8')
    )


def node2group(node):
    if isinstance(node, Task):
        if node.name.startswith('Parse'):
            return "Parser nodes"

    if isinstance(node, File):
        if 'many_files' in str(node.path):
            return "Input file nodes"

    return None


def create_dag(backend: Backend) -> DAG:
    dag = DAG("Batch main")

    sink = AutoUpdate("sink", backend)

    for f in Path(r"C:\Users\Dmitrii\Desktop\many_files").glob("*.md"):
        dag.py_task(
            f"Parse {f}",
            delayed(process_file)(file=File(f).dep, target=InMemoryArtifact("artifact").tar),
            reporter=dag.reporter.filter_events(keep_task_events=(TaskEvent.EXECUTE, )),
            targets=[sink, ]
        )

    dag.py_task("Say hello", delayed(print)("Hello there!"), depends_on=[sink, ])

    return dag


def main():
    backend = DictBackend("Batch main", ".batch.json")

    while True:
        dag = create_dag(backend)
        dag.render_online('https://dreampuf.github.io/GraphvizOnline/#', node2group=node2group)

        dag.run(backend, )

        return
        # time.sleep(10)


if __name__ == '__main__':
    main()
