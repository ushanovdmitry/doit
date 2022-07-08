from doit import DAG, DictBackend, delayed, File
from doit.artifact import InMemoryArtifact
from doit.reporter import TaskEvent

from pathlib import Path
import time


def process_file(file: File, target: InMemoryArtifact):
    target.put_data(
        file.path.read_text(encoding='utf-8')
    )


def create_dag() -> DAG:
    dag = DAG("Batch main")

    for f in Path(r"C:\Users\Dmitrii\Desktop\many_files").glob("*.md"):
        dag.py_task(
            f"Parse {f}",
            delayed(process_file)(file=File(f).dep, target=InMemoryArtifact("artifact").tar),
            reporter=dag.reporter.filter_events(keep_task_events=(TaskEvent.EXECUTE, ))
        )

    return dag


def main():
    backend = DictBackend("Batch main", ".batch.json")

    while True:
        dag = create_dag()
        # dag.render_online('https://dreampuf.github.io/GraphvizOnline/#')

        dag.run(backend, )

        return
        # time.sleep(10)


if __name__ == '__main__':
    main()
