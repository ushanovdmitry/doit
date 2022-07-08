from doit import DAG, DictBackend, delayed, FileDep, FileTar
from doit.artifact import InMemoryArtifact

from pathlib import Path
import time


def process_file(file: FileTar, target: InMemoryArtifact):
    target.put_data(
        file.path.read_text(encoding='utf-8')
    )


def create_dag() -> DAG:
    dag = DAG("Batch main")

    for f in Path(r"C:\Users\Dmitrii\Desktop\many_files").glob("*.md"):
        dag.py_task(
            f"Parse {f}",
            delayed(process_file)(file=FileDep(f), target=InMemoryArtifact("artifact").tar)
        )

    return dag


def main():
    backend = DictBackend("Batch main", ".batch.json")

    while True:
        dag = create_dag()
        dag.render_online('https://dreampuf.github.io/GraphvizOnline/#')

        dag.run(backend, )

        return
        # time.sleep(10)


if __name__ == '__main__':
    main()