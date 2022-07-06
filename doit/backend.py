from abc import ABC


class Backend(ABC):
    def set_task_run_with(self, task_name: str, artifact_label2fingerprint: dict) -> None:
        raise NotImplementedError()

    def get_task_run_with(self, task_name: str, artifact_label: str) -> str:
        raise NotImplementedError()

    def flush(self):
        raise NotImplementedError()


class DictBackend(Backend):
    def __init__(self, dag_name: str):
        self.dag_name = dag_name
        self.d = dict()

    def get_key(self, *args, create=False):
        c = self.d

        for step in args:
            if create and step not in c:
                c[step] = {}

            c = c[step]

        return c

    def set_task_run_with(self, task_name: str, artifact_label2fingerprint: dict) -> None:
        c = self.get_key(self.dag_name, "tasks", create=True)
        c[task_name] = artifact_label2fingerprint.copy()

    def get_task_run_with(self, task_name: str, artifact_label: str) -> str:
        return self.get_key(
            self.dag_name,
            "tasks",
            task_name,
            artifact_label,
            create=False
        )

    def flush(self):
        pass

