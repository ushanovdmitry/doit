import json
from abc import ABC
from typing import Union


class Backend(ABC):
    def set_task_run_with(self, task_name: str, artifact_label2fingerprint: dict) -> None:
        raise NotImplementedError()

    def get_task_run_with(self, task_name: str, artifact_label: str) -> str:
        raise NotImplementedError()

    def get_task_fingerprint(self, task_name: str) -> str:
        raise NotImplementedError()

    def set_task_fingerprint(self, task_name: str, fingerprint: str) -> None:
        raise NotImplementedError()

    def flush(self):
        raise NotImplementedError()

    def get_key(self, key):
        raise NotImplementedError()

    def set_key(self, key, val):
        raise NotImplementedError()


class DictBackend(Backend):
    def __init__(self, dag_name: str, filename: Union[str, None]):
        self.dag_name = dag_name
        self.filename = filename
        self.d = dict()

        if filename is not None:
            try:
                with open(filename, encoding='utf-8', mode='r') as fp:
                    self.d = json.load(fp)
            except FileNotFoundError:
                pass

    def _get_key(self, *args, create=False):
        c = self.d

        for step in args:
            if create and step not in c:
                c[step] = {}

            c = c[step]

        return c

    def set_task_run_with(self, task_name: str, artifact_label2fingerprint: dict) -> None:
        c = self._get_key(self.dag_name, "tasks", task_name, create=True)
        c["dependencies_when_called"] = artifact_label2fingerprint.copy()

    def get_task_run_with(self, task_name: str, artifact_label: str) -> str:
        return self._get_key(
            self.dag_name,
            "tasks",
            task_name,
            "dependencies_when_called",
            artifact_label,
            create=False
        )

    def get_task_fingerprint(self, task_name: str) -> None:
        c = self._get_key(self.dag_name, "tasks", task_name, "fingerprint")
        return c

    def set_task_fingerprint(self, task_name: str, fingerprint: str) -> None:
        c = self._get_key(self.dag_name, "tasks", task_name, create=True)
        c["fingerprint"] = fingerprint

    def flush(self):
        if self.filename is not None:
            with open(self.filename, encoding='utf-8', mode='w') as fp:
                json.dump(self.d, fp, indent=' ', ensure_ascii=False)

    def get_key(self, key):
        return self.d["KV"][key]

    def set_key(self, key, val):
        self._get_key("KV", create=True)[key] = val

