import pathlib
import hashlib
from typing import Any, Dict

from .node import Node


class ArtifactLabel(Node):
    def fingerprint(self) -> str:
        """
        String repr to check if artifact has changed
        """
        raise NotImplementedError()

    def exists(self) -> bool:
        """
        For file-like objects - if not exists, need to recalculate
        """
        raise NotImplementedError()

    def label(self) -> str:
        """
        To insert into runner's graph.
        Serves as a key in backends
        """
        raise NotImplementedError()

    def prepare_for_function_call(self) -> Any:
        """
        Helper for File-like targets
        """
        return self

    @property
    def tar(self):
        """
        Make target to use in *args and **kwargs
        """
        return AsTargetArtifact(self)

    @property
    def dep(self):
        """
        Make dependency to use in *args and **kwargs
        """
        return AsDependencyArtifact(self)


class AsDependencyArtifact:
    def __init__(self, a: ArtifactLabel):
        self.a = a


class AsTargetArtifact:
    def __init__(self, a: ArtifactLabel):
        self.a = a


# ----------------------------------------------------------------------------------------------------------------------

class File(ArtifactLabel):
    def __init__(self, path):
        self._path = pathlib.Path(path).resolve()

    @property
    def path(self):
        return self._path

    def fingerprint(self) -> str:
        with self._path.open("rb") as f:
            file_hash = hashlib.md5()
            while chunk := f.read(8192):
                file_hash.update(chunk)

        return file_hash.hexdigest()

    def exists(self) -> bool:
        return self._path.exists() and self._path.is_file()

    def label(self) -> str:
        return "[File] " + self._path.__str__()

    def prepare_for_function_call(self):
        return self


class InMemoryArtifact(ArtifactLabel):
    label2data = {}  # type: Dict[str, str]

    def __init__(self, label):
        self._label = label
        self._fingerprint_calls = 0

    def __str__(self):
        return f"<Artifact: {self._label}>"

    def fingerprint(self) -> str:
        self._fingerprint_calls += 1

        file_hash = hashlib.md5()
        file_hash.update(self.label2data[self._label].encode('utf-8'))
        return file_hash.hexdigest()

    def exists(self) -> bool:
        return self._label in self.label2data

    def label(self) -> str:
        return self._label

    def put_data(self, data: str):
        self.label2data[self._label] = data
