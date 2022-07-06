from abc import ABC
import pathlib
import hashlib
from typing import Any


class ArtifactLabel(ABC):
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

    def is_target(self) -> bool:
        """
        Type of artifact: target or dependency (this used in particular context)
        """
        raise NotImplementedError()

    def prepare_for_function_call(self) -> Any:
        """
        Helper for File-like targets
        """
        return self


class FileArtifact(ArtifactLabel):
    def __init__(self, path, is_target: bool):
        self._path = pathlib.Path(path).resolve()
        self._is_target = is_target

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

    def is_target(self) -> bool:
        return self._is_target

    def prepare_for_function_call(self):
        return self


class FileTar(FileArtifact):
    def __init__(self, path):
        super(FileTar, self).__init__(path, True)


class FileDep(FileArtifact):
    def __init__(self, path):
        super(FileDep, self).__init__(path, False)

