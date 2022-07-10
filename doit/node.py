from abc import ABC


class Node(ABC):
    def label(self):
        raise NotImplementedError()


