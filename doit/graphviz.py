from typing import List, Any

from .artifact import FileArtifact, ArtifactLabel
from .task import Task
import graphviz


def props2str(d: dict) -> str:
    if not d:
        return ""

    return "[" + ", ".join(f"{k}={v}" for k, v in d.items()) + "]"


def prepare_node(n: str):
    return n.replace('"', "")


class Digraph:
    def __init__(self):
        self.worker = graphviz.Digraph()

        self.visited_nodes = set()

    def raw_node(self, node: str, **kwargs) -> None:
        node = prepare_node(node)

        if node in self.visited_nodes:
            return

        self.visited_nodes.add(node)
        self.worker.node(name=node, **kwargs)

    def raw_edge(self, from_: str, to_: str, **kwargs) -> None:
        from_ = prepare_node(from_)
        to_ = prepare_node(to_)

        # self.lines.append(
        #     f'''    "{from_}" -> "{to_}" {props2str(kwargs)}'''
        # )

        self.worker.edge(
            tail_name=from_,
            head_name=to_,
            **kwargs
        )

    def node(self, node) -> str:
        if isinstance(node, Task):
            self.raw_node(
                node.name,
                shape="oval", color="gold", style="filled"
            )
            return node.name

        if isinstance(node, FileArtifact):
            self.raw_node(
                node.label(),
                shape="folder"
            )
            return node.label()

        if isinstance(node, ArtifactLabel):
            self.raw_node(node.label())
            return node.label()

        raise NotImplementedError()

    def edge(self, from_: Any, to_: Any):
        from_label = self.node(from_)
        to_label = self.node(to_)
        self.raw_edge(from_label, to_label, arrowhead="vee")

    def source(self) -> str:
        # return "\n".join(
        #     chain(
        #         ["digraph {"],
        #         self.lines,
        #         ["}"]
        #     )
        # )
        return self.worker.source
