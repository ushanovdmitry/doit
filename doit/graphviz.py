import dataclasses
from itertools import chain
from typing import List, Any, Callable, Tuple, Set

from .artifact import File, ArtifactLabel, InMemoryArtifact
from .task import Task


def props2str(d: dict) -> str:
    if not d:
        return ""

    return "[" + ", ".join(f"{k}={v}" for k, v in d.items()) + "]"


def prepare_node(n: str):
    return n.replace('"', "").replace("\\", "\\\\")


@dataclasses.dataclass
class NodeRenderer:
    label: Callable[[Any, ], str] = lambda x: x.label()
    attrs: dict = None


DEFAULT_STYLES: List[Tuple[Any, NodeRenderer]] = [
    (Task, NodeRenderer(lambda x: x.name, dict(shape="oval", color="gold"))),
    (File, NodeRenderer(lambda x: x.label(), dict(shape="rectangle"))),
    (InMemoryArtifact, NodeRenderer(lambda x: x.label(), dict(shape="rectangle", color="blue"))),
    (ArtifactLabel, NodeRenderer(lambda x: x.label(), dict())),
]


class Digraph:
    def __init__(self, target_nodes: Set[str] = None, affected_nodes: Set[str] = None,
                 styles: List[Tuple[Any, NodeRenderer]] = None):
        if styles is None:
            styles = DEFAULT_STYLES

        self.lines = []
        self.styles = styles

        self.target_nodes = target_nodes or {}
        self.affected_nodes = affected_nodes or {}

        self.visited_nodes = set()

    def raw_node(self, node: str, **kwargs) -> None:
        if node in self.target_nodes:
            kwargs['color'] = 'red'
        if node in self.affected_nodes:
            kwargs['style'] = 'filled'
            kwargs['fillcolor'] = 'lightgrey'

        node = prepare_node(node)

        if node in self.visited_nodes:
            return

        self.visited_nodes.add(node)

        self.lines.append(
            f'''\t"{node}" {props2str(kwargs)}'''
        )

    def raw_edge(self, from_: str, to_: str, **kwargs) -> None:
        from_ = prepare_node(from_)
        to_ = prepare_node(to_)

        self.lines.append(
            f'''\t"{from_}" -> "{to_}" {props2str(kwargs)}'''
        )

    def node(self, node) -> str:
        for klass, nr in self.styles:
            if isinstance(node, klass):
                self.raw_node(
                    nr.label(node), **nr.attrs
                )
                return nr.label(node)

        raise NotImplementedError()

    def edge(self, from_: Any, to_: Any):
        from_label = self.node(from_)
        to_label = self.node(to_)
        self.raw_edge(from_label, to_label, arrowhead="vee")

    def source(self) -> str:
        return "\n".join(
            chain(
                ["digraph {"],
                self.lines,
                ["}"]
            )
        )
