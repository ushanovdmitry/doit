from itertools import chain
from typing import Any, Set, Dict, List, Tuple
import inspect

from .artifact import File, ArtifactLabel, InMemoryArtifact
from .task import Task
from .node import Node

from .dag import DepGraph


def props2str(d: dict) -> str:
    if not d:
        return ""

    return "[" + ", ".join(f"{k}={v}" for k, v in d.items()) + "]"


def prepare_label_str(n: str):
    return n.replace('"', "").replace("\\", "\\\\")


class GroupOfNodes(Node):
    def __init__(self, label):
        self._label = label
        self.nodes = []

    def label(self):
        return self._label

    def add_node(self, node: Node):
        self.nodes.append(node)


DEFAULT_STYLES = {
    Task: dict(shape="oval", color="gold"),
    File: dict(shape="rectangle"),
    InMemoryArtifact: dict(shape="rectangle", color="blue"),
    ArtifactLabel: dict(),

    GroupOfNodes: dict(shape="box3d"),

    "target_node": dict(color='red'),
    "affected_node": dict(style='filled', fillcolor='lightgrey')
}


def _merge_dag(name2node: Dict[str, Node], graph: DepGraph, node2group):
    """
    Merge groups.

    :param name2node: dictionary name -> object
    :param graph: dictionary name -> it's dependencies (as strings)
    :param node2group: callable: name -> group label or None, if node belongs to none group
    """

    nodes = {}
    edges = []

    group_name2group = {}

    for x in graph.all_nodes():
        x_group_name = node2group(name2node[x])
        if x_group_name is not None:
            if x_group_name not in group_name2group:
                group_name2group[x_group_name] = GroupOfNodes(x_group_name)
            group_name2group[x_group_name].add_node(name2node[x])


    for x, y in graph.edges():
        x_group = node2group(name2node[x])
        y_group = node2group(name2node[y])

        if x_group is not None:
            pass


class Digraph:
    def __init__(self):
        self.lines = []
        self.visited_nodes = set()  # type: Set[str]
        self.visited_edges = set()  # type: Set[Tuple[str, str]]

    def raw_node(self, node: str, **kwargs) -> None:
        node = prepare_label_str(node)

        if node in self.visited_nodes:
            return

        self.visited_nodes.add(node)

        self.lines.append(
            f'''\t"{node}" {props2str(kwargs)}'''
        )

    def raw_edge(self, from_: str, to_: str, **kwargs) -> None:
        from_ = prepare_label_str(from_)
        to_ = prepare_label_str(to_)

        self.lines.append(
            f'''\t"{from_}" -> "{to_}" {props2str(kwargs)}'''
        )


class Renderer:
    def __init__(self, styles=None):
        if styles is None:
            styles = DEFAULT_STYLES

        self.di = Digraph()

        self.styles = styles

    def insert_dag(self, name2node, dep_graph, target_names: Set[str], affected_names: Set[str], name2group):
        pass

    def source(self) -> str:
        return "\n".join(
            chain(
                ["digraph {"],
                self.di.lines,
                ["}"]
            )
        )

    def render_online(self, service_url: str, open_url=True) -> None:
        import urllib.parse
        import webbrowser

        url = service_url + urllib.parse.quote(self.source(), safe='')
        print(f'{url}')

        if open_url:
            webbrowser.open(url, )
