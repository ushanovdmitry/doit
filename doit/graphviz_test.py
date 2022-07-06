import unittest

from .graphviz import props2str, Digraph


class GVTest(unittest.TestCase):
    def test_props2str(self):
        self.assertEqual(props2str(None), "")
        self.assertEqual(props2str({}), "")
        self.assertEqual(props2str({'red': 'flag'}), "[red=flag]")
        self.assertEqual(props2str({'red': 'flag', 'blue': 'val'}), "[red=flag, blue=val]")

    def test_digraph(self):
        g = Digraph()

        g.raw_node('hello worl"d', shape="box")
        g.raw_node('x')
        g.raw_node('x')

        g.raw_edge('hello worl"d', 'x')

        self.assertEqual(
            g.source(),
            '\n'.join(["digraph {",
                       '\t"hello world" [shape=box]',
                       '\t"x" ',
                       '\t"hello world" -> "x" ',
                       "}"])
        )


if __name__ == '__main__':
    unittest.main()
