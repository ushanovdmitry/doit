import unittest

from .backend import DictBackend


class DictBackendTest(unittest.TestCase):
    def test_fingerprints(self):
        b = DictBackend("main", None)

        b.set_task_fingerprint("T1", "abc")

        self.assertEqual("abc", b.get_task_fingerprint("T1"))

        with self.assertRaises(KeyError):
            b.get_task_fingerprint("T2")

        b.set_task_fingerprint("T1", "x")
        self.assertEqual("x", b.get_task_fingerprint("T1"))

    def test_run_with(self):
        b = DictBackend("main", None)

        b.set_task_run_with("T1", {
            "A1": "1",
            "A2": "2"
        })

        self.assertEqual("1", b.get_task_run_with("T1", "A1"))

        with self.assertRaises(KeyError):
            self.assertEqual("1", b.get_task_run_with("T1", "A3"))

        with self.assertRaises(KeyError):
            self.assertEqual("1", b.get_task_run_with("T2", "A1"))
