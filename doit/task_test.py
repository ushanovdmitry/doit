import unittest

from .task import AutoUpdate
from .backend import DictBackend


class AutoUpdateTest(unittest.TestCase):
    def test_fp(self):
        au = AutoUpdate("au", DictBackend("main", None))

        with self.assertRaises(KeyError):
            au.fingerprint()

        au.update_fingerprint()
        self.assert_(au.fingerprint()[0] == "0")

        au.update_fingerprint()
        self.assert_(au.fingerprint()[0] == "1")


if __name__ == '__main__':
    unittest.main()
