import sys
from typing import Any, Union, List

from doit.dependency import Dependency, DbmDB, MD5Checker, JSONCodec
from doit.version import VERSION

__version__ = VERSION

from doit.action import PythonAction, AbstractDependency, AbstractTarget
from doit.task import Task


class DAG:
    def __init__(self, dag_name: str, always_execute=False, continue_on_failure=False):
        self.dag_name = dag_name

        self.always_execute = always_execute
        self.continue_on_failure = continue_on_failure

        self.dep_manager = Dependency(
            DbmDB, '.doit.db', checker_cls=MD5Checker, codec_cls=JSONCodec
        )

        self.name2task = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        1. Regular expressions
        2. Last dependencies (by name, z.B.)
        3. Check for cycles
        """
        pass

    def py_task(self, name, py_callable, args=None, kwargs=None,
                targets: List[AbstractTarget] = (), depends_on: List[AbstractDependency] = (),
                always_execute=None, continue_on_failure=None,
                depends_on_src=False, execute_ones=None):

        t = Task(name, PythonAction(py_callable, args, kwargs),
                 implicit_dependencies=depends_on, implicit_targets=targets,
                 always_execute=always_execute,
                 continue_on_failure=continue_on_failure,
                 execute_ones=execute_ones)

        assert t.name not in self.name2task
        self.name2task[t.name] = t

    def cmd_task(self):
        pass

    def append(self, _task: Task):
        assert _task.name not in self.name2task
        self.name2task[_task.name] = _task

    def cli_main(self):
        from doit.doit_cmd import DoitMain
        DoitMain(self.task_list).run(sys.argv[1:])

    def run(self, targets=None):
        from doit.runner import Runner2

        runner_ = Runner2(self.dep_manager)
        runner_.run_all(self)


