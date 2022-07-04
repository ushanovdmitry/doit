import dataclasses
import sys
from pathlib import Path
from typing import Any, Union, List

from doit.dependency import Dependency, DbmDB, MD5Checker, JSONCodec
from doit.version import VERSION

__version__ = VERSION


from doit import loader
from doit.loader import create_after, task_params
from doit.doit_cmd import get_var
from doit.api import run
from doit.tools import load_ipython_extension
from doit.globals import Globals

from doit.action import PythonAction, AbstractDependency, AbstractTarget
from doit.task import Task

from doit.cmd_run import Run
from doit.control import TaskControl
from doit.runner import Runner, Runner2


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
                 dependencies=depends_on, targets=targets,
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
        auto_delayed_regex = False

        continue_ = True
        always = False

        control_ = TaskControl(
            list(self.name2task.values()),
            auto_delayed_regex=auto_delayed_regex
        )
        control_.process(targets)

        single = False

        if single:
            control_.process(targets)
            for task_name in control_.selected_tasks:
                task = control_.tasks[task_name]
                if task.has_subtask:
                    for task_name in task.task_dep:
                        sub_task = control_.tasks[task_name]
                        sub_task.task_dep = []
                else:
                    task.task_dep = []

        # runner_ = Runner(self.dep_manager, continue_, always, stream)
        # runner_.run_all(control_.task_dispatcher())

        runner_ = Runner2(self.dep_manager)
        runner_.run_all(self)


__all__ = ['get_var', 'run', 'create_after', 'task_params', 'Globals', 'Task', 'DAG', 'PythonAction']


def get_initial_workdir():
    """working-directory from where the doit command was invoked on shell"""
    return loader.initial_workdir


assert load_ipython_extension  # silence pyflakes


