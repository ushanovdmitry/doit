import sys

from doit.version import VERSION

__version__ = VERSION


from doit import loader
from doit.loader import create_after, task_params
from doit.doit_cmd import get_var
from doit.api import run
from doit.tools import load_ipython_extension
from doit.globals import Globals

from doit.action import PythonAction
from doit.task import Task


class DAG:
    def __init__(self):
        self.task_list = []

    def append(self, _task: Task):
        self.task_list.append(_task)

    def cli_main(self):
        from doit.doit_cmd import DoitMain
        DoitMain(self.task_list).run(sys.argv[1:])

    def run(self, targets=None):
        from doit.cmd_run import Run
        runner = Run(
            dag=self.task_list,
        )
        runner.execute({}, targets)


__all__ = ['get_var', 'run', 'create_after', 'task_params', 'Globals', 'Task', 'DAG', 'PythonAction']


def get_initial_workdir():
    """working-directory from where the doit command was invoked on shell"""
    return loader.initial_workdir


assert load_ipython_extension  # silence pyflakes


