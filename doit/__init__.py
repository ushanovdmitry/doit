import sys

from doit.dependency import Dependency, DbmDB, MD5Checker, JSONCodec
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

from doit.cmd_run import Run
from doit.control import TaskControl
from doit.runner import Runner
from doit.task import Stream
from doit.reporter import ConsoleReporter, JsonReporter


class DAG:
    def __init__(self):
        self.task_list = []

        outstream = sys.stdout
        failure_verbosity = 0

        self.reporter = ConsoleReporter(outstream, {'failure_verbosity': failure_verbosity})
        self.dep_manager = Dependency(
            DbmDB, '.doit.db', checker_cls=MD5Checker, codec_cls=JSONCodec
        )

    def append(self, _task: Task):
        self.task_list.append(_task)

    def cli_main(self):
        from doit.doit_cmd import DoitMain
        DoitMain(self.task_list).run(sys.argv[1:])

    def run(self, targets=None):
        auto_delayed_regex = False
        verbosity = None
        force_verbosity = False

        stream = Stream(verbosity, force_verbosity)

        continue_ = False
        always = False

        runner_ = Runner(self.dep_manager, self.reporter, continue_, always, stream)

        control_ = TaskControl(
            self.task_list,
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

        runner_.run_all(control_.task_dispatcher())


__all__ = ['get_var', 'run', 'create_after', 'task_params', 'Globals', 'Task', 'DAG', 'PythonAction']


def get_initial_workdir():
    """working-directory from where the doit command was invoked on shell"""
    return loader.initial_workdir


assert load_ipython_extension  # silence pyflakes


