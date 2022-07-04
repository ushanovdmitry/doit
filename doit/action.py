import os
import subprocess
import inspect
from abc import ABC
from copy import copy
from itertools import chain
from pathlib import PurePath
from typing import List, Any

from loguru import logger


class AbstractGraphNode(ABC):
    pass


class CanRepresentGraphNode(ABC):
    def as_graph_node(self) -> AbstractGraphNode:
        raise NotImplementedError()


class AbstractDependency(CanRepresentGraphNode, ABC):
    def is_up_to_date(self, backend):
        raise NotImplementedError()

    def value(self, backend) -> Any:
        raise NotImplementedError()


class AbstractTarget(CanRepresentGraphNode, ABC):
    def exists(self, backend):
        raise NotImplementedError()

    def value(self, backend) -> Any:
        raise NotImplementedError()

    def store(self, backend):
        raise NotImplementedError()


class AbstractAction(ABC):
    """Base class for all actions"""

    def execute(self, backend):
        raise NotImplementedError()

    def get_all_dependencies(self) -> List[AbstractDependency]:
        raise NotImplementedError()

    def get_all_targets(self) -> List[AbstractTarget]:
        raise NotImplementedError()


class CmdAction(AbstractAction):
    """
    Command line action. Spawns a new process.

    @ivar action(str,list): subprocess command string or string list,
         see subprocess.Popen first argument.
         Strings may contain python mappings with the keys: dependencies,
         changed and targets. ie. "zip %(targets)s %(changed)s"
    @ivar task(Task): reference to task that contains this action
    @ivar shell: use shell to execute command
                 see subprocess.Popen `shell` attribute
    @ivar encoding (str): encoding of the process output
    @ivar decode_error (str): value for decode() `errors` param
                              while decoding process output
    @ivar pkwargs: Popen arguments except 'stdout' and 'stderr'
    """

    STRING_FORMAT = 'old'

    def __init__(self, action, task=None, shell=True,
                 encoding='utf-8', decode_error='replace', buffering=0,
                 **pkwargs):

        self.action = action
        self.task = task
        self.result = None
        self.values = {}
        self.shell = shell
        self.encoding = encoding
        self.decode_error = decode_error
        self.pkwargs = pkwargs
        self.buffering = buffering

    def execute(self):
        """
        Execute command action
        """
        action = self.expand_action()

        # set environ to change output buffering
        subprocess_pkwargs = self.pkwargs.copy()
        env = None
        if 'env' in subprocess_pkwargs:
            env = subprocess_pkwargs['env']
            del subprocess_pkwargs['env']
        if self.buffering:
            if not env:
                env = os.environ.copy()
            env['PYTHONUNBUFFERED'] = '1'

        # spawn task process
        process = subprocess.Popen(
            action,
            shell=self.shell,
            env=env,
            **subprocess_pkwargs)

        logger.info(f"Executing {self.task.name}")

        # make sure process really terminated
        process.wait()

        # task failure
        if process.returncode != 0:
            raise Exception(f"Command failed: '{action}' returned {process.returncode}")

    def expand_action(self):
        """Expand action using task meta information if action is a string.
        Convert `Path` elements to `str` if action is a list.
        @returns: string -> expanded string if action is a string
                  list - string -> expanded list of command elements
        """
        if not self.task:
            return self.action

        # can't expand keywords if action is a list of strings
        if isinstance(self.action, list):
            action = []
            for element in self.action:
                if isinstance(element, str):
                    action.append(element)
                elif isinstance(element, PurePath):
                    action.append(str(element))
                else:
                    msg = f"{self.task.name}. CmdAction element must be a str" \
                          f" or Path from pathlib. Got '{element!r}' ({type(element)})"
                    raise Exception(msg)

            return action

        subs_dict = {
            'targets': " ".join(self.task.targets),
            'dependencies': " ".join(self.task.file_dep),
        }

        # dep_changed is set on get_status()
        # Some commands (like `clean` also uses expand_args but do not
        # uses get_status, so `changed` is not available.
        if self.task.dep_changed is not None:
            subs_dict['changed'] = " ".join(self.task.dep_changed)

        # task option parameters
        subs_dict.update(self.task.options)
        # convert positional parameters from list space-separated string
        if self.task.pos_arg:
            if self.task.pos_arg_val:
                pos_val = ' '.join(self.task.pos_arg_val)
            else:
                pos_val = ''
            subs_dict[self.task.pos_arg] = pos_val

        if self.STRING_FORMAT == 'old':
            return self.action % subs_dict
        elif self.STRING_FORMAT == 'new':
            return self.action.format(**subs_dict)
        else:
            assert self.STRING_FORMAT == 'both'
            return self.action.format(**subs_dict) % subs_dict

    def __str__(self):
        return "Cmd: %s" % self.action

    def __repr__(self):
        return "<CmdAction: '%s'>" % str(self.action)


class PythonAction(AbstractAction):
    """Python action. Execute a python callable.

    @ivar py_callable: (callable) Python callable
    @ivar args: (sequence)  Extra arguments to be passed to py_callable
    @ivar kwargs: (dict) Extra keyword arguments to be passed to py_callable
    """

    def __init__(self, py_callable, args=None, kwargs=None):
        self.py_callable = py_callable

        self.args = args or ()
        self.kwargs = kwargs or {}

        # check valid parameters
        if not hasattr(self.py_callable, '__call__'):
            msg = "PythonAction must be a 'callable' got %r."
            raise Exception(msg % self.py_callable)
        if not isinstance(self.args, (tuple, list)):
            msg = "%s args must be a 'tuple' or a 'list'. got '%s'."
            raise Exception(msg % (self.py_callable, self.args))
        if not isinstance(self.kwargs, dict):
            msg = "%s kwargs must be a 'dict'. got '%s'"
            raise Exception(msg % (self.py_callable, self.kwargs))

    def execute(self, backend):
        """
        Execute command action
        """

        args = list(self.args)
        kwargs = self.kwargs.copy()

        for i, a in enumerate(args):
            if isinstance(a, (AbstractTarget, AbstractDependency)):
                args[i] = a.value(backend)

        for k, v in kwargs.items():
            if isinstance(v, (AbstractTarget, AbstractDependency)):
                kwargs[k] = v.value(backend)

        self.py_callable(*args, **kwargs)

    def __repr__(self):
        return "<PythonAction: '%s'>" % (repr(self.py_callable))

    def get_all_dependencies(self) -> List[AbstractDependency]:
        return [
            _ for _ in chain(self.args, self.kwargs.values())
            if isinstance(_, AbstractDependency)
        ]

    def get_all_targets(self) -> List[AbstractTarget]:
        return [
            _ for _ in chain(self.args, self.kwargs.values())
            if isinstance(_, AbstractTarget)
        ]
