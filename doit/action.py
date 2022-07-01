import os
import subprocess
import inspect
from pathlib import PurePath

from loguru import logger


class BaseAction:
    """Base class for all actions"""

    def execute(self, ):
        raise NotImplementedError()


class CmdAction(BaseAction):
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


class PythonAction(BaseAction):
    """Python action. Execute a python callable.

    @ivar py_callable: (callable) Python callable
    @ivar args: (sequence)  Extra arguments to be passed to py_callable
    @ivar kwargs: (dict) Extra keyword arguments to be passed to py_callable
    @ivar task(Task): reference to task that contains this action
    """

    def __init__(self, py_callable, args=None, kwargs=None, task=None):
        self.py_callable = py_callable
        self.task = task
        self.result = None
        self.values = {}

        self.args = args or ()
        self.kwargs = kwargs or {}

        # check valid parameters
        if not hasattr(self.py_callable, '__call__'):
            msg = "%r PythonAction must be a 'callable' got %r."
            raise InvalidTask(msg % (self.task, self.py_callable))
        if inspect.isclass(self.py_callable):
            msg = "%r PythonAction can not be a class got %r."
            raise InvalidTask(msg % (self.task, self.py_callable))
        if inspect.isbuiltin(self.py_callable):
            msg = "%r PythonAction can not be a built-in got %r."
            raise InvalidTask(msg % (self.task, self.py_callable))
        if type(self.args) is not tuple and type(self.args) is not list:
            msg = "%r args must be a 'tuple' or a 'list'. got '%s'."
            raise InvalidTask(msg % (self.task, self.args))
        if type(self.kwargs) is not dict:
            msg = "%r kwargs must be a 'dict'. got '%s'"
            raise InvalidTask(msg % (self.task, self.kwargs))

    def _prepare_kwargs(self,):
        """
        Prepare keyword arguments (targets, dependencies, changed,
        cmd line options)
        Inspect python callable and add missing arguments:
        - that the callable expects
        - have not been passed (as a regular arg or as keyword arg)
        - are available internally through the task object
        """
        # Return just what was passed in task generator
        # dictionary if the task isn't available
        if not self.task:
            return self.kwargs

        func_sig = inspect.signature(self.py_callable)
        sig_params = func_sig.parameters.values()
        func_has_kwargs = any(p.kind == p.VAR_KEYWORD for p in sig_params)

        # use task meta information as extra_args
        meta_args = {
            'task': lambda: self.task,
            'targets': lambda: list(self.task.targets),
            'dependencies': lambda: list(self.task.file_dep),
            'changed': lambda: list(self.task.dep_changed),
        }

        # start with dict passed together on action definition
        kwargs = self.kwargs.copy()
        bound_args = func_sig.bind_partial(*self.args)

        # add meta_args
        for key in meta_args.keys():
            # check key is a positional parameter
            if key in func_sig.parameters:
                sig_param = func_sig.parameters[key]

                # it is forbidden to use default values for this arguments
                # because the user might be unaware of this magic.
                if sig_param.default != sig_param.empty:
                    msg = (f"Task {self.task.name}, action {self.py_callable.__name__}():"
                           f"The argument '{key}' is not allowed to have "
                           "a default value (reserved by doit)")
                    raise InvalidTask(msg)

                # if value not taken from position parameter
                if key not in bound_args.arguments:
                    kwargs[key] = meta_args[key]()

        # add tasks parameter options
        opt_args = dict(self.task.options)
        if self.task.pos_arg is not None:
            opt_args[self.task.pos_arg] = self.task.pos_arg_val

        for key in opt_args.keys():
            # check key is a positional parameter
            if key in func_sig.parameters:
                # if value not taken from position parameter
                if key not in bound_args.arguments:
                    kwargs[key] = opt_args[key]

            # if function has **kwargs include extra_arg on it
            elif func_has_kwargs and key not in kwargs:
                kwargs[key] = opt_args[key]
        return kwargs

    def execute(self, ):
        """Execute command action
        """

        kwargs = self._prepare_kwargs()

        logger.info(f"Executing {self.task.name}")
        # execute action / callable
        self.py_callable(*self.args, **kwargs)

    def __str__(self):
        # get object description excluding runtime memory address
        return "Python: %s" % str(self.py_callable)[1:].split(' at ')[0]

    def __repr__(self):
        return "<PythonAction: '%s'>" % (repr(self.py_callable))


def create_action(action, task_ref):
    """
    Create action using proper constructor based on the parameter type
    """
    if isinstance(action, BaseAction):
        action.task = task_ref
        return action

    if isinstance(action, str):
        return CmdAction(action, task_ref, shell=True)

    if isinstance(action, list):
        return CmdAction(action, task_ref, shell=False)

    if hasattr(action, '__call__'):
        return PythonAction(action, task=task_ref)

    msg = f"Task '{task_ref.name}': invalid '{action}' type. got: {type(action)}"
    raise Exception(msg)
