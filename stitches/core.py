# This file is part of Stitches.
#
# Stitches is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Stitches is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Stitches. If not, see <https://www.gnu.org/licenses/>.

from __future__ import print_function

import collections
import importlib
import json
try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO
import os
import sys

import colorful
from grass.script import core as gcore
import toml
import wurlitzer


class Error(Exception):
    def __init__(self, message):
        super(Error, self).__init__()
        self.message = message

    def __str__(self):
        return self.message


class TaskStartEvent(object):
    def __init__(self, stack, index, description):
        self.stack = stack
        self.index = index
        self.description = description


class TaskCompleteEvent(object):
    def __init__(self, stack, index):
        self.stack = stack
        self.index = index


class TaskSkipEvent(object):
    def __init__(self, stack, index):
        self.stack = stack
        self.index = index


class TaskFatalEvent(object):
    def __init__(self, stack, traceback):
        self.stack = stack
        self.traceback = traceback


class SilentReporter(object):

    def __init__(self):
        self.current_task = None

    def __call__(self, event):
        if isinstance(event, TaskStartEvent):
            self.current_task = event
        elif isinstance(event, TaskCompleteEvent):
            self.current_task = None
        elif isinstance(event, TaskFatalEvent):
            lines = ['{c.red}{}{c.reset}'.format(line, c=colorful)
                     for line in event.traceback.splitlines()]
            if self.current_task:
                print('{c.bold}[{}]: {}{c.reset}'.format(
                    self.current_task.index,
                    self.current_task.description,
                    c=colorful))
                lines = ['  {}'.format(line) for line in lines]
            for line in lines:
                print(line, file=sys.stderr)


class VerboseReporter(object):

    def __call__(self, event):
        if isinstance(event, TaskStartEvent):
            indent = ' ' * (len(event.stack) * 2)
            print('{}{c.bold}[{}]: {}{c.reset}'.format(
                indent,
                event.index,
                event.description,
                c=colorful))
        elif isinstance(event, TaskSkipEvent):
            indent = ' ' * (len(event.stack) * 2)
            print('{}  {c.orange}Skipped{c.reset}'.format(indent, c=colorful))
        elif isinstance(event, TaskCompleteEvent):
            indent = ' ' * (len(event.stack) * 2)
            print('{}  {c.green}Completed{c.reset}'.format(indent, c=colorful))
        elif isinstance(event, TaskFatalEvent):
            indent = ' ' * (len(event.stack) * 2)
            lines = ['{}  {c.red}{}{c.reset}'.format(indent, line, c=colorful)
                     for line in event.traceback.splitlines()]
            for line in lines:
                print(line, file=sys.stderr)


class Dependency(object):
    FS = 'fs'
    VECTOR = 'vector'
    RASTER = 'raster'

    def __init__(self, spec):
        self._spec = spec
        (type_, rest) = spec.split('/', 1)

        if type_ == Dependency.FS:
            self.type = type_
            self.path = rest

        elif type_ == Dependency.VECTOR or type_ == Dependency.RASTER:
            components = rest.split('@', 1)
            self.type = type_
            self.name = components[0]

            if len(components) == 1:
                self.gisdbase = None
                self.location = None
                self.mapset = None

            elif len(components) == 2:
                rest = reversed(components[1].split('/'))
                self.mapset = next(rest)
                self.location = next(rest, None)
                self.gisdbase = next(rest, None)

            else:
                raise Exception('Malformed GRASS name "{}"'.format(spec))
        else:
            raise Exception('Invalid dependency type "{}"'.format(type_))

    def fmt(self):
        return self._spec


class Platform(object):

    def file_mtime(self, path):
        return os.stat(path).st_mtime

    def file_exists(self, path):
        return os.path.exists(path)

    def map_exists(self, type_, name):
        res = gcore.read_command(
            'g.list', type=type_, pattern=name).splitlines()
        if res and res[0].decode('utf-8') == name:
            return True
        return False


class State(object):
    '''Retained state between each run.

    TODO: Try and store this is a sqlite table in the grass region.
    '''

    def __init__(self, history=None):
        self.history = collections.defaultdict(dict, **(history or {}))

    @classmethod
    def load(cls, path):
        try:
            with open(path, 'r') as fp:
                data = toml.load(fp)
        except IOError:
            data = {}
        return cls(**data)

    def save(self, path):
        serialized = toml.dumps({
            'history': self.history,
        })
        with open(path, 'w') as fp:
            fp.write(serialized)


class Context(object):

    def __init__(self, jinja, gisdbase=None, platform=None, reporter=None):
        self._path = None
        self._jinja = jinja
        self.gisdbase = gisdbase
        self.stdout = StringIO()
        self.stderr = StringIO()
        self.initial = True
        self.state = None
        self.stack = []
        self.reporter = reporter if reporter else SilentReporter()
        self.platform = platform if platform else Platform()

    def init(self, path):
        self._path = path
        self.state = State.load(self._path)

    def save(self):
        self.state.save(self._path)


class TaskHandler(object):
    '''Wrapper around a stitches Task.

    This class exists to store extra book keeping information about the
    execution of a task and to not clutter the api of a users task.
    '''

    def __init__(self, options, function):
        self.options = options
        self.function = function
        self.hash = None
        self.status = None
        self.reason = None
        self.inputs = [Dependency(d) for d in options.get('inputs', [])]
        self.outputs = [Dependency(d) for d in options.get('outputs', [])]


class InputStatus(object):
    CHANGE = 'change'
    NOCHANGE = 'nochange'
    FAIL = 'fail'
    UNKNOWN = 'unknown'


class TaskStatus(object):
    RUN = 'run'
    SKIP = 'skip'
    FAIL = 'fail'


class PlannerContext(object):
    '''Context that lives during input resolution.'''
    def __init__(self, context, tasks, skip, force, only):
        self.context = context
        self.tasks = tasks
        self.force = force
        self.skip = skip
        self.only = only
        self.created = {}
        self.task = None
        self.index = None

    def __iter__(self):
        for (i, task) in enumerate(self.tasks):
            self.task = task
            self.index = i
            yield task
            for dependency in task.outputs:
                self.created[dependency.fmt()] = i


def decision(test=None, true=None, false=None, result=None):
    '''A function to build up a decision tree.'''
    if test is not None:
        assert true and false
    def wrapper(*args, **kwargs):
        if test is not None:
            if test(*args, **kwargs):
                return true(*args, **kwargs)
            return false(*args, **kwargs)
        else:
            return result
    return wrapper


def _is_grass_map(planner, dep):
    '''Returns true if the dependency is a grass map.'''
    return dep.type == Dependency.VECTOR or dep.type == Dependency.RASTER


def _grass_map_exists(planner, dep):
    '''Returns true if the grass map exists.'''
    return planner.context.platform.map_exists(dep.type, dep.name)


def _creator_visible(planner, dep):
    '''Returns true if the creator of the map is visible in the pipeline.'''
    parent = planner.created.get(dep.fmt())
    return False if parent is None else True


def _creator_changed(planner, dep):
    '''Returns true if the creator of a map has changed.'''
    parent = planner.tasks[planner.created[dep.fmt()]]
    return parent.status != TaskStatus.SKIP


def _is_file(planner, dep):
    '''Returns true if the dependency is a file.'''
    return dep.type == Dependency.FS


def _file_exists(planner, dep):
    '''Returns true if the file exists.'''
    return planner.context.platform.file_exists(dep.path)


def _file_has_previous(planner, dep):
    '''Returns true if the task has seen the file before.'''
    history = planner.context.state.history[planner.task.hash]
    return dep.fmt() in history


def _file_mtime_recent(planner, dep):
    '''Returns true if a file has been more recently modified.'''
    history = planner.context.state.history[planner.task.hash]
    # Offset was picked, using trial and error from an actual case of where the
    # floating point comparison caused a problem
    previous = history[dep.fmt()] + 0.000001
    current = planner.context.platform.file_mtime(dep.path)
    if current > previous:
        return True
    return False


_input_decision_tree = decision(
    test=_is_grass_map,
    true=decision(
        test=_grass_map_exists,
        true=decision(
            test=_creator_visible,
            true=decision(
                test=_creator_changed,
                true=decision(result=InputStatus.CHANGE),
                false=decision(result=InputStatus.NOCHANGE),
            ),
            false=decision(result=InputStatus.UNKNOWN),
        ),
        false=decision(result=InputStatus.FAIL)
    ),
    false=decision(
        test=_is_file,
        true=decision(
            test=_file_exists,
            true=decision(
                test=_file_has_previous,
                true=decision(
                    test=_file_mtime_recent,
                    true=decision(result=InputStatus.CHANGE),
                    false=decision(result=InputStatus.NOCHANGE),
                ),
                false=decision(result=InputStatus.CHANGE)
            ),
            false=decision(result=InputStatus.FAIL)
        ),
        false=decision(result=InputStatus.FAIL)
    )
)


_task_decision_tree = decision(
    test=lambda p, _: p.force,
    true=decision(result=TaskStatus.RUN),
    false=decision(
        test=lambda p, _: p.only is not None,
        true=decision(
            test=lambda p, _: '{}'.format(p.index) == p.only,
            true=decision(result=TaskStatus.RUN),
            false=decision(result=TaskStatus.SKIP),
        ),
        false=decision(
            test=lambda p, _: p.skip is not None,
            true=decision(
                test=lambda p, _: '{}'.format(p.index) in p.skip,
                true=decision(result=TaskStatus.SKIP),
                false=decision(result=None)
            ),
            false=decision(result=None)
        )
    )
)


def prepass(context, tasks, skip=None, force=None, only=None):
    '''Setup task execution, sets task status.'''
    # Assign each task an id and create an entry in the history
    hashids = set()
    for task in tasks:
        task.hash = str(hash(json.dumps(task.options, sort_keys=True)))
        hashids.add(task.hash)

    # Filter out non-existant tasks
    keys = list(context.state.history.keys())
    for hashid in keys:
        if hashid not in hashids:
            del context.state.history[hashid]

    # Create a status for each task
    planner = PlannerContext(context, tasks, skip, force, only)
    for task in planner:
        task.status = _task_decision_tree(planner, task)
        if task.status:
            continue

        if task.hash not in context.state.history:
            task.status = TaskStatus.RUN
            task.reason = 'New task'
            continue

        failures = []
        unknowns = []
        result = TaskStatus.SKIP

        for dependency in task.inputs:
            status = _input_decision_tree(planner, dependency)
            if status == InputStatus.FAIL:
                failures.append(dependency)
            elif status == InputStatus.UNKNOWN:
                unknowns.append(dependency)
            elif status == InputStatus.CHANGE:
                result = TaskStatus.RUN

        if failures:
            task.status = TaskStatus.FAIL
            task.reason = 'Failed to resolve dependencies'
        elif unknowns:
            task.status = TaskStatus.RUN
            task.reason = 'Unknown sources for dependencies'
        else:
            task.status = result


def advance(context, task):
    '''Advance state with the result a task.'''
    history = context.state.history[task.hash]
    for dependency in task.inputs:
        if dependency.type == Dependency.FS:
            history[dependency.fmt()] = context.platform.file_mtime(
                dependency.path)


def execute(context, config, force=False, skip=None, only=None):
    # Functions are imported early, to fail early, for trivial issues, such as
    # syntax errors, import errors etc in user code, rather than halfway
    # through a pipeline
    tasks = []
    for (i, options) in enumerate(config.get('tasks', [])):
        name = options.get('task')
        if not name:
            raise Error('Task [{}] missing [task]'.format(i))
        try:
            function = getattr(
                importlib.import_module('stitches.tasks'), name)
        except AttributeError:
            (module, klass) = name.split(':')
            try:
                function = getattr(
                    importlib.import_module(module), klass)
            except ImportError:
                raise Error('Task [{}] not found'.format(name))
        tasks.append(TaskHandler(options, function))

    # Plan the task execution, work out which tasks may be skipped etc.
    prepass(context, tasks, force=force, skip=skip, only=only)

    for (i, task) in enumerate(tasks):
        message = task.options.get('message', '')
        context.reporter(TaskStartEvent(list(context.stack), i, message))

        if task.status == TaskStatus.SKIP:
            context.reporter(TaskSkipEvent(list(context.stack), i))
            continue
        elif task.status == TaskStatus.FAIL:
            raise Error(task.reason)
        elif task.status == TaskStatus.RUN:
            context.stack.append(i)
            logged = getattr(task.function, 'logged', True)
            # Exceptions are deliberately not caught here to avoid any
            # complications with nested pipelines & losing a useful stacktrace.
            # They are instead to propogate to the caller of the first pipeline
            if logged:
                with wurlitzer.pipes(stdout=context.stdout,
                                     stderr=context.stderr):
                    task.function(context, task.options.get('args'))
            else:
                task.function(context, task.options.get('args'))
            advance(context, task)
            context.save()
            context.stack.pop()
            context.reporter(TaskCompleteEvent(list(context.stack), i))
