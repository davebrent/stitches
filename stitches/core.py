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

import importlib
try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO
import json
import os
import sys
import traceback

import colorful
import toml
from grass.script import core as gcore
import wurlitzer


SKIP = 'skip'
RUN = 'run'
FAIL = 'fail'
FATAL = 'fatal'
SUCCESS = 'success'
UNKNOWN = 'unknown'


class Error(Exception):
    def __init__(self, message):
        super(Error, self).__init__()
        self.message = message

    def __str__(self):
        return self.message


class TaskStartEvent(object):
    def __init__(self, index, description):
        self.index = index
        self.description = description


class TaskCompleteEvent(object):
    def __init__(self, index):
        self.index = index


class TaskSkipEvent(object):
    def __init__(self, index):
        self.index = index


class TaskFatalEvent(object):
    def __init__(self, traceback):
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
            print('{c.bold}[{}]: {}{c.reset}'.format(
                event.index,
                event.description,
                c=colorful))
        elif isinstance(event, TaskSkipEvent):
            print('  {c.orange}Skipped{c.reset}'.format(c=colorful))
        elif isinstance(event, TaskCompleteEvent):
            print('  {c.green}Completed{c.reset}'.format(c=colorful))
        elif isinstance(event, TaskFatalEvent):
            lines = ['  {c.red}{}{c.reset}'.format(line, c=colorful)
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


class Resolver(object):

    def status(self, dependency):
        '''Return a status (ie primitive value) for a dependency.'''
        raise NotImplementedError()

    def compare(self, current, previous):
        '''Compare results returned by `status` for if the task should run.'''
        raise NotImplementedError()


class MTimeResolver(Resolver):
    '''Resolver based on last modified times.

    The last modified time is used by default, due to the usual size of
    geographic data sets. If the current time is more recent then the task may
    be resolved to run.
    '''

    def status(self, dependency):
        try:
            return os.stat(dependency.path).st_mtime
        except OSError:
            raise Exception('File does not exist')

    def compare(self, current, previous):
        if current > previous:
            return RUN
        return SKIP


class GrassResolver(Resolver):
    '''Grass map resolver.

    Without being able to compare grass datasets, the best the resolver can do
    in isolation, is, determine that the map currently exists and signal an
    unknown state.
    '''

    def status(self, dependency):
        res = gcore.read_command(
            'g.list',
            type=dependency.type,
            pattern=dependency.name).splitlines()
        if res and res[0].decode('utf-8') == dependency.name:
            return True
        raise Exception('Map {} does not exist'.format(dependency.name))

    def compare(self, current, previous):
        return UNKNOWN


class State(object):
    '''Retained state between each run.

    TODO: Try and store this is a sqlite table in the grass region.
    '''

    def __init__(self, dependencies=None):
        self.dependencies = dependencies or {}

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
            'dependencies': self.dependencies,
        })
        with open(path, 'w') as fp:
            fp.write(serialized)


class Context(object):

    def __init__(self, state, path, grassdata, jinja, resolvers=None,
                 reporter=None):
        self._path = path
        self._jinja = jinja
        self._grassdata = grassdata
        self.stdout = StringIO()
        self.stderr = StringIO()
        self.initial = True
        self.state = state
        if reporter is None:
            reporter = SilentReporter()
        self.reporter = reporter
        self.resolvers = resolvers or {
            'fs': MTimeResolver(),
            'vector': GrassResolver(),
            'raster': GrassResolver(),
        }

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

    def __call__(self):
        return self.task()


def planner(context, tasks, skip=None, force=None, only=None):
    '''Plan task execution, determine which tasks may be skipped.

    This function may only mutate the state of the `status` and `reason`
    attributes of each task. The skipping algorithm is as follows:

    - A tasks configuration is hashed and used as a key to a set of previously
      exectuted tasks (across all pipline runs)
    - If the hash is not present, then task is simply run.
    - If the tasks outputs do not yet exist, then the task is run.
    - If the task has grass inputs, check the tasks that create them, if they
      weren't skipped, then the task is run (modified times again maybe?)
    - If the task has file inputs, check the last seen modified time for this
      task, if they are different run the task
    - Otherwise, the task is skipped.
    '''
    created = {}
    def push_task(i, task):
        # Map of which task last created which dependency
        # 'type/foo@bar': <task_id>
        for dependency in task.outputs:
            created[dependency.fmt()] = i

    for (i, task) in enumerate(tasks):
        # This needs to be a string for uniform serializing and deserializing
        # to json or toml (as a key it will be coerced into a string, but
        # created as a number)
        task.hash = str(hash(json.dumps(task.options, sort_keys=True)))
        meta = context.state.dependencies.get(task.hash, None)
        if not meta:
            context.state.dependencies[task.hash] = dict(inputs={}, outputs={})

        # First test the only, force & skip now the task hash is set
        if force is not None:
            task.status = RUN
            task.reason = 'task forced to run'
            continue
        if only is not None:
            if '{}'.format(i) == only:
                task.status = RUN
                task.reason = 'task specified as "only" task'
            else:
                task.status = SKIP
                task.reason = 'task does not match "only"'
            continue
        if skip is not None:
            if '{}'.format(i) in skip:
                task.status = SKIP
                task.reason = 'task matched "skip"'
                continue

        if not meta:
            task.status = RUN
            task.reason = 'task options have been changed or unseen'
            push_task(i, task)
            continue

        exit_ = False
        for dependency in task.outputs:
            resolver = context.resolvers.get(dependency.type)
            try:
                status = resolver.status(dependency)
            except Exception:
                exit_ = True
                task.status = RUN
                task.reason = 'Task output "{}" does not exist'.format(
                    dependency.fmt())
                break
        if exit_:
            push_task(i, task)
            continue

        skips = 0
        for dependency in task.inputs:
            resolver = context.resolvers.get(dependency.type)
            exit_inner = False
            try:
                status = resolver.status(dependency)
            except Exception:
                exit_inner = True
                task.status = FAIL
                task.reason = 'task input "{}" does not exist'.format(
                    dependency.fmt())
                break
            if exit_inner:
                break

            # Hashes may collide and this specific task may not have actually
            # been seen before, so its input deps may not exist in `meta`
            depkey = dependency.fmt()
            previous = meta['inputs'].get(depkey, None)
            if previous is None:
                task.status = RUN
                task.reason = 'task input "{}" hasnt been seen'.format(
                    dependency.fmt())
                break

            # Resolvers can always determine if a task can be run, but not if
            # they can be safely skipped
            if resolver.compare(status, previous) == RUN:
                exit_ = True
                task.status = RUN
                task.reason = 'task input "{}" has been modified'.format(
                    dependency.fmt())
                break

            # File system dependencies can be safely skipped at this point
            # Grass maps are still an unknown
            if dependency.type == Dependency.FS:
                skips += 1
                task.status = SKIP
                push_task(i, task)
                continue

            parent = created.get(depkey, None)
            if parent is None:
                task.status = RUN
                task.reason = 'unable to determine status of "{}"'.format(
                    dependency.fmt())
                break

            parent = tasks[parent]
            if parent.status == SKIP:
                # The task that created this dependency was skipped, so we are
                # closer to skipping this task
                skips += 1

        if task.status == RUN:
            push_task(i, task)
            continue

        if skips != 0 and skips == len(task.inputs):
            task.status = SKIP
            task.reason = 'dependencies are all up to date'
        else:
            task.status = RUN
            task.reason = 'dependencies are not up to date or are unknown'

        push_task(i, task)


def update(context, task):
    '''When a task is successfully run, its meta data needs to be updated.'''
    # This needs to remove old dependencies from meta to keep the data fresh
    task.status = None
    task.reason = None
    meta = context.state.dependencies[task.hash]

    for dependency in task.outputs:
        depkey = dependency.fmt()
        resolver = context.resolvers.get(dependency.type)
        meta['outputs'][depkey] = resolver.status(dependency)

    for dependency in task.inputs:
        depkey = dependency.fmt()
        resolver = context.resolvers.get(dependency.type)
        meta['inputs'][depkey] = resolver.status(dependency)


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
    planner(context, tasks, force=force, skip=skip, only=only)

    for (i, task) in enumerate(tasks):
        message = task.options.get('message', '')
        context.reporter(TaskStartEvent(i, message))

        if task.status == SKIP:
            context.reporter(TaskSkipEvent(i))
            continue
        elif task.status == FAIL:
            raise Error(task.reason)
        elif task.status == RUN:
            # Exceptions are deliberately not caught here to avoid any
            # complications with nested pipelines & losing a useful stacktrace.
            # They are instead to propogate to the caller of the first pipeline
            with wurlitzer.pipes(stdout=context.stdout,
                                 stderr=context.stderr):
                task.function(context, task.options.get('args'))
            update(context, task)
            context.save()
            context.reporter(TaskCompleteEvent(i))
