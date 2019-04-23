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
import hashlib
import importlib
import json
import os
import sys

import colorful
import wurlitzer
import toml
from grass.script import core as gcore


class Error(Exception):
    def __init__(self, message):
        super(Error, self).__init__()
        self.message = message

    def __str__(self):
        return self.message


class LocationEvent(object):
    def __init__(self, gisdbase=None, location=None, mapset=None):
        self.gisdbase = gisdbase
        self.location = location
        self.mapset = mapset


class TaskEvent(object):
    def __init__(self, task, pipeline=None, ref=None, args=None, inputs=None,
                 outputs=None, removes=None, message=None, always=None,
                 status=None, hash_=None):
        self.task = task
        self.args = args
        self.inputs = inputs
        self.outputs = outputs
        self.removes = removes
        self.message = message
        self.always = always
        # Improved error reporting
        self.pipeline = pipeline
        self.ref = ref
        # Calculated later
        self.status = status
        self.hash = hash_


class TaskStartEvent(object):
    def __init__(self, ref, description):
        self.ref = ref
        self.description = description


class TaskCompleteEvent(object):
    def __init__(self, task):
        self.task = task


class TaskSkipEvent(object):
    def __init__(self, task):
        self.task = task


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
                    self.current_task.ref,
                    self.current_task.description,
                    c=colorful))
                lines = ['  {}'.format(line) for line in lines]
            for line in lines:
                print(line, file=sys.stderr)


class VerboseReporter(object):

    def __call__(self, event):
        if isinstance(event, TaskStartEvent):
            print('{c.bold}[{}]: {}{c.reset}'.format(
                event.ref,
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

    def __init__(self, path, history=None):
        self.path = path
        self.history = collections.defaultdict(dict, **(history or {}))

    @classmethod
    def load(cls, path):
        try:
            with open(path, 'r') as fp:
                data = json.load(fp)
        except IOError:
            data = {}
        return cls(path, **data)

    def save(self):
        serialized = json.dumps({
            'history': self.history,
        }, indent=2)
        with open(self.path, 'w') as fp:
            fp.write(serialized)


class OutputStatus(object):
    EXISTS = 'exists'


class InputStatus(object):
    CHANGE = 'change'
    NOCHANGE = 'nochange'
    FAIL = 'fail'
    UNKNOWN = 'unknown'


class TaskStatus(object):
    RUN = 'run'
    SKIP = 'skip'
    FAIL = 'fail'


class StatusContext(object):
    '''Context that lives during input resolution.'''
    def __init__(self, platform, history, skip, force, only):
        self.platform = platform
        self.history = history
        self.created = {}
        self.statuses = {}
        self.force = force
        self.skip = skip
        self.only = only
        self.task = None


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
    return planner.platform.map_exists(dep.type, dep.name)


def _creator_visible(planner, dep):
    '''Returns true if the creator of the map is visible in the pipeline.'''
    parent = planner.created.get(dep.fmt())
    return False if parent is None else True


def _creator_changed(planner, dep):
    '''Returns true if the creator of a map has changed.'''
    parent_status = planner.statuses[planner.created[dep.fmt()]]
    return parent_status != TaskStatus.SKIP


def _is_file(planner, dep):
    '''Returns true if the dependency is a file.'''
    return dep.type == Dependency.FS


def _file_exists(planner, dep):
    '''Returns true if the file exists.'''
    return planner.platform.file_exists(dep.path)


def _file_has_previous(planner, dep):
    '''Returns true if the task has seen the file before.'''
    history = planner.history.get(planner.task.hash, {}).get('inputs', {})
    return dep.fmt() in history


def _file_mtime_recent(planner, dep):
    '''Returns true if a file has been more recently modified.'''
    history = planner.history[planner.task.hash]
    previous = history['inputs'][dep.fmt()]
    current = planner.platform.file_mtime(dep.path)
    if current > previous:
        return True
    return False


def _task_always(planner, task):
    '''Return true if the task is marked as "always".'''
    return task.always


_output_decision_tree = decision(
    test=_is_grass_map,
    true=decision(
        test=_grass_map_exists,
        true=decision(result=OutputStatus.EXISTS),
        false=decision(result=None),
    ),
    false=decision(
        test=_is_file,
        true=decision(
            test=_file_exists,
            true=decision(result=OutputStatus.EXISTS),
            false=decision(result=None),
        ),
        false=decision(result=None),
    )
)


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
            test=lambda p, t: t.ref == p.only,
            true=decision(result=TaskStatus.RUN),
            false=decision(result=TaskStatus.SKIP),
        ),
        false=decision(
            test=lambda p, _: p.skip is not None,
            true=decision(
                test=lambda p, t: t.ref in p.skip,
                true=decision(result=TaskStatus.SKIP),
                false=decision(
                    test=_task_always,
                    true=decision(result=TaskStatus.RUN),
                    false=decision(result=None)
                )
            ),
            false=decision(
                test=_task_always,
                true=decision(result=TaskStatus.RUN),
                false=decision(result=None)
            )
        )
    )
)


def _task_status(planner, task):
    '''Return a status for a task.'''
    status = _task_decision_tree(planner, task)
    if status:
        return status

    # Look at the outputs
    non_existing = []
    for dependency in task.outputs:
        status = _output_decision_tree(planner, dependency)
        if status != OutputStatus.EXISTS:
            non_existing.append(dependency)
    if non_existing:
        return TaskStatus.RUN

    # Look at the history
    if task.hash not in planner.history:
        return TaskStatus.RUN

    # Look at the inputs
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
        return TaskStatus.FAIL
    elif unknowns:
        return TaskStatus.RUN
    else:
        return result


def advance(platform, history, task):
    '''Advance state with the result a task.'''
    task_history = history.get(task.hash, {'inputs': {}})
    # Useful for debugging retained state
    task_history['message'] = task.message
    for dependency in task.inputs:
        if dependency.type == Dependency.FS:
            task_history['inputs'][dependency.fmt()] = platform.file_mtime(
                dependency.path)
    history[task.hash] = task_history


def reconcile(history, seen):
    '''Remove previously seen keys.'''
    keys = list(history.keys())
    for key in keys:
        if key not in seen:
            del history[key]


def load(jinja_env, options, gisdbase=None, location=None, mapset='PERMANENT'):
    '''Load all tasks from a pipeline.

    This expands a pipeline, and all of sub-pipelines and location changes into
    a normalized, flat series, of events.
    '''
    stack = [('config', (None, None, options))]
    locations = []

    while stack:
        tag, data = stack.pop()
        if tag == 'location':
            yield data
            continue

        (parent, ref, options) = data

        if 'pipeline' in options:
            name = options.get('pipeline')
            args = options.get('args', {})

            variables = args.get('vars', {})
            gisdbase_ = args.get('gisdbase', gisdbase)
            location_ = args.get('location', location)
            mapset_ = args.get('mapset', mapset)

            template = jinja_env.get_template(name)
            config = toml.loads(template.render(variables))

            location = LocationEvent(
                gisdbase=config.get('gisdbase', gisdbase_),
                location=config.get('location', location_),
                mapset=config.get('mapset', mapset_))
            yield location
            if locations:
                stack.append(('location', locations.pop()))
            locations.append(location)

            tasks = list(enumerate(list(config.get('tasks', []))))
            for (i, task) in reversed(tasks):
                tref = str(i) if ref is None else '{}/{}'.format(ref, i)
                stack.append(('config', (name, tref, task)))

        elif 'task' in options:
            contributing = ['task', 'args', 'inputs', 'outputs', 'removes']
            hashable = {name: options.get(name) for name in contributing}

            hasher = hashlib.md5()
            hasher.update(json.dumps(hashable, sort_keys=True).encode('ascii'))
            hash_ = hasher.hexdigest()

            inputs = [Dependency(d) for d in options.get('inputs', [])]
            outputs = [Dependency(d) for d in options.get('outputs', [])]
            removes = [Dependency(d) for d in options.get('removes', [])]

            yield TaskEvent(options['task'],
                            pipeline=parent,
                            ref=ref,
                            hash_=hash_,
                            message=options.get('message', ''),
                            args=options.get('args', {}),
                            always=options.get('always', False),
                            inputs=inputs,
                            outputs=outputs,
                            removes=removes,)


def analyse(stream, platform, history, force=None, skip=None, only=None):
    '''Analyse the stream of tasks to be run.

    Responsible for setting the status field of a task, determining if it
    should be run or not.
    '''
    planner = StatusContext(platform, history, skip, force, only)

    for event in stream:
        if not isinstance(event, TaskEvent):
            yield event
            continue

        planner.task = event
        planner.task.status = _task_status(planner, event)
        planner.statuses[planner.task.ref] = planner.task.status
        yield planner.task

        for dependency in planner.task.outputs:
            planner.created[dependency.fmt()] = event.ref
        for dependency in planner.task.removes:
            del planner.created[dependency.fmt()]


def _load_task(task):
    name = task.task
    try:
        return getattr(
            importlib.import_module('stitches.tasks'), name)
    except AttributeError:
        (module, klass) = name.split(':')
        try:
            return getattr(
                importlib.import_module(module), klass)
        except ImportError:
            raise Error('Task "{}" not found, in "{}" at "{}"'.format(
                task.task, task.pipeline, task.ref))


def execute(stream, stdout, stderr):
    for event in stream:
        if not isinstance(event, TaskEvent):
            continue
        yield TaskStartEvent(event.ref, event.message)
        if event.status == TaskStatus.SKIP:
            yield TaskSkipEvent(event)
            continue
        elif event.status == TaskStatus.FAIL:
            raise Error(event)
        elif event.status == TaskStatus.RUN:
            function = _load_task(event)
            with wurlitzer.pipes(stdout=stdout, stderr=stderr):
                function(event.args)
            yield TaskCompleteEvent(event)
