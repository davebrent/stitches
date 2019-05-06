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
    def __init__(self, task, pipeline=None, ref=None, params=None, inputs=None,
                 outputs=None, removes=None, message=None, always=None,
                 status=None, hash_=None):
        self.task = task
        self.params = params
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
            # pylint: disable=no-member
            lines = [colorful.format('{c.red}{}{c.reset}', line)
                     for line in event.traceback.splitlines()]
            if self.current_task:
                print(colorful.format('{c.bold}[{}]: {}{c.reset}',
                                      self.current_task.ref,
                                      self.current_task.description))
                lines = ['  {}'.format(line) for line in lines]
            for line in lines:
                print(line, file=sys.stderr)


class VerboseReporter(object):

    def __call__(self, event):
        # pylint: disable=no-member
        if isinstance(event, TaskStartEvent):
            print(colorful.format('{c.bold}[{}]: {}{c.reset}',
                                  event.ref,
                                  event.description))
        elif isinstance(event, TaskSkipEvent):
            print(colorful.format('  {c.orange}Skipped{c.reset}'))
        elif isinstance(event, TaskCompleteEvent):
            print(colorful.format('  {c.green}Completed{c.reset}'))
        elif isinstance(event, TaskFatalEvent):
            lines = [colorful.format('  {c.red}{}{c.reset}', line)
                     for line in event.traceback.splitlines()]
            for line in lines:
                print(line, file=sys.stderr)


class Resource(object):
    FILE = 'file'
    VECTOR = 'vector'
    RASTER = 'raster'

    def __init__(self, ref):
        self._ref = ref
        (type_, rest) = ref.split('/', 1)

        if type_ == Resource.FILE:
            self.type = type_
            self.path = rest

        elif type_ in (Resource.VECTOR, Resource.RASTER):
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
                raise Exception('Malformed GRASS name "{}"'.format(ref))
        else:
            raise Exception('Invalid resource type "{}"'.format(type_))

    def ref(self):
        return self._ref


def _object_checksum(obj):
    hasher = hashlib.md5()
    hasher.update(json.dumps(obj, sort_keys=True).encode('ascii'))
    return hasher.hexdigest()


class Platform(object):

    def file_mtime(self, path):
        return os.stat(path).st_mtime

    def file_exists(self, path):
        return os.path.exists(path)

    def map_exists(self, type_, name):
        from ._grass import gcore
        res = gcore.read_command(
            'g.list', type=type_, pattern=name).splitlines()
        if res and res[0].decode('utf-8') == name:
            return True
        return False

    def region_hash(self):
        from ._grass import gcore
        return _object_checksum(gcore.region())


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
    if test:
        assert true and false
    def wrapper(*args, **kwargs):
        if test:
            if test(*args, **kwargs):
                return true(*args, **kwargs)
            return false(*args, **kwargs)
        return result
    return wrapper


def _is_grass_map(_, resource):
    '''Returns true if the resource is a grass map.'''
    return resource.type != Resource.FILE


def _grass_map_exists(planner, resource):
    '''Returns true if the grass map exists.'''
    return planner.platform.map_exists(resource.type, resource.name)


def _creator_visible(planner, resource):
    '''Returns true if the creator of the map is visible in the pipeline.'''
    parent = planner.created.get(resource.ref())
    return not parent is None


def _creator_changed(planner, resource):
    '''Returns true if the creator of a map has changed.'''
    parent_status = planner.statuses[planner.created[resource.ref()]]
    return parent_status != TaskStatus.SKIP


def _is_file(_, resource):
    '''Returns true if the resource is a file.'''
    return resource.type == Resource.FILE


def _file_exists(planner, resource):
    '''Returns true if the file exists.'''
    return planner.platform.file_exists(resource.path)


def _file_has_previous(planner, resource):
    '''Returns true if the task has seen the file before.'''
    history = planner.history.get(planner.task.hash, {}).get('inputs', {})
    return resource.ref() in history


def _file_mtime_recent(planner, resource):
    '''Returns true if a file has been more recently modified.'''
    history = planner.history[planner.task.hash]
    previous = history['inputs'][resource.ref()]
    current = planner.platform.file_mtime(resource.path)
    if current > previous:
        return True
    return False


def _task_always(_, task):
    '''Return true if the task is marked as "always".'''
    return task.always


_OUTPUT_DECISION_TREE = decision(
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


_INPUT_DECISION_TREE = decision(
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


_TASK_DECISION_TREE = decision(
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
    status = _TASK_DECISION_TREE(planner, task)
    if status:
        return status

    # Look at the outputs
    non_existing = []
    for resource in task.outputs:
        status = _OUTPUT_DECISION_TREE(planner, resource)
        if status != OutputStatus.EXISTS:
            non_existing.append(resource)
    if non_existing:
        return TaskStatus.RUN

    # Look at the history
    if task.hash not in planner.history:
        return TaskStatus.RUN

    # Look at the current region
    region_hash = planner.platform.region_hash()
    if planner.history[task.hash]['region'] != region_hash:
        return TaskStatus.RUN

    # Look at the inputs
    failures = []
    unknowns = []
    result = TaskStatus.SKIP
    for resource in task.inputs:
        status = _INPUT_DECISION_TREE(planner, resource)
        if status == InputStatus.FAIL:
            failures.append(resource)
        elif status == InputStatus.UNKNOWN:
            unknowns.append(resource)
        elif status == InputStatus.CHANGE:
            result = TaskStatus.RUN
    if failures:
        return TaskStatus.FAIL
    if unknowns:
        return TaskStatus.RUN
    return result


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
            params = options.get('params', {})

            variables = params.get('vars', {})
            gisdbase_ = params.get('gisdbase', gisdbase)
            location_ = params.get('location', location)
            mapset_ = params.get('mapset', mapset)

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
            contributing = ['task', 'params', 'inputs', 'outputs', 'removes']
            hashable = {name: options.get(name) for name in contributing}
            hash_ = _object_checksum(hashable)

            inputs = [Resource(ref) for ref in options.get('inputs', [])]
            outputs = [Resource(ref) for ref in options.get('outputs', [])]
            removes = [Resource(ref) for ref in options.get('removes', [])]

            yield TaskEvent(options['task'],
                            pipeline=parent,
                            ref=ref,
                            hash_=hash_,
                            message=options.get('message', ''),
                            params=options.get('params', {}),
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
    completed = set()

    for event in stream:
        if not isinstance(event, TaskEvent):
            yield event
            continue

        task = event
        planner.task = task
        planner.task.status = _task_status(planner, task)
        planner.statuses[task.ref] = task.status

        region_hash = platform.region_hash()

        yield task

        completed.add(task.hash)

        # Advance planner state
        for resource in task.outputs:
            planner.created[resource.ref()] = task.ref
        for resource in task.removes:
            del planner.created[resource.ref()]

        # Update the history
        task_history = history.get(task.hash, {'inputs': {}})
        task_history['region'] = region_hash
        task_history['message'] = task.message
        for resource in task.inputs:
            if resource.type == Resource.FILE:
                task_history['inputs'][resource.ref()] = platform.file_mtime(
                    resource.path)
        history[task.hash] = task_history

    # Remove previously seen keys.
    keys = list(history.keys())
    for key in keys:
        if key not in completed:
            del history[key]


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
                function(**event.params)
            yield TaskCompleteEvent(event)
