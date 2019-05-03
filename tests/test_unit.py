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

import hashlib
import json

import jinja2
import pytest
import toml

from stitches import Resource
from stitches import TaskStatus
from stitches import Platform
from stitches import load
from stitches import analyse


class PlatformTest(Platform):
    def __init__(self):
        self.value = 0
        self.files = {}
        self.region = {}

    def file_mtime(self, path):
        return self.value

    def file_exists(self, path):
        return self.files.get(path, True)

    def map_exists(self, type_, name):
        return True

    def region_hash(self):
        hasher = hashlib.md5()
        hasher.update(json.dumps(self.region, sort_keys=True).encode('ascii'))
        return hasher.hexdigest()


class PipelineTestState(object):

    def __init__(self):
        self.platform = PlatformTest()
        self.history = {}
        self.example_file = '''
        [[tasks]]
        task = "foo"
        inputs = ["file/foo.txt"]
        outputs = ["vector/baz"]

        [[tasks]]
        task = "bar"
        inputs = ["vector/baz"]
        outputs = ["vector/foo"]

        [[tasks]]
        task = "baz"
        inputs = ["vector/foo"]
        outputs = ["file/blah.txt"]
        '''


@pytest.fixture
def env():
    return PipelineTestState()


def test_resource_refs():
    res = Resource('file/foobar/baz.tif')
    assert (res.type, res.path) == (Resource.FILE, 'foobar/baz.tif')

    res = Resource('file//foobar/baz.tif')
    assert (res.type, res.path) == (Resource.FILE, '/foobar/baz.tif')

    res = Resource('vector/mypoint@mydb/myloc/maps')
    assert ((res.type, res.name, res.gisdbase, res.location, res.mapset) ==
            (Resource.VECTOR, 'mypoint', 'mydb', 'myloc', 'maps'))

    res = Resource('vector/mypoint')
    assert ((res.type, res.name, res.gisdbase, res.location, res.mapset) ==
            (Resource.VECTOR, 'mypoint', None, None, None))

    res = Resource('vector/mypoint@foo')
    assert ((res.type, res.name, res.gisdbase, res.location, res.mapset) ==
            (Resource.VECTOR, 'mypoint', None, None, 'foo'))


def test_pipeline_trivial_skip(env):
    '''A simple re-run of a pipeline should be skipped.'''
    jinja_env = jinja2.Environment(loader=jinja2.DictLoader({
        'mypipeline': env.example_file
    }))

    events = load(jinja_env, {'pipeline': 'mypipeline'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.RUN

    events = load(jinja_env, {'pipeline': 'mypipeline'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.SKIP


def test_pipeline_root_file_change(env):
    '''Test file modified invalidating the pipeline.'''
    jinja_env = jinja2.Environment(loader=jinja2.DictLoader({
        'mypipeline': env.example_file
    }))

    events = load(jinja_env, {'pipeline': 'mypipeline'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.RUN

    env.platform.value += 1

    events = load(jinja_env, {'pipeline': 'mypipeline'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.RUN

    events = load(jinja_env, {'pipeline': 'mypipeline'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.SKIP


def test_pipeline_root_arg_change(env):
    '''Test root task had a change of arguments.'''
    pfile = toml.loads(env.example_file)
    pfile['tasks'][0]['params'] = 1337

    jinja_env = jinja2.Environment(loader=jinja2.DictLoader({
        'mypipeline': env.example_file,
        'mypipeline2': toml.dumps(pfile)
    }))

    events = load(jinja_env, {'pipeline': 'mypipeline'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.RUN

    events = load(jinja_env, {'pipeline': 'mypipeline2'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.RUN

    events = load(jinja_env, {'pipeline': 'mypipeline2'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.SKIP


def test_pipeline_non_contributing_change(env):
    '''Test change of non-contributing keys.'''
    pfile = toml.loads(env.example_file)
    pfile['tasks'][0]['message'] = 'blah'

    jinja_env = jinja2.Environment(loader=jinja2.DictLoader({
        'mypipeline': env.example_file,
        'mypipeline2': toml.dumps(pfile)
    }))

    events = load(jinja_env, {'pipeline': 'mypipeline'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.RUN

    events = load(jinja_env, {'pipeline': 'mypipeline2'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.SKIP


def test_pipeline_always_task(env):
    '''Test always runnable task.'''
    pfile = toml.loads(env.example_file)
    for task in pfile['tasks']:
        task['always'] = True

    jinja_env = jinja2.Environment(loader=jinja2.DictLoader({
        'mypipeline': toml.dumps(pfile)
    }))

    events = load(jinja_env, {'pipeline': 'mypipeline'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.RUN

    events = load(jinja_env, {'pipeline': 'mypipeline'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.RUN


def test_pipeline_non_existing_output(env):
    '''Test always runnable task.'''
    jinja_env = jinja2.Environment(loader=jinja2.DictLoader({
        'mypipeline': env.example_file
    }))

    events = load(jinja_env, {'pipeline': 'mypipeline'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)
    for task in events:
        assert task.status == TaskStatus.RUN

    env.platform.files = {'blah.txt': False}

    events = load(jinja_env, {'pipeline': 'mypipeline'})
    next(events)  # Location event
    events = analyse(events, env.platform, env.history)

    expected = [TaskStatus.SKIP, TaskStatus.SKIP, TaskStatus.RUN]
    for (task, status) in zip(events, expected):
        assert task.status == status


def test_expand_pipeline():
    jinja_env = jinja2.Environment(loader=jinja2.DictLoader({
        'mypipeline': '''
        [[tasks]]
        pipeline = 'raster_pipeline'

        [[tasks]]
        task = 'bar'
        ''',
        'raster_pipeline': '''
        location = 'rasters'
        mapset = 'soils'

        [[tasks]]
        task = 'foo'
        '''
    }))

    root = {'pipeline': 'mypipeline'}
    stream = load(jinja_env, root, gisdbase='mydb', location='myloc')

    location_event = next(stream)
    assert location_event.gisdbase == 'mydb'
    assert location_event.location == 'myloc'
    assert location_event.mapset == 'PERMANENT'

    location_event = next(stream)
    assert location_event.gisdbase == 'mydb'
    assert location_event.location == 'rasters'
    assert location_event.mapset == 'soils'

    task_event = next(stream)
    assert task_event.ref == '0/0'
    assert task_event.task == 'foo'
    assert task_event.params == {}
    assert task_event.inputs == []
    assert task_event.outputs == []

    location_event = next(stream)
    assert location_event.gisdbase == 'mydb'
    assert location_event.location == 'myloc'
    assert location_event.mapset == 'PERMANENT'

    task_event = next(stream)
    assert task_event.ref == '1'
    assert task_event.task == 'bar'
    assert task_event.params == {}
    assert task_event.inputs == []
    assert task_event.outputs == []
