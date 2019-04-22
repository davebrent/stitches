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

import tempfile

import pytest

from stitches import Context
from stitches import Dependency
from stitches import State
from stitches import TaskHandler
from stitches import TaskStatus
from stitches import prepass
from stitches import advance
from stitches import Platform

from . import dummy_task


class PlatformTest(Platform):
    def __init__(self):
        self.value = 0

    def file_mtime(self, path):
        return self.value

    def file_exists(self, path):
        return True

    def map_exists(self, type_, name):
        return True


class PipelineTestState(object):

    def __init__(self):
        self.context = Context(None, platform=PlatformTest())
        self.context.init(tempfile.mktemp())
        self.tasks = [
            TaskHandler({
                'name': 'foo',
                'args': 1,
                'inputs': ['fs/foo.txt'],
                'outputs': ['vector/baz'],
            }, dummy_task),
            TaskHandler({
                'name': 'bar',
                'args': 2,
                'inputs': ['vector/baz'],
                'outputs': ['vector/foo'],
            }, dummy_task),
            TaskHandler({
                'name': 'baz',
                'args': 3,
                'inputs': ['vector/foo'],
                'outputs': ['fs/blah.txt'],
            }, dummy_task),
        ]


@pytest.fixture
def env(request):
    environment = PipelineTestState()
    return environment


def test_dependency_definition():
    d = Dependency('fs/foobar/baz.tif')
    assert (d.type, d.path) == ('fs', 'foobar/baz.tif')

    d = Dependency('fs//foobar/baz.tif')
    assert (d.type, d.path) == ('fs', '/foobar/baz.tif')

    d = Dependency('vector/mypoint@mydb/myloc/maps')
    assert ((d.type, d.name, d.gisdbase, d.location, d.mapset) ==
            ('vector', 'mypoint', 'mydb', 'myloc', 'maps'))

    d = Dependency('vector/mypoint')
    assert ((d.type, d.name, d.gisdbase, d.location, d.mapset) ==
            ('vector', 'mypoint', None, None, None))

    d = Dependency('vector/mypoint@foo')
    assert ((d.type, d.name, d.gisdbase, d.location, d.mapset) ==
            ('vector', 'mypoint', None, None, 'foo'))


def test_pipeline_trivial_skip(env):
    '''A simple re-run of a pipeline should be skipped.'''
    prepass(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == TaskStatus.RUN
    for task in env.tasks:
        advance(env.context, task)
    prepass(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == TaskStatus.SKIP


def test_pipeline_root_file_change(env):
    '''Test file modified invalidating the pipeline.'''
    prepass(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == TaskStatus.RUN
    for task in env.tasks:
        advance(env.context, task)

    env.context.platform.value += 1
    prepass(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == TaskStatus.RUN

    for task in env.tasks:
        advance(env.context, task)
    prepass(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == TaskStatus.SKIP


def test_pipeline_root_arg_change(env):
    '''Test root task had a change of arguments.'''
    prepass(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == TaskStatus.RUN
    for task in env.tasks:
        advance(env.context, task)

    env.tasks[0].options['args'] = 1337
    prepass(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == TaskStatus.RUN

    for task in env.tasks:
        advance(env.context, task)
    prepass(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == TaskStatus.SKIP


def test_pipeline_non_contributing_change(env):
    '''Test change of non-contributing keys.'''
    prepass(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == TaskStatus.RUN
        advance(env.context, task)
    env.tasks[0].options['message'] = 'blah'
    prepass(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == TaskStatus.SKIP


def test_pipeline_always_task(env):
    '''Test always runnable task.'''
    for task in env.tasks:
        task.options['always'] = True
    prepass(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == TaskStatus.RUN
        advance(env.context, task)
    prepass(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == TaskStatus.RUN
