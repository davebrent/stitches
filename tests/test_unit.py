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

import pytest

from stitches import Context
from stitches import Dependency
from stitches import State
from stitches import TaskHandler
from stitches import Resolver
from stitches import RUN
from stitches import SKIP
from stitches import planner
from stitches import update

from . import dummy_task


class DummyResolver(Resolver):
    def __init__(self):
        self.value = 0

    def status(self, _):
        return self.value

    def compare(self, current, previous):
        if current > previous:
            return RUN
        return SKIP


class PipelineTestState(object):

    def __init__(self):
        self.state = State()
        self.context = Context(self.state, None, None, None, resolvers={
            'fs': DummyResolver(),
            'vector': DummyResolver(),
            'raster': DummyResolver(),
        })
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
    planner(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == RUN
    for task in env.tasks:
        update(env.context, task)
    planner(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == SKIP


def test_pipeline_root_file_change(env):
    '''Test file modified invalidating the pipeline.'''
    planner(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == RUN
    for task in env.tasks:
        update(env.context, task)

    env.context.resolvers['fs'].value += 1
    planner(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == RUN

    for task in env.tasks:
        update(env.context, task)
    planner(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == SKIP


def test_pipeline_root_arg_change(env):
    '''Test root task had a change of arguments.'''
    planner(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == RUN
    for task in env.tasks:
        update(env.context, task)

    env.tasks[0].options['args'] = 1337
    planner(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == RUN

    for task in env.tasks:
        update(env.context, task)
    planner(env.context, env.tasks)
    for task in env.tasks:
        assert task.status == SKIP
