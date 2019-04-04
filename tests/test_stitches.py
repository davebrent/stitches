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

import os
import shutil
import subprocess
import tempfile

from grass_session import Session
from grass.script import core as gcore

import pytest


class Environment(object):

    def __init__(self):
        self.root = tempfile.mkdtemp(
            prefix='stitches_',
            dir=os.path.dirname(__file__))
        self.grassdata = os.path.join(self.root, 'grassdata')

    def close(self):
        shutil.rmtree(self.root, ignore_errors=False)

    def run(self, opts, config):
        fopts = dict(mode='w', dir=self.root, prefix='config_', suffix='.toml')
        with tempfile.NamedTemporaryFile(**fopts) as fp:
            fp.write(config)
            fp.flush()
            returncode = subprocess.call(['stitches'] + opts + [fp.name])
        return returncode


@pytest.fixture
def env(request):
    environment = Environment()
    request.addfinalizer(lambda: environment.close())
    return environment


def test_create_missing_location(env):
    '''The program should fail if the location is not specified.'''
    returncode = env.run(['--log', os.devnull], '''
    [pipeline]
    ''')
    assert returncode == 1


def test_create_default_grassdb(env):
    '''A pipeline should use the PERMANENT mapset if not specified.'''
    returncode = env.run([], '''
    [pipeline]
    location = 'foobar'
    ''')
    assert returncode == 0
    assert os.path.isdir(os.path.join(env.grassdata))
    assert os.path.isdir(os.path.join(env.grassdata, 'foobar'))
    assert os.path.isdir(os.path.join(env.grassdata, 'foobar', 'PERMANENT'))


@pytest.mark.skip(reason='should this case be supported?')
def test_create_named_mapset(env):
    '''A pipeline should use the specified mapset.'''
    returncode = env.run(['--log', os.devnull], '''
    [pipeline]
    location = 'foobar'
    mapset = 'blah'
    ''')
    assert returncode == 0
    assert os.path.isdir(os.path.join(env.grassdata, 'foobar', 'blah'))


def test_create_variables(env):
    '''Passing variables on the command line.'''
    returncode = env.run(['--vars', 'myvar=baz'], '''
    [pipeline]
    location = '{{ myvar }}'
    ''')
    assert returncode == 0
    assert os.path.isdir(os.path.join(env.grassdata, 'baz'))


def test_tasks_grass_import(env):
    '''Importing with the grass task.'''
    returncode = env.run([], '''
    [pipeline]
    location = 'foobar'

    [[tasks]]
    task = 'grass'
    args = {module='g.proj', c=true, proj4='+proj=utm +zone=33 +datum=WGS84'}

    [[tasks]]
    task = 'grass'
    args = {module='v.import', input='tests/point.geojson', output='mypoint'}
    ''')
    assert returncode == 0
    with Session(gisdb=env.grassdata, location='foobar'):
        maps = gcore.read_command(
            'g.list', type='vector', pattern='mypoint').splitlines()
        assert maps[0].decode('utf-8') == 'mypoint'


def test_tasks_python_func(env):
    '''Arbitrary python tasks.'''
    returncode = env.run([], '''
    [pipeline]
    location = 'foobar'

    [[tasks]]
    task = 'tests:dummy_task'
    ''')
    assert returncode == 0


def test_tasks_script_simple(env):
    '''Arbitrary script tasks.'''
    returncode = env.run([], '''
    [pipeline]
    location = 'foobar'

    [[tasks]]
    task = 'script'
    args = ['tests/myscript', 'bar']
    ''')
    assert returncode == 0


def test_tasks_composite_pipeline(env):
    '''Composing pipelines.'''
    other = '''
    [[tasks]]
    task = 'grass'
    args = {module='v.import', input='{{ in }}', output='{{ out }}'}
    '''

    config = '''
    [pipeline]
    location = 'foobar'

    [[tasks]]
    task = 'grass'
    args = {module='g.proj', c=true, proj4='+proj=utm +zone=33 +datum=WGS84'}

    [[tasks]]
    task = 'pipeline'
    args = {name='{{ other }}', vars={in='tests/point.geojson', out='mypoint'}}
    '''

    fopts = dict(mode='w', dir=env.root, prefix='config_', suffix='.toml')
    with tempfile.NamedTemporaryFile(**fopts) as fp:
        fp.write(other)
        fp.flush()
        returncode = env.run([
            '--vars',
            'other={}'.format(os.path.basename(fp.name))
        ], config)
    assert returncode == 0
    with Session(gisdb=env.grassdata, location='foobar'):
        maps = gcore.read_command(
            'g.list', type='vector', pattern='mypoint').splitlines()
        assert maps[0].decode('utf-8') == 'mypoint'
