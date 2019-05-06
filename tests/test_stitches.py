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

import json
import os
import shutil
import subprocess
import tempfile

import pytest

from stitches import session


class Environment(object):

    def __init__(self):
        self.root = tempfile.mkdtemp(
            prefix='stitches_',
            dir=os.path.dirname(__file__))
        self.gisdbase = os.path.join(self.root, 'grassdata')

    def close(self):
        shutil.rmtree(self.root, ignore_errors=False)

    def run(self, opts, config):
        fopts = dict(mode='w', dir=self.root, prefix='config_', suffix='.toml')
        with tempfile.NamedTemporaryFile(**fopts) as fp:
            fp.write(config)
            fp.flush()
            cmd = ['stitches', '--gisdbase', self.gisdbase] + opts + [fp.name]
            proc = subprocess.Popen(cmd,
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            (out, err) = proc.communicate()
            returncode = proc.returncode
        return (returncode, out.decode('utf-8'), err.decode('utf-8'))


@pytest.fixture
def env(request):
    environment = Environment()
    request.addfinalizer(environment.close)
    return environment


def test_create_missing_location(env):
    '''The program should fail if the location is not specified.'''
    returncode, _, _ = env.run(['--log', os.devnull], '''
    ''')
    assert returncode == 1


def test_create_default_grassdb(env):
    '''A pipeline should use the PERMANENT mapset if not specified.'''
    returncode, _, _ = env.run([], '''
    location = 'foobar'
    ''')
    assert returncode == 0
    assert os.path.isdir(os.path.join(env.gisdbase))
    assert os.path.isdir(os.path.join(env.gisdbase, 'foobar'))
    assert os.path.isdir(os.path.join(env.gisdbase, 'foobar', 'PERMANENT'))


def test_create_named_mapset(env):
    '''A pipeline should use the specified mapset.'''
    returncode, _, _ = env.run(['--log', os.devnull], '''
    location = 'foobar'
    mapset = 'blah'
    ''')
    assert returncode == 0
    assert os.path.isdir(os.path.join(env.gisdbase, 'foobar', 'blah'))


def test_create_variables(env):
    '''Passing variables on the command line.'''
    returncode, _, _ = env.run(['--vars', 'myvar=baz'], '''
    location = '{{ myvar }}'
    ''')
    assert returncode == 0
    assert os.path.isdir(os.path.join(env.gisdbase, 'baz'))


def test_tasks_grass_import(env):
    '''Importing with the grass task.'''
    returncode, _, _ = env.run([], '''
    location = 'foobar'

    [[tasks]]
    task = 'grass'
    params = {module='g.proj', c=true, proj4='+proj=utm +zone=33 +datum=WGS84'}

    [[tasks]]
    task = 'grass'
    params = {module='v.import', input='tests/point.geojson', output='mypoint'}
    ''')
    assert returncode == 0
    with session(env.gisdbase, 'foobar', mapset='PERMANENT'):
        from stitches._grass import gcore
        maps = gcore.read_command(
            'g.list', type='vector', pattern='mypoint').splitlines()
        assert maps[0].decode('utf-8') == 'mypoint'


def test_tasks_python_func(env):
    '''Arbitrary python tasks.'''
    returncode, _, _ = env.run([], '''
    location = 'foobar'

    [[tasks]]
    task = 'tests:dummy_task'
    ''')
    assert returncode == 0


def test_tasks_script_simple(env):
    '''Arbitrary script tasks.'''
    returncode, _, _ = env.run([], '''
    location = 'foobar'

    [[tasks]]
    task = 'script'
    params = {cmd=['tests/myscript', 'bar']}
    ''')
    assert returncode == 0


def test_tasks_composite_pipeline(env):
    '''Composing pipelines.'''
    other = '''
    [[tasks]]
    task = 'grass'
    params = {module='v.import', input='{{ in }}', output='{{ out }}'}
    '''

    config = '''
    location = 'foobar'

    [[tasks]]
    task = 'grass'
    params = {module='g.proj', c=true, proj4='+proj=utm +zone=33 +datum=WGS84'}

    [[tasks]]
    pipeline = '{{ other }}'
    params = {vars={in='tests/point.geojson', out='mypoint'}}
    '''

    fopts = dict(mode='w', dir=env.root, prefix='config_', suffix='.toml')
    with tempfile.NamedTemporaryFile(**fopts) as fp:
        fp.write(other)
        fp.flush()
        returncode, _, _ = env.run([
            '--vars',
            'other={}'.format(os.path.basename(fp.name))
        ], config)
    assert returncode == 0
    with session(env.gisdbase, 'foobar'):
        from stitches._grass import gcore
        maps = gcore.read_command(
            'g.list', type='vector', pattern='mypoint').splitlines()
        assert maps[0].decode('utf-8') == 'mypoint'


def test_tasks_composite_pipeline_output(env):
    '''Composing pipelines.'''
    other = '''
    [[tasks]]
    task = 'grass'
    message = 'c'
    params = {module='v.import', input='tests/point.geojson', output='mypoint'}
    '''

    config = '''
    location = 'foobar'

    [[tasks]]
    task = 'grass'
    message = 'a'
    params = {module='g.proj', c=true, proj4='+proj=utm +zone=33 +datum=WGS84'}

    [[tasks]]
    pipeline = '{{ other }}'
    message = 'b'
    '''

    fopts = dict(mode='w', dir=env.root, prefix='config_', suffix='.toml')
    with tempfile.NamedTemporaryFile(**fopts) as fp:
        fp.write(other)
        fp.flush()
        returncode, output, _ = env.run([
            '--verbose',
            '--vars',
            'other={}'.format(os.path.basename(fp.name))
        ], config)
    assert returncode == 0
    assert output == '''[0]: a
  Completed
[1/0]: c
  Completed
'''


def test_tasks_composite_pipeline_retained_state(env):
    '''Composing pipelines.'''
    other = '''
    [[tasks]]
    task = 'grass'
    message = 'c'
    params = {module='v.import', input='tests/point.geojson', output='mypoint'}
    '''

    config = '''
    location = 'foobar'

    [[tasks]]
    task = 'grass'
    message = 'a'
    params = {module='g.proj', c=true, proj4='+proj=utm +zone=33 +datum=WGS84'}

    [[tasks]]
    pipeline = '{{ other }}'
    message = 'b'
    '''

    fopts = dict(mode='w', dir=env.root, prefix='config_', suffix='.toml')
    with tempfile.NamedTemporaryFile(**fopts) as fp:
        fp.write(other)
        fp.flush()
        returncode, _, _ = env.run([
            '--verbose',
            '--vars',
            'other={}'.format(os.path.basename(fp.name))
        ], config)
        assert returncode == 0
        returncode, output, _ = env.run([
            '--verbose',
            '--vars',
            'other={}'.format(os.path.basename(fp.name))
        ], config)
        assert returncode == 0
        assert output == '''[0]: a
  Completed
[1/0]: c
  Skipped
'''


def test_state_cleaning(env):
    '''Retained state should remove unneeded info.'''
    returncode, _, _ = env.run([], '''
    location = 'foobar'

    [[tasks]]
    task = 'grass'
    params = {module='g.proj', c=true, proj4='+proj=utm +zone=33 +datum=WGS84'}
    ''')
    assert returncode == 0

    returncode, _, _ = env.run([], '''
    location = 'foobar'

    [[tasks]]
    task = 'grass'
    params = {module='g.proj', c=true, proj4='+proj=utm +zone=31 +datum=WGS84'}
    ''')
    assert returncode == 0

    state_path = os.path.join(env.gisdbase, 'foobar', 'PERMANENT',
                              'stitches.state.json')
    with open(state_path) as fp:
        state = json.load(fp)
        assert len(state['history'].keys()) == 1


def test_tasks_region_change(env):
    '''Region settings are checked when caching tasks.'''
    pipeline = '''
    location = 'foobar'

    [[tasks]]
    message = 'a'
    task = 'grass'
    [tasks.params]
    module = 'g.proj'
    c = true
    proj4 = '+proj=utm +zone={{ zone }} +datum=WGS84'

    [[tasks]]
    task = 'grass'
    message = 'b'
    [tasks.params]
    module = 'v.import'
    input = 'tests/point.geojson'
    output = 'mypoint'
    overwrite = true
    '''
    returncode, _, _ = env.run(['--verbose', '--vars', 'zone=33'], pipeline)
    assert returncode == 0

    _, output, _ = env.run(['--verbose', '--vars', 'zone=32'], pipeline)
    assert output == '''[0]: a
  Completed
[1]: b
  Completed
'''

    _, output, _ = env.run(['--verbose', '--vars', 'zone=32'], pipeline)
    assert output == '''[0]: a
  Completed
[1]: b
  Skipped
'''
