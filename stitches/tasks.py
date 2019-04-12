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

import subprocess

import toml
import grass_session
from grass.pygrass.modules import Module

from .core import Error
from .core import execute


def pipeline(context, params):
    '''A task for running a pipeline.'''
    name = params.pop('name')
    variables = params.pop('vars', {})

    template = context._jinja.get_template(name)
    config = toml.loads(template.render(variables))

    gisdbase = config.get('gisdbase', context.gisdbase)
    mapset = config.get('mapset')
    location = config.get('location')

    if context.initial:
        context.initial = False
        if not gisdbase or not location:
            raise Error('Missing GISDBASE and Location parameters')
        # Create opts needs to be not None otherwise grass session will
        # not create the location automatically
        with grass_session.Session(gisdb=gisdbase,
                                   location=location,
                                   mapset=mapset,
                                   create_opts=''):
            execute(context, config, **params)
    else:
        execute(context, config, **params)


def grass(context, params):
    '''A task for calling GRASS modules.'''
    name = params.pop('module')
    module = Module(name)
    module(**params)


def script(context, params):
    '''A task for running an executable.'''
    subprocess.check_call(params)
