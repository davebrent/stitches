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

    if context.initial:
        region_settings = config.get('pipeline', {})
        if not region_settings:
            raise Error('A stitches file must contain a pipeline definition')
        mapset = region_settings.get('mapset')
        location = region_settings.get('location', None)
        if not location:
            raise Error('A stitches file must contain [pipeline.location]')

    if context.initial:
        context.initial = False
        # Create opts needs to be not None otherwise grass session will
        # not create the location automatically
        with grass_session.Session(gisdb=context._grassdata,
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
