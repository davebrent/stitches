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

from grass.pygrass.modules import Module  # pylint: disable=import-error


def grass(params):
    '''A task for calling GRASS modules.'''
    name = params.pop('module')
    module = Module(name)
    module(**params)


def script(params):
    '''A task for running an executable.'''
    subprocess.check_call(params)
