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

from ._grass import Module


def grass(module=None, **kwargs):
    '''A task for calling GRASS modules.'''
    assert module
    instance = Module(module)
    instance(**kwargs)


def script(cmd=None):
    '''A task for running an executable.'''
    assert cmd
    subprocess.check_call(cmd)
