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


def grass(module=None, **kwargs):
    '''Run a GRASS GIS command.

    Please refer to the relevant version of `documentation`_ for
    ``grass.pygrass.modules.Module`` for more information.

    .. _documentation: https://grass.osgeo.org/grass76/manuals/libpython/pygrass_modules.html

    Keyword Args:
        module (str): GRASS GIS command name
        **kwargs: Keyword arguments passed to ``grass.pygrass.modules.Module``

    '''
    from ._grass import Module
    assert module
    instance = Module(module)
    instance(**kwargs)


def script(cmd=None):
    '''Run an arbitrary shell command.

    Keyword Args:
        cmd (list): A sequence of program arguments

    '''
    assert cmd
    subprocess.check_call(cmd)
