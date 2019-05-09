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

import contextlib
import os
import subprocess
import sys


def _process(cmd):
    pipe = subprocess.PIPE
    proc = subprocess.Popen(cmd, stdin=pipe, stdout=pipe, stderr=pipe)
    (out, err) = proc.communicate()
    returncode = proc.returncode
    return (returncode, out.decode('utf-8'), err.decode('utf-8'))


def _grass_binary(version=None):
    grassbin = os.environ.get('GRASSBIN')
    if grassbin:
        return grassbin

    pattern = 'grass{version}'
    if sys.platform.startswith('win'):
        pattern = 'C:\\OSGeo4W\\bin\\grass{version}svn.bat'
    elif sys.platform.startswith('darwin'):
        pattern = '/Applications/GRASS/GRASS-{version[0]}.{version[1]}.app'

    versions = [version] if version else ['76', '74']
    for ver in versions:
        grassbin = pattern.format(version=ver)
        code, _, _ = _process([grassbin, '--config'])
        if code == 0:
            return grassbin

    raise RuntimeError('Cannot find the GRASS GIS binary')


def _grass_install_dir(grassbin):
    code, out, err = _process([grassbin, '--config', 'path'])
    if code != 0:
        raise RuntimeError(err)
    return out.strip()


@contextlib.contextmanager
def session(gisdbase, location, mapset=None, c=None, version=None,
            grassbin=None, gisbase=None, skip=None):
    if skip:
        yield
        return

    grassbin = grassbin if grassbin else _grass_binary(version=version)
    gisbase = gisbase if gisbase else _grass_install_dir(grassbin)

    mapset = mapset or 'PERMANENT'
    lpath = os.path.join(gisdbase, location)
    mpath = os.path.join(lpath, mapset)

    for path in [lpath, mpath]:
        if not os.path.exists(path):
            code, _, err = _process([grassbin, '-c', c or '', '-e', path])
            if code != 0:
                raise RuntimeError(err)

    os.environ['GISBASE'] = gisbase
    grass_python_path = os.path.join(gisbase, 'etc', 'python')
    sys.path.append(grass_python_path)
    from ._grass import gsetup

    try:
        gsetup.init(gisbase, dbase=gisdbase, location=location, mapset=mapset)
        yield
    finally:
        os.remove(os.environ['GISRC'])
        os.environ.pop('GISRC')
        os.environ.pop('GIS_LOCK')
