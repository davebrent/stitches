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

'''
Stitches.

Usage:
  stitches [--gisdbase=<path>] [--location=<name>] [--mapset=<name>]
           [[--skip=<task>]... [--force] | --only=<task>]
           [--log=<path>] [--verbose]
           [--vars=<vars>] <pipeline>

Options:
  -h --help             Show this screen.
  -v --verbose          Show more output.
  --log=<path>          Task log output path.
  --gisdbase=<path>     Initial GRASS GIS database directory.
  --location=<name>     Initial GRASS location.
  --mapset=<name>       Initial GRASS Mapset.
  --skip=<task>         Comma-separated list of tasks to skip.
  --only=<task>         Run a single task.
  --force               Force all tasks to run.
  --vars=<vars>         Initial pipeline variables.
'''

from __future__ import print_function

import datetime
import os
import sys
import traceback
try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO

import docopt
import jinja2

from .core import State
from .core import Platform
from .core import TaskFatalEvent
from .core import TaskCompleteEvent
from .core import VerboseReporter
from .core import SilentReporter
from .core import analyse
from .core import load
from .core import execute
from .session import session


def main():
    args = docopt.docopt(__doc__)

    variables = {}
    if args['--vars']:
        for var in args['--vars'].split(' '):
            name, value = var.split('=')
            variables[name] = value

    reporter = SilentReporter()
    if args['--verbose']:
        reporter = VerboseReporter()

    root = os.path.dirname(os.path.abspath(args['<pipeline>']))
    jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(root))
    jinja_env.filters['basename'] = os.path.basename
    jinja_env.filters['dirname'] = os.path.dirname

    # Load the stream of tasks
    stream = load(jinja_env, {
        'pipeline': os.path.basename(args['<pipeline>']),
        'params': {
            'vars': variables,
            'gisdbase': args['--gisdbase'],
            'location': args['--location'],
            'mapset': args['--mapset'],
        }
    })

    # First event in the stream must be a location event
    head = next(stream)
    gisdbase = head.gisdbase
    location = head.location
    mapset = head.mapset

    # Load previous state
    state = State.load(os.path.join(
        gisdbase, location, mapset or 'PERMANENT', 'stitches.state.json'
    ))

    platform = Platform()

    # Analyse the stream of events with the previous state
    stream = analyse(stream, platform, state.history,
                     force=args['--force'],
                     skip=args['--skip'],
                     only=args['--only'])

    (code, stdout, stderr) = (0, StringIO(), StringIO())
    try:
        os.environ['GRASS_MESSAGE_FORMAT'] = 'plain'
        with session(gisdbase, location, mapset=mapset):
            for event in execute(stream, stdout, stderr):
                if isinstance(event, TaskCompleteEvent):
                    state.save()
                reporter(event)
            state.save()
    except Exception:  # pylint: disable=broad-except
        stack_trace = traceback.format_exc()
        reporter(TaskFatalEvent(stack_trace))
        if not args['--log']:
            uniq = datetime.datetime.now().strftime('%H_%M_%S_%f')
            args['--log'] = 'stitches.grass-{}.log'.format(uniq)
        code = 1

    if args['--log']:
        outlog = stdout.getvalue()
        errlog = stderr.getvalue()
        with open(args['--log'], 'w') as fp:
            print('****STDOUT****', file=fp)
            fp.write(outlog)
            print('****STDERR****', file=fp)
            fp.write(errlog)
        stdout.close()
        stderr.close()

    sys.exit(code)
