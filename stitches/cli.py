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

'''Stitches.

Usage:
  stitches [--gisdbase=<path>] [[--skip=<task>]... [--force] | --only=<task>]
           [--log=<path>] [--verbose] [--vars=<vars>] <config>

Options:
  -h --help             Show this screen.
  -v --verbose          Show more output.
  --gisdbase=<path>     Set path to the GRASS GIS Database.
  --log=<path>          Print GRASS's stdout at the end of run.
  --skip=<task>         Skip a task.
  --only=<task>         Run a single task.
  --force               Force each task to run.
  --vars=<vars>         Variables to run the pipeline with.
'''

from __future__ import print_function

import datetime
import os
import sys
import traceback

import docopt
import jinja2

from .tasks import pipeline
from .core import State
from .core import Context
from .core import TaskFatalEvent
from .core import VerboseReporter
from .core import SilentReporter


def main():
    args = docopt.docopt(__doc__)

    root = os.path.dirname(os.path.abspath(args['<config>']))

    variables = {}
    if args['--vars']:
        for var in args['--vars'].split(' '):
            name, value = var.split('=')
            variables[name] = value

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(root))
    state_path = os.path.join(root, 'stitches.state.toml')
    state = State.load(state_path)

    reporter = SilentReporter()
    if args['--verbose']:
        reporter = VerboseReporter()
    context = Context(state, state_path, env,
                      gisdbase=args['--gisdbase'],
                      reporter=reporter)

    code = 0
    try:
        pipeline(context, {
            'vars': variables,
            'name': os.path.basename(args['<config>']),
            'skip': args['--skip'],
            'force': args['--force'],
            'only': args['--only'],
        })
    except Exception:  # pylint: disable=broad-except
        stack_trace = traceback.format_exc()
        context.reporter(TaskFatalEvent(stack_trace))
        if not args['--log']:
            uniq = datetime.datetime.now().strftime('%H_%M_%S_%f')
            args['--log'] = 'stitches.grass-{}.log'.format(uniq)
        code = 1

    if args['--log']:
        outlog = context.stdout.getvalue()
        errlog = context.stderr.getvalue()
        with open(args['--log'], 'w') as fp:
            print('****STDOUT****', file=fp)
            fp.write(outlog)
            print('****STDERR****', file=fp)
            fp.write(errlog)
        context.stdout.close()
        context.stderr.close()

    sys.exit(code)
