Stitches
========

.. image:: https://readthedocs.org/projects/stitches/badge/?version=latest&style=flat-square
   :target: https://stitches.readthedocs.io/en/latest
   :alt: Documentation Status

Stitches is a task runner for `GRASS GIS`_, an alternative to running BASH and
Python scripts with Grass's ``--exec`` option.

.. image:: http://davepoulter.net/media/stitches.png
   :alt: Stitches output
   :align: center

Features
--------
- Session support: no need to start `GRASS GIS`_ before running any tasks.
- Caching: task state is tracked to skip tasks when possible to do so.
- Composability: tasks may be organised into pipelines and used as tasks.
- Pipelines may be called with custom variables and use `Jinja2`_ in their
  definitions for more generic data processing.
- Custom tasks may be written as simple python functions.

.. _GRASS GIS: https://grass.osgeo.org/
.. _Jinja2: http://jinja.pocoo.org/docs/2.10/

Install
-------
.. code-block:: shell

   $ pip install stitches-gis

Development
-----------
.. code-block:: shell

   $ tox          # Run tests
   $ tox -e lint  # Lint source
   $ tox -e docs  # Build documentation

Contribute
----------
- `Issue Tracker`_
- `Source Code`_
- `Documentation`_

.. _Issue Tracker: https://github.com/davebrent/stitches/issues
.. _Source Code: https://github.com/davebrent/stitches
.. _Documentation: https://stitches.readthedocs.io/en/latest

License
-------
The project is licensed under GPLv3.
