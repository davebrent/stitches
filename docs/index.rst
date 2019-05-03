.. include:: _terms.rst

Stitches documentation
======================
Stitches is a task runner for |GRASS|, an alternative to running BASH and
Python scripts with Grass's ``--exec`` option.

Features
--------

- Session support: no need to start |GRASS| before running any tasks.
- :ref:`Caching`: task state is tracked to skip tasks when possible to do so.
- Composability: tasks may be organised into pipelines and used as tasks.
- Pipelines may be called with custom variables and use |Jinja| in their
  definitions for more generic data processing.
- Custom tasks may be written as simple python functions.

.. toctree::
   :maxdepth: 2

   install.rst
   quickstart.rst
   usage.rst
   concepts.rst
   reference.rst
   contribute.rst
