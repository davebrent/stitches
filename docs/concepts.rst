.. include:: _terms.rst

Concepts
========

Pipeline
--------
A pipeline is a |Jinja| template file, that renders a |TOML| file, containing a
list of :ref:`task` definitions, to be executed sequentially.

Although there is no hard restriction, it is expected that a pipeline be run
multiple times (such as during development) so it is suggested that they be
`indempotent`_ with respect to its inputs and outputs.

.. _indempotent: https://en.wikipedia.org/wiki/Idempotence

A pipeline may declare the |GRASS| database, location and mapset that it should
be run against, or these values may be passed in via the command line.

Task
----
A task may consist of one of the following:

- One of the provided :ref:`built-in`.
- Another pipeline.
- An importable python callable, in the form of ``importable.module:function``.
  The referenced function is called with the task definition's ``params`` field
  as keyword arguments.

Resource
--------
Resources may consist of |GRASS| maps or regular files, their references should
follow the format ``<type>/(<filepath> | <grassref>)``. Examples of valid
references:

.. code-block:: python

   'file/foobar/baz.tif'                  # Relative path
   'file//foobar/baz.tif'                 # Absolute path
   'vector/map@gisdbase/location/mapset'  # Map in specific database
   'vector/map@location/mapset'           # Map in a specific location
   'vector/map@mapset'                    # Map in a specific mapset
   'vector/map'                           # Map in this mapset

Its recommended to reference the resources used by a task to make the most of
:ref:`Caching`.

Caching
-------
The current state of resources used in a pipeline is tracked. If the following
conditions are met the task will be skipped:

- The task is executed in the same `region` as its previous execution.
- The tasks ``params`` are unchanged.
- No input files have been modified.
- Tasks that created any input maps were also skipped.
- Its output resources already exist.

A task will not be skipped if it is not possible for stitches to track the
creation of any mapset used by the task.

State
-----
The state of the initial pipeline's execution is stored in a file called
``stitches.state.json`` in the pipeline's `initial` mapset. This may lead to
unexpected results when running different `initial` pipelines against the same
mapset.

Errors & Logging
----------------
In the event that a task raises an exception, the output of all tasks,
including |GRASS| output, is automatically written to file for inspection. This
log may be written to a specified location and will always be outputted using
the ``--log`` option.
