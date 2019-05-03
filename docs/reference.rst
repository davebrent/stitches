Reference
=========

Toml configuration options
--------------------------

Pipeline
~~~~~~~~

.. csv-table::
   :header: "Property", "Type", "Description"
   :widths: 15, 15, 70

   ``gisdbase``, str, Initial grass database directory.
   ``location``, str, Initial grass location.
   ``mapset``, str, Initial grass mapset (default: ``'PERMANENT'``).
   ``tasks``, List[:ref:`Task`], Tasks to run against the mapset.

Task
~~~~

.. csv-table::
   :header: "Property", "Type", "Description"
   :widths: 15, 15, 70

   ``message``, str, Text to display when the task is run.
   ``pipeline``, str, Path to a pipeline file.
   ``task``, str, Built-in task name (see :ref:`built-in`) or a reference to an importable python function eg. ``package.module:function``.
   ``inputs``, List[str], List of input resources.
   ``outputs``, List[str], List of output resources.
   ``removes``, List[str], List of resources removed by the task.
   ``always``, bool, Option to always run the task/pipeline.
   ``params``, dict, Task/pipeline keyword arguments.

- Either ``pipeline`` or ``task`` must be defined.

Pipeline task ``params``
~~~~~~~~~~~~~~~~~~~~~~~~

.. csv-table::
   :header: "Property", "Type", "Description"
   :widths: 15, 15, 70

   ``gisdbase``, str, Grass database directory (not implemented).
   ``location``, str, Grass location (not implemented).
   ``mapset``, str, Grass mapset (not implemented).
   ``vars``, dict, Variables passed into the pipeline.

- Switching database, location and mapset automatically, when calling another
  pipeline, is not yet implemented.

.. _built-in:

Built-in Tasks
--------------

.. automodule:: stitches.tasks
    :members:
