Usage
=====

.. literalinclude:: ../stitches/cli.py
   :language: bash
   :start-after: '''
   :end-before: '''

Run a pipeline with custom variables

.. code-block:: bash

   $ stitches --vars="foo='hello' bar='world'" pipeline.toml

Skip the 2nd and 4th tasks in a pipeline

.. code-block:: bash

   $ stitches --skip=1,3 pipeline.toml
