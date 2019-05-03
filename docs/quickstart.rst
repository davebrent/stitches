Quickstart
==========
Once stitches is installed, the ``stitches`` command should become available in
your ``$PATH``.

Create a simple pipeline file

.. code-block::

   gisdbase = 'grassdata'
   location = 'helloworld'

   [[tasks]]
   message = 'Hello world'
   task = 'grass'
   params = {module='g.proj', c=true, proj4='+proj=utm +zone=33 +datum=WGS84'}

Save this file as :file:`pipeline.toml` (or any name you like).

Then run the pipeline with stitches in verbose mode

.. code-block:: bash

   $ stitches --verbose pipeline.toml

This should print the following to the console

.. code-block:: bash

   [0]: Hello world
     Completed

Please see the `examples`_ folder for more advanced uses of pipelines.

.. _`examples`: https://github.com/davebrent/stitches/tree/master/examples
