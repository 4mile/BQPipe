=============================
bqpipe: BigQuery & DataFrames
=============================

A lightweight Python wrapper around Google Cloud BigQuery's API
to simplify read and write between Pandas DataFrames and BigQuery.

Features
--------
- Create new tables with specified schema
- Append DataFrames to existing tables
- Simply read from tables or arbitrary SELECT statements
- Get BigQuery metadata (datasets, tables, table schemas, etc.)


Dependencies
------------
BQPipe supports Python 3.7+.

Installation requires `NumPy
<http://www.numpy.org/>`_,
`Pandas
<https://pandas.pydata.org/>`_,
`Google-Cloud-BigQuery
<https://pypi.org/project/google-cloud-bigquery/>`_,
and `PyArrow
<https://pypi.org/project/pyarrow/>`_.

Installation
------------
The latest stable release (and older versions) can be
installed from PyPI:

.. code-block:: console

    pip install bqpipe

You may instead want to use the development version from Github:

.. code-block:: console

    pip install git+https://github.com/4mile/bqpipe.git#egg=bqpipe

Testing
-------
Coming Soon

Development
-----------
BQPipe development takes place on `GitHub`_.

.. _Github: https://github.com/4mile/bqpipe

Please submit any reproducible bugs you encounter to
the `Issue Tracker`_.

.. _Issue Tracker: https://github.com/4mile/bqpipe/issues)