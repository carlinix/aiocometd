Installation
============

aiocometd requires Python 3.11 or newer.

.. code-block:: bash

    pip install aiocometd

Development
-----------

Install uv_, clone the repository, and create the locked development
environment:

.. code-block:: bash

    uv sync --all-groups --all-extras

Run the local checks through uv:

.. code-block:: bash

    uv run ruff check .
    uv run ruff format --check .
    uv run coverage run -m unittest discover tests/unit
    uv run sphinx-build -W --keep-going -b html docs/source docs/build/html

The command-line example has one optional dependency. Install it with:

.. code-block:: bash

    uv sync --extra examples

.. _uv: https://docs.astral.sh/uv/
