Changelog
=========

Unreleased
----------

- Transfer project maintenance to Ricardo Carlini Sperandio from 2025
- Replace ``setup.py`` and tool-specific configuration with ``pyproject.toml``
- Adopt uv for dependency locking, environments, and package builds
- Replace Travis CI and tox with a GitHub Actions Python 3.11-3.14 matrix
- Add strict distribution metadata, wheel-content, and clean-install smoke
  checks to continuous integration
- Add an opt-in, tag-driven release workflow for PyPI, Google Artifact
  Registry, and GitHub releases
- Replace Flake8 and Pylint with Ruff linting and formatting
- Stop creating unmanaged asyncio event loops during transport construction

0.4.5 (2019-03-14)
------------------

- Fix connection issues when used with reverse proxy servers with cookie based
  sticky sessions

0.4.4 (2019-02-26)
------------------

- Refactor the websocket transport implementation to use a single connection
  per client

0.4.3 (2019-02-12)
------------------

- Fix reconnection issue on Salesforce Streaming API

0.4.2 (2019-01-15)
------------------

- Fix the handling of invalid websocket transport responses
- Fix the handling of failed subscription responses

0.4.1 (2019-01-04)
------------------

- Add documentation links

0.4.0 (2019-01-04)
------------------

- Add type hints
- Add integration tests

0.3.1 (2018-06-15)
------------------

- Fix premature request timeout issue

0.3.0 (2018-05-04)
------------------

- Enable the usage of third party JSON libraries
- Fix detection and recovery from network failures

0.2.3 (2018-04-24)
------------------

- Fix RST rendering issues

0.2.2 (2018-04-24)
------------------

- Fix documentation typos
- Improve examples
- Reorganise documentation

0.2.1 (2018-04-21)
------------------

- Add PyPI badge to README

0.2.0 (2018-04-21)
------------------

- Supported transports:
   - ``long-polling``
   - ``websocket``
- Automatic reconnection after network failures
- Extensions
