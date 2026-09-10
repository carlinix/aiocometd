Releasing
=========

Releases are built once by ``.github/workflows/release.yml``. The validated
wheel and source distribution are then reused unchanged by every enabled
package registry and by the GitHub release.

Repository configuration
------------------------

PyPI publishing is disabled by default. To enable it:

#. Create a GitHub environment named ``pypi``.
#. Configure PyPI Trusted Publishing with owner ``carlinix``, repository
   ``aiocometd``, workflow ``release.yml``, and environment ``pypi``.
#. Set the GitHub Actions repository variable ``ENABLE_PYPI_PUBLISH`` to
   ``true``.

Google Artifact Registry publishing is also disabled by default. To enable it:

#. Create a GitHub environment named ``gcp``.
#. Configure Workload Identity Federation for this repository and grant its
   service account permission to upload packages to the target repository.
#. Add the GitHub Actions secrets ``GCP_WORKLOAD_IDENTITY_PROVIDER`` and
   ``GCP_SERVICE_ACCOUNT``.
#. Set ``GCP_ARTIFACT_REGISTRY_URL`` to the upload URL, such as
   ``https://REGION-python.pkg.dev/PROJECT/REPOSITORY/``.
#. Set ``ENABLE_GCP_PUBLISH`` to ``true``.

No long-lived PyPI or Google Cloud credential is required. A disabled registry
job is skipped; an enabled registry must publish successfully before the
GitHub release is created.

Release procedure
-----------------

#. Set ``project.version`` in ``pyproject.toml`` to the release version and
   update this changelog.
#. Merge the release commit and wait for continuous integration to pass.
#. Create and push an annotated tag whose value matches the package version.
   A leading ``v`` is optional::

       git tag -a v1.1.0 -m v1.1.0
       git push origin v1.1.0

#. Verify the Release workflow. It validates the tag, builds and checks both
   distributions, publishes to each enabled registry, and creates the GitHub
   release with the same files.

An existing version tag can be retried with the workflow's manual ``ref``
input. Publishing a version that already exists in a package registry is
expected to fail; published package files are immutable.
