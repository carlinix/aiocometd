"""Sphinx configuration for the aiocometd documentation."""

from importlib.metadata import version as package_version

project = "aiocometd"
copyright = "2018-2025, Róbert Márki; 2025-2026, Ricardo Carlini Sperandio"
author = "Ricardo Carlini Sperandio"
release = package_version("aiocometd")
version = release

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
]

root_doc = "index"
html_theme = "alabaster"
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}
autoclass_content = "both"
autodoc_typehints = "description"
