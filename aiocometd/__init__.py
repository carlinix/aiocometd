"""CometD client for asyncio."""

import logging
from importlib.metadata import version

from aiocometd import transports
from aiocometd.client import Client
from aiocometd.constants import ConnectionType
from aiocometd.extensions import AuthExtension, Extension

__version__ = version("aiocometd")

__all__ = [
    "AuthExtension",
    "Client",
    "ConnectionType",
    "Extension",
    "__version__",
    "transports",
]

# Create a default handler to avoid warnings in applications without logging
# configuration
logging.getLogger(__name__).addHandler(logging.NullHandler())
