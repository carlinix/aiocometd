"""Type definitions for aiocometd."""

from __future__ import annotations

import ssl
from typing import Any, TypeAlias
from collections.abc import Awaitable, Callable
from aiohttp import Fingerprint
from aiocometd.constants import ConnectionType


# ---------------------------------------------------------------------------
# Coroutine & JSON Type Aliases
# ---------------------------------------------------------------------------

CoroFunction: TypeAlias = Callable[..., Awaitable[Any]]
JsonObject: TypeAlias = dict[str, Any]
JsonDumper: TypeAlias = Callable[[JsonObject], str]
JsonLoader: TypeAlias = Callable[[str], JsonObject]
Payload: TypeAlias = list[JsonObject]
Headers: TypeAlias = dict[str, str]

# ---------------------------------------------------------------------------
# Protocol-Specific Type Aliases
# ---------------------------------------------------------------------------

ConnectionTypeSpec: TypeAlias = ConnectionType | list[ConnectionType]
SSLValidationMode: TypeAlias = ssl.SSLContext | Fingerprint | bool
