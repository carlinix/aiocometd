"""Utility functions for handling CometD message validation and errors."""

from __future__ import annotations

import asyncio
import re
from functools import wraps
from http import HTTPStatus
from typing import Any

from aiocometd.constants import META_CHANNEL_PREFIX, SERVICE_CHANNEL_PREFIX
from aiocometd.typing_utils import CoroFunction, JsonObject


def defer(coro_func: CoroFunction, delay: int | float | None = None) -> CoroFunction:
    """Return a coroutine function that defers execution by a given delay.

    Args:
        coro_func (CoroFunction): The coroutine function to be wrapped.
        delay (int | float | None): Optional delay in seconds before executing
            the coroutine.

    Returns:
        CoroFunction: A coroutine function wrapper that executes after a delay.
    """

    @wraps(coro_func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        if delay:
            await asyncio.sleep(delay)
        return await coro_func(*args, **kwargs)

    return wrapper


def get_error_code(error_field: str | None) -> int | None:
    """Extract the HTTP-like error code from a CometD error field.

    The error field typically follows the format: "code:args:message".

    Args:
        error_field (str | None): The error field from a CometD message.

    Returns:
        Optional[int]: The numeric error code if found, otherwise ``None``.
    """
    if error_field is None:
        return None
    match = re.search(r"^\d{3}", error_field)
    return int(match[0]) if match else None


def get_error_message(error_field: str | None) -> str | None:
    """Extract the description part from a CometD error field.

    The description is typically the last section in the error string
    after the final colon (e.g., "403::Invalid token" → "Invalid token").

    Args:
        error_field (str | None): The error field from a CometD message.

    Returns:
        Optional[str]: The error message description, or ``None`` if not found.
    """
    if error_field is None:
        return None
    match = re.search(r"(?<=:)[^:]*$", error_field)
    return match[0] if match else None


def get_error_args(error_field: str | None) -> list[str] | None:
    """Extract the argument list from a CometD error field.

    The arguments are typically found between the first and second colons
    (e.g., "403:user,scope:Unauthorized" → ["user", "scope"]).

    Args:
        error_field (str | None): The error field from a CometD message.

    Returns:
        Optional[list[str]]: A list of argument strings, an empty list if the
        section exists but is empty, or ``None`` if not found.
    """
    if error_field is None:
        return None
    match = re.search(r"(?<=:).*(?=:)", error_field)
    if not match:
        return None
    return match[0].split(",") if match[0] else []


def is_matching_response(
    response_message: JsonObject, message: JsonObject | None
) -> bool:
    """Check if a response message corresponds to a sent message.

    Two messages are considered matching if:
    - Their ``channel`` values are equal.
    - Their ``id`` fields are equal (if present).
    - The response contains a ``successful`` field.

    Args:
        response_message (JsonObject): The received message.
        message (Optional[JsonObject]): The original sent message.

    Returns:
        bool: ``True`` if the response matches the message, otherwise ``False``.
    """
    if message is None or response_message is None:
        return False

    return (
        message["channel"] == response_message["channel"]
        and message.get("id") == response_message.get("id")
        and "successful" in response_message
    )


def is_server_error_message(response_message: JsonObject) -> bool:
    """Check if a response message indicates a server-side error.

    Args:
        response_message (JsonObject): The response message to inspect.

    Returns:
        bool: ``True`` if ``successful`` is missing or False, otherwise ``False``.
    """
    return not response_message.get("successful", True)


def is_event_message(response_message: JsonObject) -> bool:
    """Check if a message is an event message.

    An event message is:
    - Not on a meta channel.
    - Not a service response (i.e., no ``id`` field if on a service channel).
    - Contains a ``data`` field.

    Args:
        response_message (JsonObject): The response message to check.

    Returns:
        bool: ``True`` if the message is an event message, otherwise ``False``.
    """
    channel = response_message["channel"]
    return (
        not channel.startswith(META_CHANNEL_PREFIX)
        and (
            not channel.startswith(SERVICE_CHANNEL_PREFIX)
            or "id" not in response_message
        )
        and "data" in response_message
    )


def is_auth_error_message(response_message: JsonObject) -> bool:
    """Check whether a response message indicates an authentication failure.

    A message is considered an authentication error if its error code is
    ``401 Unauthorized`` or ``403 Forbidden``.

    Args:
        response_message (JsonObject): The response message to inspect.

    Returns:
        bool: ``True`` if the message represents an authentication error,
        otherwise ``False``.
    """
    error_code = get_error_code(response_message.get("error"))
    return error_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN)
