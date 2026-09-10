"""Exception types for aiocometd.

Exception hierarchy::

    AiocometdException
        ├── ClientError
        │     └── ClientInvalidOperation
        └── TransportError
              ├── TransportInvalidOperation
              ├── TransportTimeoutError
              ├── TransportConnectionClosed
              └── ServerError
"""

from __future__ import annotations

from typing import cast

from aiocometd import utils
from aiocometd.typing_utils import JsonObject


class AiocometdException(Exception):
    """Base exception type for all aiocometd errors.

    All custom exceptions in this package inherit from this class.
    """


# ---------------------------------------------------------------------------
# Transport-level exceptions
# ---------------------------------------------------------------------------


class TransportError(AiocometdException):
    """Error raised during the transportation of messages."""


class TransportInvalidOperation(TransportError):
    """Raised when the requested operation cannot be executed in the current transport state."""


class TransportTimeoutError(TransportError):
    """Raised when a transport operation exceeds its allowed timeout."""


class TransportConnectionClosed(TransportError):
    """Raised when the transport connection closes unexpectedly."""


# ---------------------------------------------------------------------------
# Server-level exceptions
# ---------------------------------------------------------------------------


class ServerError(AiocometdException):
    """CometD server-side error.

    This exception is raised when the server responds with an error message. If the response contains an `error` field,
    it is parsed according to the CometD specification.

    See: https://docs.cometd.org/current/reference/#_code_error_code
    """

    def __init__(self, message: str, response: JsonObject | None) -> None:
        """Initialize a ServerError.

        Args:
            message (str): A textual description of the error.
            response (Optional[utils.JsonObject]): The server response message that triggered the error.
        """
        super().__init__(message, response)

    @property
    def message(self) -> str:
        """Return the error description provided during initialization."""
        return cast(str, self.args[0])

    @property
    def response(self) -> JsonObject | None:
        """Return the server response message, if available."""
        return cast(JsonObject | None, self.args[1])

    @property
    def error(self) -> str | None:
        """Return the raw `error` field from the server response, if present."""
        if self.response is None:
            return None
        return cast(str | None, self.response.get("error"))

    @property
    def error_code(self) -> int | None:
        """Return the numeric error code extracted from the response.

        Returns:
            Optional[int]: The numeric code (e.g., 401, 403) if found in the error field, otherwise ``None``.
        """
        return utils.get_error_code(self.error)

    @property
    def error_message(self) -> str | None:
        """Return the descriptive part of the error field.

        Returns:
            Optional[str]: The human-readable error message if present, otherwise ``None``.
        """
        return utils.get_error_message(self.error)

    @property
    def error_args(self) -> list[str] | None:
        """Return the list of argument values included in the error field.

        Returns:
            Optional[List[str]]: List of arguments extracted from the error field, or ``None`` if not applicable.
        """
        return utils.get_error_args(self.error)


# ---------------------------------------------------------------------------
# Client-level exceptions
# ---------------------------------------------------------------------------


class ClientError(AiocometdException):
    """CometD client-side error."""


class ClientInvalidOperation(ClientError):
    """Raised when a client operation cannot be performed in its current state."""
