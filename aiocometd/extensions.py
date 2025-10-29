"""Extension classes for aiocometd."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional
from aiocometd.typing_utils import Payload, Headers


class Extension(ABC):
    """Defines the operations supported by CometD extensions."""

    @abstractmethod
    async def outgoing(self, payload: Payload, headers: Headers) -> None:
        """Process outgoing payload and headers.

        This method is called just before a payload is sent to the server. Extensions can modify the payload or headers
        before transmission, such as adding metadata, authentication tokens, or performing logging.

        Args:
            payload (Payload): List of outgoing messages to be sent.
            headers (Headers): HTTP headers to include with the outgoing messages.
        """
        raise NotImplementedError("Subclasses must implement outgoing()")

    @abstractmethod
    async def incoming(self, payload: Payload, headers: Optional[Headers] = None) -> None:
        """Process incoming payload and headers.

        This method is called immediately after a payload is received from the server. Extensions can use this to modify,
        validate, or inspect incoming messages before they are handled by the client.

        Args:
            payload (Payload): List of incoming messages from the server.
            headers (Optional[Headers]): Optional HTTP headers received with the messages.
        """
        raise NotImplementedError("Subclasses must implement incoming()")


class AuthExtension(Extension):
    """Extension class that adds authentication support.

    This extension provides hooks for injecting authentication information into outgoing requests and handling
    authentication-related responses from the server. It can be subclassed to implement specific authentication
    mechanisms such as token-based, OAuth, or custom headers.
    """

    async def outgoing(self, payload: Payload, headers: Headers) -> None:
        """Attach authentication data to outgoing payloads or headers.

        Called immediately before the payload is sent to the server. Implementations can modify the outgoing messages
        or add authentication-related headers (for example, authorization tokens or API keys).

        Args:
            payload (Payload): List of outgoing messages to be sent to the server.
            headers (Headers): HTTP headers that will accompany the outgoing messages. Can be modified in-place.
        """
        pass

    async def incoming(self, payload: Payload, headers: Optional[Headers] = None) -> None:
        """Handle authentication-related responses from the server.

        Called right after a payload is received from the server. Implementations can use this to detect authentication
        errors (such as expired tokens) and trigger credential refresh logic or reauthentication if necessary.

        Args:
            payload (Payload): List of incoming messages received from the server.
            headers (Optional[Headers]): Optional HTTP headers included in the server's response.
        """
        pass

    async def authenticate(self) -> None:
        """Handle authentication retry after a failed attempt.

        This method is called after an authentication failure. For authentication schemes with static credentials, there
        is usually no need to override this method. However, for schemes with expiring credentials (e.g., OAuth, JWT),
        subclasses can override this method to refresh or regenerate credentials before retrying the connection.
        """
        raise NotImplementedError("Subclasses must implement authenticate()")
