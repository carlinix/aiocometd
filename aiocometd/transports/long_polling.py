"""Long-polling transport implementation for CometD."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

from aiocometd.constants import ConnectionType
from aiocometd.exceptions import TransportError
from aiocometd.transports.base import TransportBase
from aiocometd.transports.registry import register_transport
from aiocometd.typing_utils import Headers, JsonObject, Payload

LOGGER = logging.getLogger(__name__)


@register_transport(ConnectionType.LONG_POLLING)
class LongPollingTransport(TransportBase):
    """CometD long-polling transport implementation.

    This transport maintains a limited number of concurrent HTTP connections to the CometD server and uses
    standard HTTP POST requests to exchange JSON payloads. Each request blocks until the server returns a
    response or the timeout expires.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the long-polling transport.

        Args:
            **kwargs: Keyword arguments forwarded to the :class:`TransportBase` constructor.
        """
        super().__init__(**kwargs)
        #: Semaphore to limit concurrent HTTP requests to two.
        self._http_semaphore = asyncio.Semaphore(2)

    async def _send_final_payload(
        self, payload: Payload, *, headers: Headers
    ) -> JsonObject:
        """Send the final payload to the server and return the response message.

        This method performs the actual HTTP POST request used by the long-polling transport. It handles
        connection errors, converts responses to JSON, and ensures that the response includes a matching
        CometD message for the first element of the sent payload.

        Args:
            payload (Payload): A list of messages to send to the CometD server.
            headers (Headers): HTTP headers to include in the outgoing request.

        Returns:
            JsonObject: The CometD response message matching the first message in the payload.

        Raises:
            TransportError: If the HTTP request fails or no matching response is received.
        """
        try:
            session = self._http_session
            request_timeout = self.request_timeout
            timeout = (
                aiohttp.ClientTimeout(total=request_timeout)
                if request_timeout is not None
                else None
            )
            async with self._http_semaphore:
                response = await session.post(
                    self._url,
                    json=payload,
                    ssl=self.ssl if self.ssl is not None else True,
                    headers=headers,
                    timeout=timeout,
                )
            response_payload = await response.json(loads=self._json_loads)
            headers = response.headers
        except aiohttp.client_exceptions.ClientError as error:
            LOGGER.warning("Failed to send payload: %s", error)
            raise TransportError(str(error)) from error

        response_message = await self._consume_payload(
            response_payload,
            headers=headers,
            find_response_for=payload[0],
        )

        if response_message is None:
            error_message = (
                "No response message received for the first message in the payload."
            )
            LOGGER.warning(error_message)
            raise TransportError(error_message)

        return response_message
