"""WebSocket transport class definition.

Implements a CometD transport over WebSocket. This class provides asynchronous
message sending, receiving, and exchange tracking between the client and server.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import Any, Awaitable, Callable, Dict, Optional, cast, AsyncContextManager

import aiohttp
import aiohttp.client_ws

from aiocometd.constants import ConnectionType
from aiocometd.exceptions import TransportError, TransportConnectionClosed
from aiocometd.typing_utils import JsonObject
from aiocometd.transports.registry import register_transport
from aiocometd.transports.base import TransportBase, Payload, Headers

LOGGER = logging.getLogger(__name__)

#: Asynchronous factory function that returns an aiohttp.ClientSession
AsyncSessionFactory = Callable[[], Awaitable[aiohttp.ClientSession]]
#: WebSocket client response type alias
WebSocket = aiohttp.client_ws.ClientWebSocketResponse
#: Asynchronous context manager for a WebSocket
WebSocketContextManager = AsyncContextManager[WebSocket]


class WebSocketFactory:
    """Factory class for creating and managing aiohttp WebSocket connections.

    This helper allows the creation of reusable WebSocket objects without explicit
    context-manager syntax. It ensures proper cleanup when connections are closed.
    """

    def __init__(self, http_session: aiohttp.ClientSession) -> None:
        """Initialize the factory with a given HTTP session.

        Args:
            http_session: The aiohttp client session used to create WebSocket connections.
        """
        self._http_session = http_session
        self._context: Optional[WebSocketContextManager] = None
        self._socket: Optional[WebSocket] = None

    async def close(self) -> None:
        """Close any active WebSocket connection."""
        with suppress(Exception):
            await self._exit()

    async def __call__(self, *args: Any, **kwargs: Any) -> WebSocket:
        """Create or reuse an existing WebSocket connection.

        Args:
            *args: Positional arguments for `aiohttp.ClientSession.ws_connect`.
            **kwargs: Keyword arguments for `aiohttp.ClientSession.ws_connect`.

        Returns:
            The active WebSocket object.
        """
        # Clean up a previously closed socket
        if self._socket is not None and self._socket.closed:
            await self._exit()

        # Create a new connection if no valid socket exists
        if self._socket is None:
            self._socket = await self._enter(*args, **kwargs)

        return self._socket

    async def _enter(self, *args: Any, **kwargs: Any) -> WebSocket:
        """Enter the WebSocket context and return a connected socket."""
        self._context = self._http_session.ws_connect(*args, **kwargs)
        return await self._context.__aenter__()

    async def _exit(self) -> None:
        """Exit the WebSocket context, closing the connection."""
        if self._context:
            await self._context.__aexit__(None, None, None)
            self._socket = self._context = None


@register_transport(ConnectionType.WEBSOCKET)
class WebSocketTransport(TransportBase):
    """WebSocket transport implementation for CometD communication."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the WebSocket transport."""
        super().__init__(**kwargs)
        self._socket_factory = WebSocketFactory(self._http_session)
        self._pending_exchanges: Dict[int, asyncio.Future[JsonObject]] = {}
        self._receive_task: Optional[asyncio.Task[None]] = None

    # -------------------------------------------------------------------------
    # Socket management
    # -------------------------------------------------------------------------
    async def _reset_socket(self) -> None:
        """Close and recreate the WebSocket factory."""
        await self._socket_factory.close()
        self._socket_factory = WebSocketFactory(self._http_session)

    async def _get_socket(self, headers: Headers) -> WebSocket:
        """Obtain or create a WebSocket connection.

        Args:
            headers: HTTP headers to send during connection upgrade.

        Returns:
            The active WebSocket object.
        """
        return await self._socket_factory(
            self.endpoint,
            ssl=self.ssl,
            headers=headers,
            receive_timeout=self.request_timeout,
            autoping=True,
        )

    # -------------------------------------------------------------------------
    # Message exchange
    # -------------------------------------------------------------------------
    def _create_exchange_future(self, payload: Payload) -> asyncio.Future[JsonObject]:
        """Create a future representing a client-server message exchange.

        The first message's ID in the payload is used as the key to resolve responses.

        Args:
            payload: Outgoing payload sent to the server.

        Returns:
            A future that will yield the server's response message.
        """
        future: asyncio.Future[JsonObject] = asyncio.Future()
        self._pending_exchanges[payload[0]["id"]] = future
        return future

    def _set_exchange_results(self, response_payload: Payload) -> None:
        """Resolve pending exchange futures with matching response messages."""
        for response_message in response_payload:
            if "id" not in response_message:
                continue
            message_id = response_message["id"]
            if message_id in self._pending_exchanges:
                exchange = self._pending_exchanges.pop(message_id)
                if not exchange.done():
                    exchange.set_result(response_message)

    def _set_exchange_errors(self, error: Exception) -> None:
        """Reject all pending exchanges with the given error."""
        for exchange in self._pending_exchanges.values():
            if not exchange.done():
                exchange.set_exception(error)
        self._pending_exchanges.clear()

    # -------------------------------------------------------------------------
    # Send and receive
    # -------------------------------------------------------------------------
    async def _send_final_payload(self, payload: Payload, *, headers: Headers) -> JsonObject:
        """Send the finalized payload over WebSocket.

        This method handles connection resets and reconnections on failure.

        Args:
            payload: List of messages to send.
            headers: HTTP headers for the WebSocket connection.

        Returns:
            The response message corresponding to the first message in the payload.

        Raises:
            TransportError: If the payload cannot be sent.
        """
        try:
            try:
                socket = await self._get_socket(headers)
                return await self._send_socket_payload(socket, payload)
            except asyncio.TimeoutError:
                await self._reset_socket()
                raise
            except TransportConnectionClosed:
                socket = await self._get_socket(headers)
                return await self._send_socket_payload(socket, payload)
        except aiohttp.client_exceptions.ClientError as error:
            LOGGER.warning("Failed to send payload: %s", error)
            raise TransportError(str(error)) from error

    async def _send_socket_payload(self, socket: WebSocket, payload: Payload) -> JsonObject:
        """Send a payload through an open WebSocket and await its response."""
        future = self._create_exchange_future(payload)
        try:
            await socket.send_json(payload, dumps=self._json_dumps)
        except Exception as error:
            self._set_exchange_errors(error)
            raise

        self._start_receive_task(socket)
        return await future

    # -------------------------------------------------------------------------
    # Receiving loop
    # -------------------------------------------------------------------------
    def _start_receive_task(self, socket: WebSocket) -> None:
        """Ensure the background receive task is running."""
        if self._receive_task is None:
            self._receive_task = self._loop.create_task(self._receive(socket))
            self._receive_task.add_done_callback(self._receive_done)

    async def _receive(self, socket: WebSocket) -> None:
        """Continuously receive messages from the WebSocket connection."""
        try:
            while True:
                response = await socket.receive()
                if response.type == aiohttp.WSMsgType.CLOSE:
                    raise TransportConnectionClosed("Received CLOSE message from server.")

                try:
                    response_payload = cast(Payload, response.json(loads=self._json_loads))
                except TypeError:
                    raise TransportError("Received invalid JSON payload from server.") from None

                await self._consume_payload(response_payload)
                self._set_exchange_results(response_payload)
        except Exception as error:
            self._set_exchange_errors(error)
            raise

    def _receive_done(self, future: asyncio.Task[None]) -> None:
        """Callback executed when the receive loop terminates."""
        try:
            result = future.result()
        except Exception as error:  # pylint: disable=broad-except
            result = error

        self._receive_task = None
        LOGGER.debug("Receive task finished with: %r", result)

    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------
    async def close(self) -> None:
        """Cancel the receive loop, close the socket, and release resources."""
        if self._receive_task is not None and not self._receive_task.done():
            self._receive_task.cancel()
            await asyncio.wait([self._receive_task])

        await self._socket_factory.close()
        await super().close()
