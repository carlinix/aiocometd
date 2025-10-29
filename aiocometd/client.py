"""CometD asynchronous client implementation."""

from __future__ import annotations

import asyncio
import json
import logging
import reprlib
from collections import abc
from contextlib import suppress
from types import TracebackType
from typing import (
    Any,
    AsyncIterator,
    List,
    Optional,
    Set,
    Type,
    Union,
)

import aiohttp

from aiocometd.constants import (
    DEFAULT_CONNECTION_TYPE,
    ConnectionType,
    MetaChannel,
    SERVICE_CHANNEL_PREFIX,
    TransportState,
)
from aiocometd.exceptions import (
    ClientError,
    ClientInvalidOperation,
    ServerError,
    TransportTimeoutError,
)
from aiocometd.extensions import AuthExtension, Extension
from aiocometd.transports import create_transport
from aiocometd.transports.abc import Transport
from aiocometd.typing_utils import (
    ConnectionTypeSpec,
    JsonDumper,
    JsonLoader,
    JsonObject,
    SSLValidationMode,
)
from aiocometd.utils import is_server_error_message

LOGGER = logging.getLogger(__name__)


class Client:
    """Asynchronous CometD client.

    This class manages a connection to a CometD server using one or more
    transport protocols (e.g., WebSocket or long-polling).

    Attributes:
        url: CometD service URL.
        connection_timeout: Time (in seconds) to wait for reconnection.
        ssl: SSL validation mode.
        extensions: Optional list of protocol extensions.
        auth: Optional authentication extension.
        _transport: The currently active transport.
        _incoming_queue: Queue for incoming messages.
    """

    _SERVER_ERROR_MESSAGES = {
        MetaChannel.HANDSHAKE: "Handshake request failed.",
        MetaChannel.CONNECT: "Connect request failed.",
        MetaChannel.DISCONNECT: "Disconnect request failed.",
        MetaChannel.SUBSCRIBE: "Subscribe request failed.",
        MetaChannel.UNSUBSCRIBE: "Unsubscribe request failed.",
    }

    _DEFAULT_CONNECTION_TYPES = [ConnectionType.WEBSOCKET, ConnectionType.LONG_POLLING]
    _HTTP_SESSION_CLOSE_TIMEOUT = 0.25

    def __init__(
        self,
        url: str,
        connection_types: Optional[ConnectionTypeSpec] = None,
        *,
        connection_timeout: Union[int, float] = 10.0,
        ssl: Optional[SSLValidationMode] = None,
        max_pending_count: int = 100,
        extensions: Optional[List[Extension]] = None,
        auth: Optional[AuthExtension] = None,
        json_dumps: JsonDumper = json.dumps,
        json_loads: JsonLoader = json.loads,
    ) -> None:
        """Initialize a CometD client.

        Args:
            url: CometD service URL.
            connection_types: Preferred transport connection types.
            connection_timeout: Max time (seconds) to re-establish connections.
            ssl: SSL validation mode or context.
            max_pending_count: Maximum prefetched messages in the queue.
            extensions: Optional list of protocol extensions.
            auth: Optional authentication extension.
            json_dumps: JSON serialization function.
            json_loads: JSON deserialization function.
        """
        self.url = url
        if isinstance(connection_types, ConnectionType):
            self._connection_types = [connection_types]
        elif isinstance(connection_types, abc.Iterable):
            self._connection_types = list(connection_types)
        else:
            self._connection_types = self._DEFAULT_CONNECTION_TYPES

        self._incoming_queue: Optional[asyncio.Queue[JsonObject]] = None
        self._transport: Optional[Transport] = None
        self._closed = True
        self.connection_timeout = connection_timeout
        self.ssl = ssl
        self._max_pending_count = max_pending_count
        self.extensions = extensions
        self.auth = auth
        self._json_dumps = json_dumps
        self._json_loads = json_loads
        self._http_session: Optional[aiohttp.ClientSession] = None

    def __repr__(self) -> str:
        """Return a concise developer-friendly representation."""
        return (
            f"{self.__class__.__name__}("
            f"url={reprlib.repr(self.url)}, "
            f"connection_types={reprlib.repr(self._connection_types)}, "
            f"connection_timeout={self.connection_timeout!r}, "
            f"ssl={self.ssl!r}, "
            f"max_pending_count={self._max_pending_count!r}, "
            f"extensions={self.extensions!r}, "
            f"auth={self.auth!r})"
        )

    @property
    def closed(self) -> bool:
        """Return whether the client is closed."""
        return self._closed

    @property
    def subscriptions(self) -> Set[str]:
        """Return the set of active subscriptions."""
        return self._transport.subscriptions if self._transport else set()

    @property
    def connection_type(self) -> Optional[ConnectionType]:
        """Return the active connection type if the client is open."""
        return self._transport.connection_type if self._transport else None

    @property
    def pending_count(self) -> int:
        """Return the number of unconsumed incoming messages."""
        return self._incoming_queue.qsize() if self._incoming_queue else 0

    @property
    def has_pending_messages(self) -> bool:
        """Return True if there are unconsumed messages."""
        return self.pending_count > 0

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Return an active HTTP session, creating one if needed."""
        if self._http_session is None:
            self._http_session = aiohttp.ClientSession(json_serialize=self._json_dumps)
        return self._http_session

    async def _close_http_session(self) -> None:
        """Close the HTTP session gracefully."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            await asyncio.sleep(self._HTTP_SESSION_CLOSE_TIMEOUT)

    def _pick_connection_type(self, connection_types: List[str]) -> Optional[ConnectionType]:
        """Select the most preferred supported connection type."""
        available = {ConnectionType(t) for t in connection_types if t in ConnectionType._value2member_map_}
        intersection = list(set(available) & set(self._connection_types))
        return min(intersection, key=self._connection_types.index) if intersection else None

    async def _negotiate_transport(self) -> Transport:
        """Negotiate and create the appropriate transport."""
        self._incoming_queue = asyncio.Queue(maxsize=self._max_pending_count)
        http_session = await self._get_http_session()

        transport = create_transport(
            DEFAULT_CONNECTION_TYPE,
            url=self.url,
            incoming_queue=self._incoming_queue,
            ssl=self.ssl,
            extensions=self.extensions,
            auth=self.auth,
            json_dumps=self._json_dumps,
            json_loads=self._json_loads,
            http_session=http_session,
        )

        try:
            response = await transport.handshake(self._connection_types)
            self._verify_response(response)
            LOGGER.info("Server supports connection types: %r", response["supportedConnectionTypes"])

            chosen_type = self._pick_connection_type(response["supportedConnectionTypes"])
            if not chosen_type:
                raise ClientError("Server offers no supported connection types.")

            if transport.connection_type != chosen_type:
                client_id = transport.client_id
                advice = transport.reconnect_advice
                await transport.close()
                transport = create_transport(
                    chosen_type,
                    url=self.url,
                    incoming_queue=self._incoming_queue,
                    client_id=client_id,
                    ssl=self.ssl,
                    extensions=self.extensions,
                    auth=self.auth,
                    json_dumps=self._json_dumps,
                    json_loads=self._json_loads,
                    reconnect_advice=advice,
                    http_session=http_session,
                )

            return transport
        except Exception:
            await transport.close()
            await self._close_http_session()
            raise

    async def open(self) -> None:
        """Establish a connection to the CometD server."""
        if not self.closed:
            raise ClientInvalidOperation("Client is already open.")

        LOGGER.info("Opening client with connection types %r", [t.value for t in self._connection_types])
        self._transport = await self._negotiate_transport()

        response = await self._transport.connect()
        self._verify_response(response)
        self._closed = False

        LOGGER.info("Client opened using connection type %r", self.connection_type.value if self.connection_type else "?")

    async def close(self) -> None:
        """Close the connection gracefully."""
        if self.closed:
            return

        if self.pending_count:
            LOGGER.warning("Closing client with %s pending messages...", self.pending_count)
        else:
            LOGGER.info("Closing client...")

        try:
            if self._transport:
                await self._transport.disconnect()
                await self._transport.close()
            await self._close_http_session()
        finally:
            self._closed = True
            LOGGER.info("Client closed.")

    async def subscribe(self, channel: str) -> None:
        """Subscribe to a specific channel."""
        if self.closed:
            raise ClientInvalidOperation("Cannot subscribe while client is closed.")
        await self._check_server_disconnected()

        assert self._transport
        response = await self._transport.subscribe(channel)
        self._verify_response(response)
        LOGGER.info("Subscribed to channel %s", channel)

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a specific channel."""
        if self.closed:
            raise ClientInvalidOperation("Cannot unsubscribe while client is closed.")
        await self._check_server_disconnected()

        assert self._transport
        response = await self._transport.unsubscribe(channel)
        self._verify_response(response)
        LOGGER.info("Unsubscribed from channel %s", channel)

    async def publish(self, channel: str, data: JsonObject) -> JsonObject:
        """Publish a message to a channel."""
        if self.closed:
            raise ClientInvalidOperation("Cannot publish while client is closed.")
        await self._check_server_disconnected()

        assert self._transport
        response = await self._transport.publish(channel, data)
        self._verify_response(response)
        return response

    def _verify_response(self, response: JsonObject) -> None:
        """Raise a ServerError if the response is not successful."""
        if is_server_error_message(response):
            self._raise_server_error(response)

    def _raise_server_error(self, response: JsonObject) -> None:
        """Raise a server-side error based on the response."""
        channel = response.get("channel")
        message = self._SERVER_ERROR_MESSAGES.get(channel)
        if not message:
            message = "Service request failed." if channel.startswith(SERVICE_CHANNEL_PREFIX) else "Publish request failed."
        raise ServerError(message, response)

    async def receive(self) -> JsonObject:
        """Wait for the next message from the server."""
        if not self.closed or self.has_pending_messages:
            response = await self._get_message(self.connection_timeout)
            self._verify_response(response)
            return response
        raise ClientInvalidOperation("Client is closed and no messages remain.")

    async def __aiter__(self) -> AsyncIterator[JsonObject]:
        """Iterate asynchronously over incoming messages."""
        while True:
            try:
                yield await self.receive()
            except ClientInvalidOperation:
                break

    async def __aenter__(self) -> Client:
        """Enter the async context manager."""
        try:
            await self.open()
        except Exception:
            await self.close()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Exit the async context and close the client."""
        await self.close()

    async def _get_message(self, connection_timeout: Union[int, float]) -> JsonObject:
        """Wait for the next available message or raise a timeout."""
        tasks: List[asyncio.Future[Any]] = []

        if connection_timeout:
            timeout_task = asyncio.ensure_future(self._wait_connection_timeout(connection_timeout))
            tasks.append(timeout_task)

        assert self._incoming_queue is not None

        get_task = asyncio.ensure_future(self._incoming_queue.get(),
                                         )
        tasks.append(get_task)

        assert self._transport is not None

        server_disconnected_task = asyncio.ensure_future(
            self._transport.wait_for_state(TransportState.SERVER_DISCONNECTED)
        )
        tasks.append(server_disconnected_task)
        try:
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()

            if get_task in done:
                return get_task.result()

            if server_disconnected_task in done:
                await self.close()
                raise ServerError("Connection closed by the server",
                                  self._transport.last_connect_result)
            raise TransportTimeoutError("Lost connection with the server.")
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            raise

    async def _wait_connection_timeout(self, timeout: Union[int, float]) -> None:
        """Monitor the connection timeout window."""
        assert self._transport
        while True:
            await self._transport.wait_for_state(TransportState.CONNECTING)
            try:
                await asyncio.wait_for(
                    self._transport.wait_for_state(TransportState.CONNECTED),
                    timeout,
                )
            except asyncio.TimeoutError:
                break

    async def _check_server_disconnected(self) -> None:
        """Raise an error if the transport has been disconnected by the server."""
        if self._transport and self._transport.state == TransportState.SERVER_DISCONNECTED:
            await self.close()
            raise ServerError("Connection closed by the server", self._transport.last_connect_result)
