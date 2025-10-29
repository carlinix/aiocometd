"""Base transport abstract class definition.

This module defines the base implementation of a CometD transport class. It provides
the common logic used by various concrete transport implementations such as
`LongPollingTransport` and `WebSocketTransport`.

Subclasses must implement the `_send_final_payload()` method and define their
`connection_type` (usually via the `@register_transport` decorator).
"""

from __future__ import annotations

import asyncio
import json
import logging
from abc import abstractmethod
from contextlib import suppress
from typing import Any, Awaitable, ClassVar, Optional, Union, List, Set

import aiohttp

from aiocometd.constants import (
    ConnectionType,
    MetaChannel,
    TransportState,
    HANDSHAKE_MESSAGE,
    CONNECT_MESSAGE,
    DISCONNECT_MESSAGE,
    SUBSCRIBE_MESSAGE,
    UNSUBSCRIBE_MESSAGE,
    PUBLISH_MESSAGE,
)
from aiocometd.utils import (
    defer,
    is_matching_response,
    is_auth_error_message,
    is_event_message,
)
from aiocometd.exceptions import TransportInvalidOperation, TransportError
from aiocometd.typing_utils import (
    SSLValidationMode,
    JsonObject,
    JsonLoader,
    JsonDumper,
    Headers,
    Payload,
)
from aiocometd.extensions import Extension, AuthExtension
from aiocometd.transports.abc import Transport

LOGGER = logging.getLogger(__name__)


class TransportBase(Transport):  # pylint: disable=too-many-instance-attributes
    """Base transport implementation.

    This class contains most of the common transport operations. Subclasses can use it
    as a foundation for implementing specific connection mechanisms (e.g., long-polling,
    websocket). It manages the handshake, connection lifecycle, message handling,
    reconnection logic, and extension hooks.

    Attributes:
        REQUEST_TIMEOUT_INCREASE_FACTOR (float): Factor by which to increase request
            timeouts to avoid premature timeouts due to network latency.
        connection_type (ClassVar[ConnectionType]): The transport's connection type,
            assigned via the `@register_transport` decorator.
    """

    REQUEST_TIMEOUT_INCREASE_FACTOR: float = 1.2
    connection_type: ClassVar[ConnectionType]

    def __init__(
        self,
        *,
        url: str,
        incoming_queue: asyncio.Queue[JsonObject],
        http_session: aiohttp.ClientSession,
        client_id: Optional[str] = None,
        reconnection_timeout: Union[int, float] = 1,
        ssl: Optional[SSLValidationMode] = None,
        extensions: Optional[List[Extension]] = None,
        auth: Optional[AuthExtension] = None,
        json_dumps: JsonDumper = json.dumps,
        json_loads: JsonLoader = json.loads,
        reconnect_advice: Optional[JsonObject] = None,
    ) -> None:
        """Initialize the base transport.

        Args:
            url: CometD service URL.
            incoming_queue: Queue for consuming incoming event messages.
            http_session: Shared aiohttp client session.
            client_id: Client ID assigned by the server, if available.
            reconnection_timeout: Delay (seconds) before retrying after failure.
            ssl: SSL validation mode (context, fingerprint, or bool).
            extensions: List of protocol extensions.
            auth: Authentication extension.
            json_dumps: JSON serializer function.
            json_loads: JSON deserializer function.
            reconnect_advice: Initial reconnect advice from the server.
        """
        self.incoming_queue = incoming_queue
        self._url = url
        self._http_session = http_session
        self._client_id = client_id
        self._message_id = 0
        self._reconnect_advice: JsonObject = reconnect_advice or {}
        self._subscriptions: Set[str] = set()
        self._subscribe_on_connect = False
        self._state_events = {state: asyncio.Event() for state in TransportState}
        self._state = TransportState.DISCONNECTED
        self._connect_task: Optional[asyncio.Future[JsonObject]] = None
        self._reconnect_timeout = reconnection_timeout
        self.ssl = ssl
        self._extensions = extensions or []
        self._auth = auth
        self._json_dumps = json_dumps
        self._json_loads = json_loads

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            LOGGER.debug("Created new event loop for TransportBase (no running loop detected).")

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------
    @property
    def endpoint(self) -> str:
        """Return the CometD service endpoint URL."""
        return self._url

    @property
    def client_id(self) -> Optional[str]:
        """Return the client ID assigned by the server."""
        return self._client_id

    @property
    def subscriptions(self) -> Set[str]:
        """Return the set of currently subscribed channels."""
        return self._subscriptions

    @property
    def last_connect_result(self) -> Optional[JsonObject]:
        """Return the result of the last connect operation."""
        if self._connect_task and self._connect_task.done():
            return self._connect_task.result()
        return None

    @property
    def reconnect_advice(self) -> JsonObject:
        """Return the reconnection advice parameters from the server."""
        return self._reconnect_advice

    @property
    def state(self) -> TransportState:
        """Return the current state of the transport."""
        return self._state

    @property
    def request_timeout(self) -> Optional[float]:
        """Return the effective request timeout, adjusted by the increase factor."""
        timeout = self.reconnect_advice.get("timeout")
        if isinstance(timeout, (int, float)):
            return (timeout / 1000) * self.REQUEST_TIMEOUT_INCREASE_FACTOR
        return None

    @property
    def connection_type(self) -> ConnectionType:  # pragma: no cover
        """Return the transport's connection type.

        This placeholder is overridden in subclasses registered via
        `@register_transport`.
        """
        return None  # type: ignore[return-value]

    @property
    def _state(self) -> TransportState:
        """Return the current transport state, defaulting to DISCONNECTED."""
        return self.__dict__.get("_state", TransportState.DISCONNECTED)

    # -------------------------------------------------------------------------
    # Setters
    # -------------------------------------------------------------------------
    @_state.setter
    def _state(self, value: TransportState) -> None:
        """Update transport state and signal any waiting coroutines."""
        self._set_state_event(self._state, value)
        self.__dict__["_state"] = value


    # -------------------------------------------------------------------------
    # State management
    # -------------------------------------------------------------------------
    def _set_state_event(self, old_state: TransportState, new_state: TransportState) -> None:
        """Update asyncio.Event flags for state transitions."""
        if new_state != old_state:
            self._state_events[old_state].clear()
            self._state_events[new_state].set()

    async def wait_for_state(self, state: TransportState) -> None:
        """Block until the transport enters the given state."""
        await self._state_events[state].wait()

    async def handshake(self, connection_types: List[ConnectionType]) -> JsonObject:
        """Perform the handshake operation.

        Args:
            connection_types: List of supported connection types.

        Returns:
            The handshake response message.

        Raises:
            TransportError: If the network request fails.
        """
        self._message_id = 0
        connection_types = list(connection_types)
        if self.connection_type not in connection_types:
            connection_types.append(self.connection_type)

        response = await self._send_message(
            HANDSHAKE_MESSAGE.copy(),
            supportedConnectionTypes=[ct.value for ct in connection_types],
        )
        if response.get("successful"):
            self._client_id = response.get("clientId")
            self._subscribe_on_connect = True
        return response

    def _finalize_message(self, message: JsonObject) -> None:
        """Attach ID, client ID, and connection type to the outgoing message."""
        if "id" in message:
            message["id"] = str(self._message_id)
            self._message_id += 1
        if "clientId" in message:
            message["clientId"] = self.client_id
        if "connectionType" in message:
            message["connectionType"] = self.connection_type.value

    def _finalize_payload(self, payload: Union[JsonObject, Payload]) -> None:
        """Finalize a single message or list of messages."""
        if isinstance(payload, list):
            for msg in payload:
                self._finalize_message(msg)
        else:
            self._finalize_message(payload)

    async def _send_message(self, message: JsonObject, **kwargs: Any) -> JsonObject:
        """Send a single message and return its response."""
        message.update(kwargs)
        return await self._send_payload_with_auth([message])

    async def _send_payload_with_auth(self, payload: Payload) -> JsonObject:
        """Send a payload, retrying once if authentication fails."""
        response = await self._send_payload(payload)
        if self._auth and is_auth_error_message(response):
            await self._auth.authenticate()
            return await self._send_payload(payload)
        return response

    async def _send_payload(self, payload: Payload) -> JsonObject:
        """Finalize and send a payload to the server."""
        self._finalize_payload(payload)
        headers: Headers = {}
        await self._process_outgoing_payload(payload, headers)
        return await self._send_final_payload(payload, headers=headers)

    @abstractmethod
    async def _send_final_payload(self, payload: Payload, *, headers: Headers) -> JsonObject:
        """Send the finalized payload and return the response.

        Subclasses must implement this method.

        Args:
            payload: List of messages to send.
            headers: HTTP headers to include.

        Returns:
            The response message corresponding to the first message.
        """

    # -------------------------------------------------------------------------
    # Extension hooks
    # -------------------------------------------------------------------------
    async def _process_outgoing_payload(self, payload: Payload, headers: Headers) -> None:
        """Apply outgoing extensions and authentication headers."""
        for ext in self._extensions:
            await ext.outgoing(payload, headers)
        if self._auth:
            await self._auth.outgoing(payload, headers)

    async def _process_incoming_payload(self, payload: Payload, headers: Optional[Headers] = None) -> None:
        """Apply incoming extensions to the payload."""
        if self._auth:
            await self._auth.incoming(payload, headers)
        for ext in self._extensions:
            await ext.incoming(payload, headers)

    # -------------------------------------------------------------------------
    # Payload consumption
    # -------------------------------------------------------------------------
    async def _consume_message(self, response_message: JsonObject) -> None:
        """Queue event messages for consumption by clients."""
        if is_event_message(response_message):
            await self.incoming_queue.put(response_message)

    async def _consume_payload(
        self,
        payload: Payload,
        *,
        headers: Optional[Headers] = None,
        find_response_for: Optional[JsonObject] = None,
    ) -> Optional[JsonObject]:
        """Process the received payload, updating state and returning relevant responses."""
        await self._process_incoming_payload(payload, headers)
        result: Optional[JsonObject] = None

        for message in payload:
            if "advice" in message:
                self._reconnect_advice = message["advice"]

            self._update_subscriptions(message)

            if result is None and is_matching_response(message, find_response_for):
                result = message
                continue

            await self._consume_message(message)

        return result


    def _update_subscriptions(self, response_message: JsonObject) -> None:
        """Update internal subscriptions based on a subscribe/unsubscribe response."""
        channel = response_message.get("channel")
        sub = response_message.get("subscription")

        if channel == MetaChannel.SUBSCRIBE:
            if response_message.get("successful") and sub not in self._subscriptions:
                self._subscriptions.add(sub)
            elif not response_message.get("successful") and sub in self._subscriptions:
                self._subscriptions.remove(sub)

        elif channel == MetaChannel.UNSUBSCRIBE:
            if response_message.get("successful") and sub in self._subscriptions:
                self._subscriptions.remove(sub)

    # -------------------------------------------------------------------------
    # Connection lifecycle
    # -------------------------------------------------------------------------
    def _start_connect_task(self, coro: Awaitable[JsonObject]) -> Awaitable[JsonObject]:
        """Schedule and track a background connect coroutine."""
        self._connect_task = asyncio.ensure_future(coro)
        self._connect_task.add_done_callback(self._connect_done)
        return self._connect_task

    async def _stop_connect_task(self) -> None:
        """Cancel and await any existing connection task."""
        if self._connect_task and not self._connect_task.done():
            self._connect_task.cancel()
            await asyncio.wait([self._connect_task])

    async def connect(self) -> JsonObject:
        """Initiate or resume the connection loop."""
        if not self.client_id:
            raise TransportInvalidOperation("Handshake required before connecting.")
        if self.state not in {TransportState.DISCONNECTED, TransportState.SERVER_DISCONNECTED}:
            raise TransportInvalidOperation("Must disconnect before reconnecting.")

        self._state = TransportState.CONNECTING
        return await self._start_connect_task(self._connect())

    async def _connect(self) -> JsonObject:
        """Execute the connect cycle and resubscribe if necessary."""
        payload = [CONNECT_MESSAGE.copy()]
        if self._subscribe_on_connect and self.subscriptions:
            for sub in self.subscriptions:
                msg = SUBSCRIBE_MESSAGE.copy()
                msg["subscription"] = sub  # type: ignore
                payload.append(msg)
        result = await self._send_payload_with_auth(payload)
        self._subscribe_on_connect = not result.get("successful", False)
        return result

    def _connect_done(self, future: asyncio.Future[JsonObject]) -> None:
        """Callback invoked when a connect or handshake operation completes."""
        reconnect_advice = "retry"
        reconnect_timeout = self.reconnect_advice.get("interval")
        try:
            result: Union[JsonObject, Exception] = future.result()
            if isinstance(result, dict) and not result.get("successful", True):
                reconnect_advice = result.get("advice", {}).get("reconnect", reconnect_advice)
            self._state = TransportState.CONNECTED
        except Exception as error:  # pylint: disable=broad-except
            result = error
            reconnect_timeout = self._reconnect_timeout
            if self.state != TransportState.DISCONNECTING:
                self._state = TransportState.CONNECTING

        LOGGER.debug("Connect task finished with: %r", result)
        if self.state != TransportState.DISCONNECTING:
            self._follow_advice(reconnect_advice, reconnect_timeout)

    def _follow_advice(self, reconnect_advice: str, reconnect_timeout: Optional[Union[int, float]]) -> None:
        """Follow server reconnection advice (handshake, retry, or stop)."""
        if reconnect_advice == "handshake":
            handshake_coro = defer(self.handshake, delay=reconnect_timeout)
            self._start_connect_task(handshake_coro([self.connection_type]))
        elif reconnect_advice == "retry":
            connect_coro = defer(self._connect, delay=reconnect_timeout)
            self._start_connect_task(connect_coro())
        else:
            LOGGER.warning("No reconnect advice provided; no further operations will be scheduled.")
            self._state = TransportState.SERVER_DISCONNECTED

    async def disconnect(self) -> None:
        """Gracefully disconnect from the server."""
        try:
            should_send_message = self.state == TransportState.CONNECTED
            self._state = TransportState.DISCONNECTING
            await self._stop_connect_task()

            if should_send_message:
                with suppress(TransportError):
                    await self._send_message(DISCONNECT_MESSAGE.copy())
        finally:
            self._state = TransportState.DISCONNECTED

    async def close(self) -> None:
        """Close the transport and release all allocated resources."""
        pass

    async def subscribe(self, channel: str) -> JsonObject:
        """Subscribe to a channel."""
        if self.state not in {TransportState.CONNECTING, TransportState.CONNECTED}:
            raise TransportInvalidOperation("Cannot subscribe before connecting.")
        return await self._send_message(SUBSCRIBE_MESSAGE.copy(), subscription=channel)

    async def unsubscribe(self, channel: str) -> JsonObject:
        """Unsubscribe from a channel."""
        if self.state not in {TransportState.CONNECTING, TransportState.CONNECTED}:
            raise TransportInvalidOperation("Cannot unsubscribe before connecting.")
        return await self._send_message(UNSUBSCRIBE_MESSAGE.copy(), subscription=channel)

    async def publish(self, channel: str, data: JsonObject) -> JsonObject:
        """Publish a message to the specified channel."""
        if self.state not in {TransportState.CONNECTING, TransportState.CONNECTED}:
            raise TransportInvalidOperation("Cannot publish before connecting.")
        return await self._send_message(PUBLISH_MESSAGE.copy(), channel=channel, data=data)
