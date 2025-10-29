"""Abstract base class definition for CometD transport implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Set, List

from aiocometd.constants import ConnectionType, TransportState
from aiocometd.typing_utils import JsonObject



class Transport(ABC):
    """Defines the operations and lifecycle behavior of CometD transport classes.

    All transport implementations (e.g., LongPollingTransport, WebSocketTransport) must subclass this class and provide
    concrete implementations for the following methods and properties.
    """

    # -----------------------------------------------------------------------
    # Properties
    # -----------------------------------------------------------------------

    @property
    @abstractmethod
    def connection_type(self) -> ConnectionType:
        """Return the transport's connection type.

        Returns:
            ConnectionType: The CometD connection type (e.g., long-polling, websocket).
        """

    @property
    @abstractmethod
    def endpoint(self) -> str:
        """Return the CometD service endpoint URL.

        Returns:
            str: The URL of the CometD service endpoint.
        """

    @property
    @abstractmethod
    def client_id(self) -> Optional[str]:
        """Return the client ID assigned by the server.

        Returns:
            Optional[str]: The client ID if assigned, otherwise ``None``.
        """

    @property
    @abstractmethod
    def state(self) -> TransportState:
        """Return the current state of the transport.

        Returns:
            TransportState: The current transport state (e.g., CONNECTED, DISCONNECTED).
        """

    @property
    @abstractmethod
    def subscriptions(self) -> Set[str]:
        """Return the set of subscribed channels.

        Returns:
            Set[str]: Names of currently subscribed channels.
        """

    @property
    @abstractmethod
    def last_connect_result(self) -> Optional[JsonObject]:
        """Return the result of the last successful connect request.

        Returns:
            Optional[JsonObject]: The last connect response if available, otherwise ``None``.
        """

    @property
    @abstractmethod
    def reconnect_advice(self) -> JsonObject:
        """Return the server-provided reconnection advice.

        Returns:
            JsonObject: Parameters describing how and when to reconnect.
        """

    # -----------------------------------------------------------------------
    # Lifecycle and message operations
    # -----------------------------------------------------------------------

    @abstractmethod
    async def handshake(self, connection_types: List[ConnectionType]) -> JsonObject:
        """Perform the CometD handshake operation.

        Args:
            connection_types (List[ConnectionType]): The list of supported connection types.

        Returns:
            JsonObject: The handshake response message.

        Raises:
            TransportError: If the network request fails or the handshake cannot be completed.
        """

    @abstractmethod
    async def connect(self) -> JsonObject:
        """Establish a connection to the CometD server.

        The transport will attempt to maintain a continuous connection but returns as soon as the first connection
        succeeds.

        Returns:
            JsonObject: The response message of the first successful connection.

        Raises:
            TransportInvalidOperation: If the transport has no client ID or is not in a DISCONNECTED state.
            TransportError: If the network request fails.
        """

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the server.

        The disconnect message is only sent if the transport is currently connected.
        """

    @abstractmethod
    async def close(self) -> None:
        """Close the transport and release associated resources."""

    @abstractmethod
    async def subscribe(self, channel: str) -> JsonObject:
        """Subscribe to a specific channel.

        Args:
            channel (str): The name of the channel to subscribe to.

        Returns:
            JsonObject: The subscribe response message.

        Raises:
            TransportInvalidOperation: If the transport is not CONNECTED or CONNECTING.
            TransportError: If the network request fails.
        """

    @abstractmethod
    async def unsubscribe(self, channel: str) -> JsonObject:
        """Unsubscribe from a specific channel.

        Args:
            channel (str): The name of the channel to unsubscribe from.

        Returns:
            JsonObject: The unsubscribe response message.

        Raises:
            TransportInvalidOperation: If the transport is not CONNECTED or CONNECTING.
            TransportError: If the network request fails.
        """

    @abstractmethod
    async def publish(self, channel: str, data: JsonObject) -> JsonObject:
        """Publish data to the specified channel.

        Args:
            channel (str): The name of the channel to publish to.
            data (JsonObject): The data payload to send.

        Returns:
            JsonObject: The publish response message.

        Raises:
            TransportInvalidOperation: If the transport is not CONNECTED or CONNECTING.
            TransportError: If the network request fails.
        """

    @abstractmethod
    async def wait_for_state(self, state: TransportState) -> None:
        """Wait for the transport to enter a specific state.

        Args:
            state (TransportState): The expected state to wait for.
        """
