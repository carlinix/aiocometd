"""Functions for transport class registration and instantiation."""

from __future__ import annotations

from typing import Type, Callable, Any

from aiocometd.constants import ConnectionType
from aiocometd.exceptions import TransportInvalidOperation
from aiocometd.transports.abc import Transport

#: Registry of transport classes mapped by their connection type.
TRANSPORT_CLASSES: dict[ConnectionType, Type[Transport]] = {}


def register_transport(conn_type: ConnectionType) -> Callable[[Type[Transport]], Type[Transport]]:
    """Class decorator for registering transport classes.

    This decorator registers a transport implementation class for the specified
    connection type. It also dynamically sets the class property ``connection_type``
    to return the given ``conn_type``.

    Example:
        >>> @register_transport(ConnectionType.LONG_POLLING)
        ... class LongPollingTransport(TransportBase): # type: ignore[misc]
        ...     pass

    Args:
        conn_type (ConnectionType): The connection type associated with the transport.

    Returns:
        Callable[[Type[Transport]], Type[Transport]]: A decorator that registers the class
        and returns it unmodified.
    """

    def decorator(cls: Type[Transport]) -> Type[Transport]:
        TRANSPORT_CLASSES[conn_type] = cls

        @property
        def connection_type(self: Transport) -> ConnectionType:
            """Return the connection type associated with this transport."""
            return conn_type

        # TODO: spend some time in future to fix it.
        cls.connection_type = connection_type
        return cls

    return decorator


def create_transport(connection_type: ConnectionType, *args: Any, **kwargs: Any) -> Transport:
    """Create a transport instance for the specified connection type.

    Looks up the registered transport class for the given connection type and
    returns an initialized instance.

    Args:
        connection_type (ConnectionType): The desired connection type.
        *args: Positional arguments passed to the transport constructor.
        **kwargs: Keyword arguments passed to the transport constructor.

    Returns:
        Transport: An instance of the registered transport implementation.

    Raises:
        TransportInvalidOperation: If no transport class is registered for the given connection type.
    """
    if connection_type not in TRANSPORT_CLASSES:
        raise TransportInvalidOperation(
            f"There is no transport registered for connection type {connection_type!r}"
        )

    return TRANSPORT_CLASSES[connection_type](*args, **kwargs) # type: ignore[unused-args]
