"""Functions for transport class registration and instantiation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from aiocometd.constants import ConnectionType
from aiocometd.exceptions import TransportInvalidOperation
from aiocometd.transports.abc import Transport

#: Registry of transport classes mapped by their connection type.
TRANSPORT_CLASSES: dict[ConnectionType, type[Transport]] = {}
TransportType = TypeVar("TransportType", bound=type[Transport])


def register_transport(
    conn_type: ConnectionType,
) -> Callable[[TransportType], TransportType]:
    """Class decorator for registering transport classes.

    This decorator registers a transport implementation class for the specified
    connection type and sets its ``connection_type`` class attribute.

    Example:
        >>> @register_transport(ConnectionType.LONG_POLLING)
        ... class LongPollingTransport(TransportBase):  # type: ignore[misc]
        ...     pass

    Args:
        conn_type (ConnectionType): The connection type associated with the transport.

    Returns:
        Callable[[Type[Transport]], Type[Transport]]: A decorator that registers the class
        and returns it unmodified.
    """

    def decorator(cls: TransportType) -> TransportType:
        TRANSPORT_CLASSES[conn_type] = cls
        cls.connection_type = conn_type
        return cls

    return decorator


def create_transport(
    connection_type: ConnectionType, *args: Any, **kwargs: Any
) -> Transport:
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

    return TRANSPORT_CLASSES[connection_type](*args, **kwargs)
