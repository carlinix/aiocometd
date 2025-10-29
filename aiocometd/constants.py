"""Transport-related constants and definitions for CometD protocol."""

from __future__ import annotations

from enum import Enum, StrEnum, auto, unique
from dataclasses import dataclass, asdict
from typing import Optional


# ---------------------------------------------------------------------------
# Connection Types
# ---------------------------------------------------------------------------

@unique
class ConnectionType(StrEnum):
    """CometD connection types."""
    LONG_POLLING = "long-polling"
    WEBSOCKET = "websocket"


DEFAULT_CONNECTION_TYPE: ConnectionType = ConnectionType.LONG_POLLING

# ---------------------------------------------------------------------------
# Channel Prefixes
# ---------------------------------------------------------------------------

META_CHANNEL_PREFIX: str = "/meta/"
SERVICE_CHANNEL_PREFIX: str = "/service/"

# ---------------------------------------------------------------------------
# Meta Channels
# ---------------------------------------------------------------------------

@unique
class MetaChannel(StrEnum):
    """CometD meta channel names."""
    HANDSHAKE = f"{META_CHANNEL_PREFIX}handshake"
    CONNECT = f"{META_CHANNEL_PREFIX}connect"
    DISCONNECT = f"{META_CHANNEL_PREFIX}disconnect"
    SUBSCRIBE = f"{META_CHANNEL_PREFIX}subscribe"
    UNSUBSCRIBE = f"{META_CHANNEL_PREFIX}unsubscribe"


# ---------------------------------------------------------------------------
# Transport State
# ---------------------------------------------------------------------------

@unique
class TransportState(Enum):
    """Describes a transport object's state."""
    DISCONNECTED = auto()
    SERVER_DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    DISCONNECTING = auto()


# ---------------------------------------------------------------------------
# Message Templates
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class HandshakeMessage:
    channel: MetaChannel = MetaChannel.HANDSHAKE
    version: str = "1.0"
    supportedConnectionTypes: Optional[list[ConnectionType]] = None
    minimumVersion: str = "1.0"
    id: Optional[str] = None


@dataclass(frozen=True, slots=True)
class ConnectMessage:
    channel: MetaChannel = MetaChannel.CONNECT
    clientId: Optional[str] = None
    connectionType: Optional[ConnectionType] = None
    id: Optional[str] = None


@dataclass(frozen=True, slots=True)
class DisconnectMessage:
    channel: MetaChannel = MetaChannel.DISCONNECT
    clientId: Optional[str] = None
    id: Optional[str] = None


@dataclass(frozen=True, slots=True)
class SubscribeMessage:
    channel: MetaChannel = MetaChannel.SUBSCRIBE
    clientId: Optional[str] = None
    subscription: Optional[str] = None
    id: Optional[str] = None


@dataclass(frozen=True, slots=True)
class UnsubscribeMessage:
    channel: MetaChannel = MetaChannel.UNSUBSCRIBE
    clientId: Optional[str] = None
    subscription: Optional[str] = None
    id: Optional[str] = None


@dataclass(frozen=True, slots=True)
class PublishMessage:
    channel: Optional[str] = None
    clientId: Optional[str] = None
    data: Optional[dict] = None
    id: Optional[str] = None


# Optionally expose ready-to-use template dicts
HANDSHAKE_MESSAGE = asdict(HandshakeMessage())
CONNECT_MESSAGE = asdict(ConnectMessage())
DISCONNECT_MESSAGE = asdict(DisconnectMessage())
SUBSCRIBE_MESSAGE = asdict(SubscribeMessage())
UNSUBSCRIBE_MESSAGE = asdict(UnsubscribeMessage())
PUBLISH_MESSAGE = asdict(PublishMessage())
