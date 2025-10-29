import asyncio
import unittest
from enum import Enum, unique
from unittest import mock

from aiocometd.client import Client
from aiocometd.exceptions import (
    ServerError,
    ClientInvalidOperation,
    TransportError,
    TransportTimeoutError,
    ClientError,
)
from aiocometd.constants import (
    DEFAULT_CONNECTION_TYPE,
    ConnectionType,
    MetaChannel,
    SERVICE_CHANNEL_PREFIX,
    TransportState,
)



@unique
class MockConnectionType(Enum):
    TYPE1 = "type1"
    TYPE2 = "type2"
    TYPE3 = "type3"
    TYPE4 = "type4"

class TestClient(unittest.IsolatedAsyncioTestCase):
    """Modernized Client tests (Python ≥3.11)."""

    def setUp(self):
        self.client = Client("")

    def test_init_with_loop(self):
        loop = object()
        with self.assertRaises(TypeError):
            Client(url=None, loop=loop)

    async def long_task(self, result, timeout=None):
        if timeout:
            await asyncio.sleep(timeout)
        if not isinstance(result, Exception):
            return result
        raise result

    def test_init_no_loop_argument(self):
        """Client should be constructible without explicit event loop."""
        client = Client(url=None)
        self.assertTrue(client.closed)

    async def test_client_uses_running_loop(self):
        """Client should use the currently running loop for async ops."""
        client = Client(url=None)
        # Trigger async call that relies on asyncio.get_running_loop()
        loop = asyncio.get_running_loop()
        session = await client._get_http_session()
        self.assertFalse(session.closed)
        current = asyncio.get_running_loop()
        self.assertIs(loop, current)
        await client._close_http_session()

    def test_init_with_no_connection_types(self):
        client = Client(url=None)
        self.assertEqual(
            client._connection_types,
            [ConnectionType.WEBSOCKET, ConnectionType.LONG_POLLING],
        )

    def test_init_with_connection_types_list(self):
        lst = [ConnectionType.LONG_POLLING, ConnectionType.WEBSOCKET]
        client = Client(url=None, connection_types=lst)
        self.assertEqual(client._connection_types, lst)

    def test_init_with_connection_type_value(self):
        ctype = ConnectionType.LONG_POLLING
        client = Client(url=None, connection_types=ctype)
        self.assertEqual(client._connection_types, [ctype])

    def test_subscriptions(self):
        self.client._transport = mock.MagicMock()
        self.client._transport.subscriptions = {"channel1", "channel2"}
        result = self.client.subscriptions
        self.assertEqual(result, self.client._transport.subscriptions)

    def test_subscriptions_empty_on_none_transport(self):
        self.client._transport = None
        result = self.client.subscriptions
        self.assertEqual(result, set())

    def test_connection_type(self):
        self.client._transport = mock.MagicMock()
        self.client._transport.connection_type = object()
        result = self.client.connection_type
        self.assertIs(result, self.client._transport.connection_type)

    def test_connection_type_none_on_no_transport(self):
        self.assertIsNone(self.client.connection_type)

    def test_closed(self):
        self.assertIs(self.client.closed, self.client._closed)

    def test_closed_read_only(self):
        with self.assertRaises(AttributeError):
            self.client.closed = False

    async def test_get_http_session(self):
        self.client._http_session = object()
        session = await self.client._get_http_session()
        self.assertEqual(session, self.client._http_session)

    @mock.patch("aiocometd.client.aiohttp.ClientSession")
    async def test_get_http_session_creates_session(self, client_session_cls):
        self.client._http_session = None
        session = object()
        client_session_cls.return_value = session
        result = await self.client._get_http_session()
        self.assertEqual(result, session)
        client_session_cls.assert_called_with(
            json_serialize=self.client._json_dumps
        )

    @mock.patch("aiocometd.client.asyncio")
    async def test_close_http_session(self, asyncio_mock):
        self.client._http_session = mock.MagicMock()
        self.client._http_session.closed = False
        self.client._http_session.close = mock.AsyncMock()
        asyncio_mock.sleep = mock.AsyncMock()
        await self.client._close_http_session()
        self.client._http_session.close.assert_called()
        asyncio_mock.sleep.assert_called_with(
            self.client._HTTP_SESSION_CLOSE_TIMEOUT
        )

    @mock.patch("aiocometd.client.asyncio")
    async def test_close_http_session_already_closed(self, asyncio_mock):
        self.client._http_session = mock.MagicMock()
        self.client._http_session.closed = True
        self.client._http_session.close = mock.AsyncMock()
        asyncio_mock.sleep = mock.AsyncMock()
        await self.client._close_http_session()
        self.client._http_session.close.assert_not_called()
        asyncio_mock.sleep.assert_not_called()

    @mock.patch("aiocometd.client.ConnectionType", new=MockConnectionType)
    def test_pick_connection_type(self):
        self.client._connection_types = [
            MockConnectionType.TYPE1,
            MockConnectionType.TYPE2,
            MockConnectionType.TYPE3,
        ]
        supported = [
            MockConnectionType.TYPE2.value,
            MockConnectionType.TYPE3.value,
            MockConnectionType.TYPE4.value,
        ]
        result = self.client._pick_connection_type(supported)
        self.assertEqual(result, MockConnectionType.TYPE2)

    @mock.patch("aiocometd.client.ConnectionType", new=MockConnectionType)
    def test_pick_connection_type_without_overlap(self):
        self.client._connection_types = [
            MockConnectionType.TYPE1,
            MockConnectionType.TYPE2,
        ]
        supported = [MockConnectionType.TYPE3.value, MockConnectionType.TYPE4.value]
        result = self.client._pick_connection_type(supported)
        self.assertIsNone(result)

    @mock.patch("aiocometd.client.create_transport")
    async def test_negotiate_transport_default(self, create_transport):
        response = {
            "supportedConnectionTypes": [DEFAULT_CONNECTION_TYPE.value],
            "successful": True
        }
        transport = mock.MagicMock()
        transport.connection_type = DEFAULT_CONNECTION_TYPE
        transport.handshake = mock.AsyncMock(return_value=response)
        create_transport.return_value = transport
        self.client._pick_connection_type = \
            mock.MagicMock(return_value=DEFAULT_CONNECTION_TYPE)
        self.client._verify_response = mock.MagicMock()
        self.client.extensions = object()
        self.client.auth = object()
        http_session = object()
        self.client._get_http_session = mock.AsyncMock(return_value=http_session)

        with self.assertLogs("aiocometd.client", "DEBUG") as log:
            result = await self.client._negotiate_transport()

        self.assertEqual(result, transport)
        create_transport.assert_called_with(
            DEFAULT_CONNECTION_TYPE,
            url=self.client.url,
            incoming_queue=self.client._incoming_queue,
            ssl=self.client.ssl,
            extensions=self.client.extensions,
            auth=self.client.auth,
            json_dumps=self.client._json_dumps,
            json_loads=self.client._json_loads,
            http_session=http_session,
        )
        transport.handshake.assert_called_with(self.client._connection_types)
        self.client._verify_response.assert_called_with(response)
        self.client._pick_connection_type.assert_called_with(
            response["supportedConnectionTypes"])
        log_message = ("INFO:aiocometd.client:Server supports connection types: {!r}"
                       .format(response["supportedConnectionTypes"]))
        self.assertEqual(log.output, [log_message])

    @mock.patch("aiocometd.client.create_transport")
    async def test_negotiate_transport_error(self, create_transport):
        response = {
            "supportedConnectionTypes": [DEFAULT_CONNECTION_TYPE.value],
            "successful": True
        }
        transport = mock.MagicMock()
        transport.connection_type = DEFAULT_CONNECTION_TYPE
        transport.handshake = mock.AsyncMock(return_value=response)
        transport.close = mock.AsyncMock()
        create_transport.return_value = transport
        self.client._pick_connection_type = mock.MagicMock(return_value=None)
        self.client.extensions = object()
        self.client.auth = object()
        http_session = object()
        self.client._get_http_session = mock.AsyncMock(return_value=http_session)

        with self.assertRaisesRegex(ClientError,
                                    "Server offers no supported connection types."):
            with self.assertLogs("aiocometd.client", "DEBUG") as log:
                await self.client._negotiate_transport()

        create_transport.assert_called_with(
            DEFAULT_CONNECTION_TYPE,
            url=self.client.url,
            incoming_queue=self.client._incoming_queue,
            ssl=self.client.ssl,
            extensions=self.client.extensions,
            auth=self.client.auth,
            json_dumps=self.client._json_dumps,
            json_loads=self.client._json_loads,
            http_session=http_session
        )
        transport.handshake.assert_called_with(self.client._connection_types)
        self.client._pick_connection_type.assert_called_with(
            response["supportedConnectionTypes"])
        transport.close.assert_called()
        log_message = ("INFO:aiocometd.client:Server supports connection types: {!r}"
                       .format(response["supportedConnectionTypes"]))
        self.assertEqual(log.output, [log_message])

    @mock.patch("aiocometd.client.create_transport")
    async def test_negotiate_transport_non_default(self, create_transport):
        non_default_type = ConnectionType.WEBSOCKET
        self.client._connection_types = [non_default_type]
        response = {
            "supportedConnectionTypes": [DEFAULT_CONNECTION_TYPE.value,
                                         non_default_type.value],
            "successful": True
        }
        transport1 = mock.MagicMock()
        transport1.connection_type = DEFAULT_CONNECTION_TYPE
        transport1.client_id = "client_id"
        transport1.handshake = mock.AsyncMock(return_value=response)
        transport1.reconnect_advice = object()
        transport1.close = mock.AsyncMock()
        transport2 = mock.MagicMock()
        transport2.connection_type = non_default_type
        transport2.client_id = None
        create_transport.side_effect = [transport1, transport2]
        self.client._pick_connection_type = \
            mock.MagicMock(return_value=non_default_type)
        self.client._verify_response = mock.MagicMock()
        self.client.extensions = object()
        self.client.auth = object()
        http_session = object()
        self.client._get_http_session = mock.AsyncMock(
            return_value=http_session
        )

        with self.assertLogs("aiocometd.client", "DEBUG") as log:
            result = await self.client._negotiate_transport()

        self.assertEqual(result, transport2)
        create_transport.assert_has_calls(
            [
                mock.call(
                    DEFAULT_CONNECTION_TYPE,
                    url=self.client.url,
                    incoming_queue=self.client._incoming_queue,
                    ssl=self.client.ssl,
                    extensions=self.client.extensions,
                    auth=self.client.auth,
                    json_dumps=self.client._json_dumps,
                    json_loads=self.client._json_loads,
                    http_session=http_session,
                ),
                mock.call(
                    non_default_type,
                    url=self.client.url,
                    incoming_queue=self.client._incoming_queue,
                    client_id=transport1.client_id,
                    ssl=self.client.ssl,
                    extensions=self.client.extensions,
                    auth=self.client.auth,
                    json_dumps=self.client._json_dumps,
                    json_loads=self.client._json_loads,
                    reconnect_advice=transport1.reconnect_advice,
                    http_session=http_session,
                )
            ]
        )
        transport1.handshake.assert_called_with(self.client._connection_types)
        self.client._verify_response.assert_called_with(response)
        self.client._pick_connection_type.assert_called_with(
            response["supportedConnectionTypes"])
        transport1.close.assert_called()
        log_message = ("INFO:aiocometd.client:Server supports connection types: {!r}"
                       .format(response["supportedConnectionTypes"]))
        self.assertEqual(log.output, [log_message])

    async def test_open(self):
        transport = mock.MagicMock()
        transport.connection_type = ConnectionType.LONG_POLLING
        connect_result = object()
        transport.connect = mock.AsyncMock(return_value=connect_result)
        self.client._negotiate_transport = mock.AsyncMock(
            return_value=transport
        )
        self.client._verify_response = mock.MagicMock()
        self.client._closed = True

        with self.assertLogs("aiocometd.client", "DEBUG") as log:
            await self.client.open()

        self.client._negotiate_transport.assert_called()
        transport.connect.assert_called()
        self.client._verify_response.assert_called_with(connect_result)
        self.assertEqual(
            log.output,
            ["INFO:aiocometd.client:Opening client with connection types {!r}"
             .format([t.value for t in self.client._connection_types]),
             "INFO:aiocometd.client:Client opened using connection type {!r}"
             .format(self.client.connection_type.value)])

    async def test_open_if_already_open(self):
        transport = mock.MagicMock()
        connect_result = object()
        transport.connect = mock.AsyncMock(return_value=connect_result)
        self.client._negotiate_transport = mock.AsyncMock(
            return_value=transport
        )
        self.client._verify_response = mock.MagicMock()
        self.client._closed = False

        with self.assertRaisesRegex(ClientInvalidOperation,
                                    "Client is already open."):
            await self.client.open()

        self.client._negotiate_transport.assert_not_called()
        transport.connect.assert_not_called()
        self.client._verify_response.assert_not_called()

    async def test_close_with_pending_messages(self):
        self.client._closed = False
        self.client._transport = mock.MagicMock()
        self.client._transport.client_id = "client_id"
        self.client._transport.disconnect = mock.AsyncMock()
        self.client._transport.close = mock.AsyncMock()
        self.client._close_http_session = mock.AsyncMock()
        self.client._incoming_queue = asyncio.Queue()
        self.client._incoming_queue.put_nowait(object())
        expected_log = [
            "WARNING:aiocometd.client:Closing client with {} pending messages...".format(self.client.pending_count),
            "INFO:aiocometd.client:Client closed."
        ]

        with self.assertLogs("aiocometd.client", "DEBUG") as log:
            await self.client.close()

        self.client._transport.disconnect.assert_called()
        self.client._transport.close.assert_called()
        self.client._close_http_session.assert_called()
        self.assertTrue(self.client.closed)
        self.assertEqual(log.output, expected_log)

    async def test_close_if_already_closed(self):
        self.client._closed = True
        self.client._transport = mock.MagicMock()
        self.client._transport.client_id = "client_id"
        self.client._transport.disconnect = mock.AsyncMock()
        self.client._transport.close = mock.AsyncMock()
        self.client._close_http_session = mock.AsyncMock()

        await self.client.close()

        self.client._transport.disconnect.assert_not_called()
        self.client._transport.close.assert_not_called()
        self.client._close_http_session.assert_not_called()
        self.assertTrue(self.client.closed)

    async def test_close_on_transport_error(self):
        self.client._closed = False
        self.client._transport = mock.MagicMock()
        self.client._transport.client_id = "client_id"
        error = TransportError("description")
        self.client._transport.disconnect = mock.AsyncMock(
            side_effect=error
        )
        self.client._transport.close = mock.AsyncMock()
        self.client._close_http_session = mock.AsyncMock()
        expected_log = ["INFO:aiocometd.client:Closing client...",
                        "INFO:aiocometd.client:Client closed."]

        with self.assertLogs("aiocometd.client", "DEBUG") as log:
            with self.assertRaisesRegex(TransportError, str(error)):
                await self.client.close()

        self.assertEqual(log.output, expected_log)
        self.client._transport.disconnect.assert_called()
        self.client._transport.close.assert_not_called()
        self.client._close_http_session.assert_not_called()
        self.assertTrue(self.client.closed)

    async def test_close_no_transport(self):
        self.client._closed = False
        self.client._transport = None
        self.client._close_http_session = mock.AsyncMock()
        expected_log = [
            "INFO:aiocometd.client:Closing client...",
            "INFO:aiocometd.client:Client closed."
        ]

        with self.assertLogs("aiocometd.client", "DEBUG") as log:
            await self.client.close()

        self.assertTrue(self.client.closed)
        self.assertEqual(log.output, expected_log)
        self.client._close_http_session.assert_called()

    async def test_subscribe(self):
        response = {
            "channel": MetaChannel.SUBSCRIBE,
            "successful": True,
            "subscription": "channel1",
            "id": "1"
        }
        self.client._transport = mock.MagicMock()
        self.client._transport.subscribe = mock.AsyncMock(
            return_value=response
        )
        self.client._check_server_disconnected = mock.AsyncMock()
        self.client._closed = False

        with self.assertLogs("aiocometd.client", "DEBUG") as log:
            await self.client.subscribe("channel1")

        self.client._transport.subscribe.assert_called_with("channel1")
        self.assertEqual(log.output,
                         ["INFO:aiocometd.client:Subscribed to channel {}"
                          .format("channel1")])
        self.client._check_server_disconnected.assert_called()

    async def test_subscribe_on_closed(self):
        self.client._closed = True
        self.client._check_server_disconnected = mock.AsyncMock()

        with self.assertRaisesRegex(ClientInvalidOperation,
                                    "Cannot subscribe while client is closed."):
            await self.client.subscribe("channel1")

        self.client._check_server_disconnected.assert_not_called()

    async def test_subscribe_error(self):
        response = {
            "channel": MetaChannel.SUBSCRIBE,
            "successful": False,
            "subscription": "channel1",
            "id": "1"
        }
        self.client._transport = mock.MagicMock()
        self.client._transport.subscribe = mock.AsyncMock(
            return_value=response
        )
        self.client._check_server_disconnected = mock.AsyncMock()
        self.client._closed = False
        error = ServerError("Subscribe request failed.", response)

        with self.assertRaisesRegex(ServerError, str(error)):
            await self.client.subscribe("channel1")

        self.client._transport.subscribe.assert_called_with("channel1")
        self.client._check_server_disconnected.assert_called()

    async def test_unsubscribe(self):
        response = {
            "channel": MetaChannel.UNSUBSCRIBE,
            "successful": True,
            "subscription": "channel1",
            "id": "1"
        }
        self.client._transport = mock.MagicMock()
        self.client._transport.unsubscribe = mock.AsyncMock(
            return_value=response
        )
        self.client._check_server_disconnected = mock.AsyncMock()
        self.client._closed = False

        with self.assertLogs("aiocometd.client", "DEBUG") as log:
            await self.client.unsubscribe("channel1")

        self.client._transport.unsubscribe.assert_called_with("channel1")
        self.assertEqual(log.output,
                         ["INFO:aiocometd.client:Unsubscribed from channel {}"
                            .format("channel1")])
        self.client._check_server_disconnected.assert_called()

    async def test_unsubscribe_on_closed(self):
        self.client._closed = True
        self.client._check_server_disconnected = mock.AsyncMock()

        with self.assertRaisesRegex(ClientInvalidOperation,
                                    "Cannot unsubscribe while client is closed."):
            await self.client.unsubscribe("channel1")

        self.client._check_server_disconnected.assert_not_called()

    async def test_unsubscribe_error(self):
        response = {
            "channel": MetaChannel.UNSUBSCRIBE,
            "successful": False,
            "subscription": "channel1",
            "id": "1"
        }
        self.client._transport = mock.MagicMock()
        self.client._transport.unsubscribe = mock.AsyncMock(
            return_value=response
        )
        self.client._check_server_disconnected = mock.AsyncMock()
        self.client._closed = False
        error = ServerError("Unsubscribe request failed.", response)

        with self.assertRaisesRegex(ServerError, str(error)):
            await self.client.unsubscribe("channel1")

        self.client._transport.unsubscribe.assert_called_with("channel1")
        self.client._check_server_disconnected.assert_called()

    async def test_publish(self):
        response = {
            "channel": "/channel1",
            "successful": True,
            "id": "1"
        }
        data = {}
        self.client._transport = mock.MagicMock()
        self.client._transport.publish = mock.AsyncMock(
            return_value=response
        )
        self.client._check_server_disconnected = mock.AsyncMock()
        self.client._closed = False

        result = await self.client.publish("channel1", data)

        self.assertEqual(result, response)
        self.client._transport.publish.assert_called_with("channel1", data)
        self.client._check_server_disconnected.assert_called()
    async def test_publish_on_closed(self):
        self.client._closed = True
        self.client._check_server_disconnected = mock.AsyncMock()

        with self.assertRaisesRegex(ClientInvalidOperation,
                                    "Cannot publish while client is closed."):
            await self.client.publish("channel1", {})

        self.client._check_server_disconnected.assert_not_called()

    async def test_publish_error(self):
        response = {
            "channel": "/channel1",
            "successful": False,
            "id": "1"
        }
        data = {}
        self.client._transport = mock.MagicMock()
        self.client._transport.publish = mock.AsyncMock(
            return_value=response
        )
        self.client._check_server_disconnected = mock.AsyncMock()
        self.client._closed = False
        error = ServerError("Publish request failed.", response)

        with self.assertRaisesRegex(ServerError, str(error)):
            await self.client.publish("channel1", data)

        self.client._transport.publish.assert_called_with("channel1", data)
        self.client._check_server_disconnected.assert_called()

    def test_repr(self):
        import re
        """Ensure that the Client's __repr__ returns the correct string representation."""
        self.client.url = "http://example.com"
        result = repr(self.client)

        # Permite tanto ConnectionType completo quanto truncado (<ConnectionTy...>)
        pattern = re.compile(
            r"Client\(url='http://example\.com', "
            r"connection_types=\[\<ConnectionTy.*websocket.*?, \<ConnectionTy.*long-polling.*?\], "
            r"connection_timeout=10\.0, ssl=None, max_pending_count=100, "
            r"extensions=None, auth=None\)"
        )

        assert pattern.match(result), f"Unexpected __repr__: {result}"

    def test_verify_response_on_success(self):
        self.client._raise_server_error = mock.MagicMock()
        response = {
            "channel": "/channel1",
            "successful": True,
            "id": "1"
        }

        self.client._verify_response(response)

        self.client._raise_server_error.assert_not_called()

    def test_verify_response_on_error(self):
        self.client._raise_server_error = mock.MagicMock()
        response = {
            "channel": "/channel1",
            "successful": False,
            "id": "1"
        }

        self.client._verify_response(response)

        self.client._raise_server_error.assert_called_with(response)

    def test_verify_response_no_successful_status(self):
        self.client._raise_server_error = mock.MagicMock()
        response = {
            "channel": "/channel1",
            "id": "1"
        }

        self.client._verify_response(response)

        self.client._raise_server_error.assert_not_called()

    def test_raise_server_error_meta(self):
        response = {
            "channel": MetaChannel.SUBSCRIBE,
            "successful": False,
            "id": "1"
        }
        error_message = \
            type(self.client)._SERVER_ERROR_MESSAGES[response["channel"]]

        with self.assertRaisesRegex(ServerError, error_message):
            self.client._raise_server_error(response)

    def test_raise_server_error_service(self):
        response = {
            "channel": SERVICE_CHANNEL_PREFIX + "test",
            "successful": False,
            "id": "1"
        }

        with self.assertRaisesRegex(ServerError, "Service request failed."):
            self.client._raise_server_error(response)

    def test_raise_server_error_publish(self):
        response = {
            "channel": "/some/channel",
            "successful": False,
            "id": "1"
        }

        with self.assertRaisesRegex(ServerError, "Publish request failed."):
            self.client._raise_server_error(response)

    async def test_pending_count(self):
        self.client._incoming_queue = asyncio.Queue()
        await self.client._incoming_queue.put(1)
        await self.client._incoming_queue.put(2)

        self.assertEqual(self.client.pending_count, 2)

    async def test_pending_count_if_none_queue(self):
        self.client._incoming_queue = None
        self.assertEqual(self.client.pending_count, 0)

    async def test_has_pending_messages(self):
        self.client._incoming_queue = asyncio.Queue()
        await self.client._incoming_queue.put(1)
        self.assertTrue(self.client.has_pending_messages)

    async def test_has_pending_messages_false(self):
        self.client._incoming_queue = None
        self.assertFalse(self.client.has_pending_messages)

    async def test_receive_on_closed(self):
        self.client._closed = True
        self.client._incoming_queue = None

        with self.assertRaisesRegex(ClientInvalidOperation,
                                    "Client is closed and no messages remain."):
            await self.client.receive()

    async def test_receive_on_closed_and_pending_messages(self):
        self.client._closed = True
        response = {
            "channel": "/channel1",
            "data": {},
            "id": "1"
        }
        self.client._incoming_queue = mock.MagicMock()
        self.client._incoming_queue.qsize.return_value = 1
        self.client._get_message = mock.AsyncMock(return_value=response)
        self.client._verify_response = mock.MagicMock()

        result = await self.client.receive()

        self.assertEqual(result, response)
        self.client._get_message.assert_called_with(
            self.client.connection_timeout)
        self.client._verify_response.assert_called_with(response)

    async def test_receive_on_open(self):
        self.client._closed = False
        response = {
            "channel": "/channel1",
            "data": {},
            "id": "1"
        }
        self.client._incoming_queue = mock.MagicMock()
        self.client._incoming_queue.qsize.return_value = 1
        self.client._get_message = mock.AsyncMock(return_value=response)
        self.client._verify_response = mock.MagicMock()

        result = await self.client.receive()

        self.assertEqual(result, response)
        self.client._get_message.assert_called_with(
            self.client.connection_timeout)
        self.client._verify_response.assert_called_with(response)

    async def test_receive_on_connection_timeout(self):
        self.client._closed = True
        self.client._incoming_queue = mock.MagicMock()
        self.client._incoming_queue.qsize.return_value = 1
        self.client._get_message = mock.AsyncMock(
            side_effect=TransportTimeoutError())
        self.client._verify_response = mock.MagicMock()

        with self.assertRaises(TransportTimeoutError):
            await self.client.receive()

        self.client._get_message.assert_called_with(
            self.client.connection_timeout)
        self.client._verify_response.assert_not_called()

    async def test_aiter(self):
        responses = [
            {
                "channel": "/channel1",
                "data": {},
                "id": "1"
            },
            {
                "channel": "/channel2",
                "data": {},
                "id": "2"
            }
        ]
        self.client.receive = mock.AsyncMock(
            side_effect=responses + [ClientInvalidOperation()])

        result = []
        async for message in self.client:
            result.append(message)

        self.assertEqual(result, responses)

    async def test_context_manager(self):
        self.client.open = mock.AsyncMock()
        self.client.close = mock.AsyncMock()

        async with self.client as client:
            pass

        self.assertIs(client, self.client)
        self.client.open.assert_called()
        self.client.close.assert_called()

    async def test_context_manager_on_enter_error(self):
        self.client.open = mock.AsyncMock(
            side_effect=TransportError()
        )
        self.client.close = mock.AsyncMock()

        with self.assertRaises(TransportError):
            async with self.client:
                pass

        self.client.open.assert_called()
        self.client.close.assert_called()

    @mock.patch("aiocometd.client.asyncio")
    async def test_wait_connection_timeout_on_timeout(self, asyncio_mock):
        self.client._transport = mock.MagicMock()
        self.client._transport.wait_for_state = mock.AsyncMock()
        timeout = 2

        async def raise_timeout(*args, **kwargs):
            raise asyncio.TimeoutError()

        async def fake_wait_for_state(*args, **kwargs):
            return None

        done_future = asyncio.Future()
        done_future.set_result(None)
        self.client._transport.wait_for_state = mock.Mock(return_value=done_future)

        asyncio_mock.wait_for = mock.AsyncMock(side_effect=raise_timeout)
        asyncio_mock.TimeoutError = asyncio.TimeoutError
        await self.client._wait_connection_timeout(timeout)
        self.client._transport.wait_for_state.assert_has_calls([
            mock.call(TransportState.CONNECTING),
            mock.call(TransportState.CONNECTED)
        ])
        asyncio_mock.wait_for.assert_called()

    @mock.patch("aiocometd.client.asyncio")
    async def test_wait_connection_timeout_iterations(self, asyncio_mock):
        self.client._transport = mock.MagicMock()
        self.client._transport.wait_for_state = mock.AsyncMock()
        timeout = 2
        asyncio_mock.wait_for = mock.AsyncMock(
            side_effect=[None, asyncio.TimeoutError()]
        )
        asyncio_mock.TimeoutError = asyncio.TimeoutError
        done_future = asyncio.Future()
        done_future.set_result(None)
        self.client._transport.wait_for_state = mock.Mock(return_value=done_future)

        await self.client._wait_connection_timeout(timeout)

        self.client._transport.wait_for_state.assert_has_calls([
            mock.call(TransportState.CONNECTING),
            mock.call(TransportState.CONNECTED),
            mock.call(TransportState.CONNECTING),
            mock.call(TransportState.CONNECTED)
        ])
        asyncio_mock.wait_for.assert_called()

    async def test_get_message_no_timeout(self):
        self.client._incoming_queue = mock.MagicMock()
        self.client._incoming_queue.get = mock.AsyncMock(
            return_value=object()
        )
        self.client._wait_connection_timeout = mock.MagicMock()
        self.client._transport = mock.MagicMock()
        self.client._transport.wait_for_state = mock.MagicMock(
            return_value=self.long_task(None, timeout=1)
        )

        result = await self.client._get_message(None)

        self.assertIs(result, self.client._incoming_queue.get.return_value)
        self.client._wait_connection_timeout.assert_not_called()
        self.client._transport.wait_for_state.assert_called_with(
            TransportState.SERVER_DISCONNECTED
        )

    async def test_get_message_with_timeout_not_triggered(self):
        self.client._incoming_queue = mock.MagicMock()
        get_result = object()
        self.client._incoming_queue.get = mock.MagicMock(
            return_value=self.long_task(get_result, timeout=None)
        )
        self.client._wait_connection_timeout = mock.MagicMock(
            return_value=self.long_task(None, timeout=1)
        )
        self.client._transport = mock.MagicMock()
        self.client._transport.wait_for_state = mock.MagicMock(
            return_value=self.long_task(None, timeout=1)
        )
        timeout = 2

        result = await self.client._get_message(timeout)

        self.assertIs(result, get_result)
        self.client._wait_connection_timeout.assert_called_with(timeout)
        self.client._transport.wait_for_state.assert_called_with(
            TransportState.SERVER_DISCONNECTED
        )

    async def test_get_message_with_timeout_triggered(self):
        self.client._incoming_queue = mock.MagicMock()
        get_result = object()
        self.client._incoming_queue.get = mock.MagicMock(
            return_value=self.long_task(get_result, timeout=1)
        )
        self.client._wait_connection_timeout = mock.MagicMock(
            return_value=self.long_task(None, timeout=None)
        )
        self.client._transport = mock.MagicMock()
        self.client._transport.wait_for_state = mock.MagicMock(
            return_value=self.long_task(None, timeout=1)
        )
        timeout = 2

        with self.assertRaisesRegex(TransportTimeoutError,
                                    "Lost connection with the server"):
            await self.client._get_message(timeout)

        self.client._incoming_queue.get.assert_called()
        self.client._wait_connection_timeout.assert_called_with(timeout)
        self.client._transport.wait_for_state.assert_called_with(
            TransportState.SERVER_DISCONNECTED
        )

    async def test_get_message_with_server_disconnected(self):
        self.client._incoming_queue = mock.MagicMock()
        get_result = object()
        self.client._incoming_queue.get = mock.MagicMock(
            return_value=self.long_task(get_result, timeout=1)
        )
        self.client._wait_connection_timeout = mock.MagicMock(
            return_value=self.long_task(None, timeout=1)
        )
        self.client._transport = mock.MagicMock()
        self.client.close = mock.AsyncMock()
        self.client._transport.wait_for_state = mock.MagicMock(
            return_value=self.long_task(None, timeout=None)
        )
        timeout = 2
        self.client._transport.state = TransportState.SERVER_DISCONNECTED

        with self.assertRaisesRegex(ServerError,
                                    "Connection closed by the server"):
            await self.client._get_message(timeout)

        self.client._wait_connection_timeout.assert_called_with(timeout)
        self.client._transport.wait_for_state.assert_called_with(
            TransportState.SERVER_DISCONNECTED
        )
        self.client.close.assert_called()

    @mock.patch("aiocometd.client.asyncio")
    async def test_get_message_cancelled(self, asyncio_mock):
        self.client._incoming_queue = mock.MagicMock()
        self.client._wait_connection_timeout = mock.MagicMock()
        self.client._transport = mock.MagicMock()
        self.client._transport.wait_for_state = mock.MagicMock()
        asyncio_mock.ensure_future = mock.MagicMock(
            side_effect=[mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
        )
        asyncio_mock.wait = mock.AsyncMock(
            side_effect=asyncio.CancelledError()
        )
        asyncio_mock.CancelledError = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.client._get_message(1)

        for task in asyncio_mock.ensure_future.side_effect:
            task.cancel.assert_called()

    async def test_check_server_disconnected_on_disconnected(self):
        self.client._transport = mock.MagicMock()
        self.client._transport.state = TransportState.SERVER_DISCONNECTED
        self.client._transport.last_connect_result = object()

        with self.assertRaisesRegex(ServerError,
                                    "Connection closed by the server") as cm:
            await self.client._check_server_disconnected()

        self.assertEqual(cm.exception.response,
                         self.client._transport.last_connect_result)

    async def test_check_server_disconnected_on_not_disconnected(self):
        self.client._transport = mock.MagicMock()
        self.client._transport.state = TransportState.CONNECTED

        await self.client._check_server_disconnected()

    async def test_check_server_disconnected_on_none_transport(self):
        self.client._transport = None
        await self.client._check_server_disconnected()
