import asyncio
import unittest
from unittest import mock

from aiocometd.transports.base import TransportBase
from aiocometd.constants import (
    ConnectionType,
    MetaChannel,
    TransportState,
    CONNECT_MESSAGE,
    SUBSCRIBE_MESSAGE,
    DISCONNECT_MESSAGE,
    PUBLISH_MESSAGE,
    UNSUBSCRIBE_MESSAGE,
)
from aiocometd.extensions import Extension, AuthExtension
from aiocometd.exceptions import TransportInvalidOperation, TransportError


class TransportBaseImpl(TransportBase):
    async def _send_final_payload(self, payload):
        pass

    @property
    def connection_type(self):
        return ConnectionType.LONG_POLLING


class TestTransportBase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.transport = TransportBaseImpl(
            url="example.com/cometd",
            incoming_queue=None,
            http_session=None,
        )

    async def long_task(self, result, timeout=None):
        if timeout:
            await asyncio.sleep(timeout)
        if not isinstance(result, Exception):
            return result
        else:
            raise result

    def test_init_with_loop(self):
        """Ensure passing an explicit loop raises an appropriate error."""
        loop = object()

        with self.assertRaises((TypeError, TransportInvalidOperation)) as ctx:
            TransportBaseImpl(
                url=None, incoming_queue=None, http_session=None, loop=loop
            )

        self.assertIn("loop", str(ctx.exception))

    async def test_init_with_reconnect_advice(self):
        """Ensure reconnect_advice is set correctly when provided."""
        advice = object()

        transport = TransportBaseImpl(
            url="http://example.com",
            incoming_queue=None,
            http_session=None,
            reconnect_advice=advice,
        )

        self.assertIs(transport.reconnect_advice, advice)
        self.assertIsInstance(transport._loop, asyncio.AbstractEventLoop)

    def test_init_without_reconnect_advice(self):
        transport = TransportBaseImpl(
            url=None, incoming_queue=None, http_session=None
        )
        self.assertEqual(transport.reconnect_advice, {})

    def test_finalize_message_updates_fields(self):
        message = {
            "field": "value",
            "id": None,
            "clientId": None,
            "connectionType": None,
        }
        self.transport._client_id = "client_id"
        self.transport._finalize_message(message)
        self.assertEqual(message["id"], "0")
        self.assertEqual(self.transport._message_id, 1)
        self.assertEqual(message["clientId"], self.transport.client_id)
        self.assertEqual(
            message["connectionType"], self.transport.connection_type.value
        )

    def test_finalize_message_ignores_non_existing_fields(self):
        message = {"field": "value"}
        self.transport._client_id = "client_id"
        self.transport._finalize_message(message)
        self.assertEqual(message["field"], "value")

    def test_finalize_payload_single_message(self):
        payload = {"field": "value", "id": None, "clientId": None}
        self.transport._finalize_message = mock.MagicMock()
        self.transport._finalize_payload(payload)
        self.transport._finalize_message.assert_called_once_with(payload)

    def test_finalize_payload_multiple_messages(self):
        payload = [
            {"field": "value", "id": None, "clientId": None, "connectionType": None},
            {"field2": "value2", "id": None, "clientId": None, "connectionType": None},
        ]
        self.transport._finalize_message = mock.MagicMock()
        self.transport._finalize_payload(payload)
        self.transport._finalize_message.assert_has_calls(
            [mock.call(payload[0]), mock.call(payload[1])]
        )

    @mock.patch("aiocometd.transports.base.is_event_message")
    async def test_consume_message_non_event_message(self, is_event_message):
        is_event_message.return_value = False
        self.transport._incoming_queue = mock.MagicMock()
        response_message = object()
        await self.transport._consume_message(response_message)
        is_event_message.assert_called_with(response_message)


    @mock.patch("aiocometd.transports.base.is_event_message")
    async def test_consume_message_event_message(self, is_event_message):
        is_event_message.return_value = True
        self.transport.incoming_queue = mock.MagicMock()
        self.transport.incoming_queue.put = mock.AsyncMock()
        response_message = {"x": object}
        await self.transport._consume_message(response_message)
        is_event_message.assert_called_with(response_message)

    async def test_process_incoming_payload(self):
        extension = mock.create_autospec(Extension)
        auth = mock.create_autospec(AuthExtension)
        self.transport._extensions = [extension]
        self.transport._auth = auth
        payload = object()
        headers = object()
        await self.transport._process_incoming_payload(payload, headers)
        extension.incoming.assert_called_with(payload, headers)
        auth.incoming.assert_called_with(payload, headers)

    async def test_process_outgoing_payload(self):
        extension = mock.create_autospec(Extension)
        auth = mock.create_autospec(AuthExtension)
        self.transport._extensions = [extension]
        self.transport._auth = auth
        payload = object()
        headers = object()
        await self.transport._process_outgoing_payload(payload, headers)
        extension.outgoing.assert_called_with(payload, headers)
        auth.outgoing.assert_called_with(payload, headers)

    async def test_process_outgoing_payload_without_auth(self):
        extension = mock.create_autospec(Extension)
        self.transport._extensions = [extension]
        self.transport._auth = None
        payload = object()
        headers = object()
        await self.transport._process_outgoing_payload(payload, headers)
        extension.outgoing.assert_called_with(payload, headers)

    async def test_send_payload(self):
        payload = object()
        self.transport._finalize_payload = mock.MagicMock()
        response = object()
        self.transport._send_final_payload = mock.AsyncMock(return_value=response)
        self.transport._process_outgoing_payload = mock.AsyncMock()
        result = await self.transport._send_payload(payload)
        self.assertEqual(result, response)
        self.transport._finalize_payload.assert_called_with(payload)
        self.transport._send_final_payload.assert_awaited_with(payload, headers={})
        self.transport._process_outgoing_payload.assert_awaited_with(payload, {})

    async def test_send_payload_with_auth(self):
        response = object()
        payload = object()
        self.transport._send_payload = mock.AsyncMock(return_value=response)
        self.transport._auth = None
        self.transport._is_auth_error_message = mock.MagicMock(return_value=False)
        result = await self.transport._send_payload_with_auth(payload)
        self.assertIs(result, response)
        self.transport._send_payload.assert_awaited_with(payload)
        self.transport._is_auth_error_message.assert_not_called()

    @mock.patch("aiocometd.transports.base.is_auth_error_message")
    async def test_send_payload_with_auth_with_extension(self, is_auth_error_message):
        response = object()
        payload = object()
        self.transport._send_payload = mock.AsyncMock(return_value=response)
        self.transport._auth = mock.create_autospec(AuthExtension)
        is_auth_error_message.return_value = False
        result = await self.transport._send_payload_with_auth(payload)
        self.assertIs(result, response)
        self.transport._send_payload.assert_awaited_with(payload)
        is_auth_error_message.assert_called_with(response)
        self.transport._auth.authenticate.assert_not_called()

    @mock.patch("aiocometd.transports.base.is_auth_error_message")
    async def test_send_payload_with_auth_with_extension_error(
        self, is_auth_error_message
    ):
        response = object()
        response2 = object()
        payload = object()
        self.transport._send_payload = mock.AsyncMock(
            side_effect=[response, response2]
        )
        self.transport._auth = mock.create_autospec(AuthExtension)
        is_auth_error_message.return_value = True
        result = await self.transport._send_payload_with_auth(payload)
        self.assertIs(result, response2)
        self.transport._send_payload.assert_has_awaits(
            [mock.call(payload), mock.call(payload)]
        )
        is_auth_error_message.assert_called_with(response)
        self.transport._auth.authenticate.assert_called()
