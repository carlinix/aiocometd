import asyncio
import unittest
from unittest import mock

from aiohttp import client_exceptions, WSMsgType

from aiocometd.transports.websocket import WebSocketTransport, WebSocketFactory
from aiocometd.constants import ConnectionType
from aiocometd.exceptions import TransportConnectionClosed, TransportError

class TestWebSocketFactory(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.session = mock.MagicMock()
        self.url = "http://example.com/"
        self.factory = WebSocketFactory(self.session)

    async def test_enter(self):
        socket = object()
        context = mock.MagicMock()
        context.__aenter__ = mock.AsyncMock(return_value=socket)
        self.session.ws_connect.return_value = context
        args = [object()]
        kwargs = {"key": "value"}

        result = await self.factory._enter(*args, **kwargs)

        self.session.ws_connect.assert_called_with(*args, **kwargs)
        self.assertEqual(self.factory._context, context)
        self.assertEqual(result, socket)

    async def test_exit(self):
        socket = object()
        context = mock.MagicMock()
        context.__aexit__ = mock.AsyncMock(return_value=socket)
        self.factory._context = context
        self.factory._socket = socket

        await self.factory._exit()

        context.__aexit__.assert_awaited()
        self.assertIsNone(self.factory._context)
        self.assertIsNone(self.factory._socket)

    async def test_exit_none_context(self):
        self.factory._context = None
        await self.factory._exit()

    async def test_close(self):
        self.factory._exit = mock.AsyncMock()
        await self.factory.close()
        self.factory._exit.assert_awaited()

    async def test_close_suppresses_errors(self):
        self.factory._exit = mock.AsyncMock(side_effect=AttributeError())
        await self.factory.close()
        self.factory._exit.assert_awaited()

    async def test_call_socket_creates_socket(self):
        self.factory._enter = mock.AsyncMock()
        args = [object()]
        kwargs = {"key": "value"}

        await self.factory(*args, **kwargs)

        self.factory._enter.assert_awaited_with(*args, **kwargs)
        self.assertEqual(self.factory._socket, self.factory._enter.return_value)

    async def test_call_socket_returns_open_socket(self):
        self.factory._enter = mock.AsyncMock()
        socket = mock.MagicMock()
        socket.closed = False
        self.factory._socket = socket

        result = await self.factory()

        self.assertEqual(result, socket)
        self.factory._enter.assert_not_called()

    async def test_call_socket_creates_new_if_closed(self):
        self.factory._exit = mock.AsyncMock()
        socket = mock.MagicMock()
        socket.closed = True
        self.factory._socket = socket

        await self.factory()
        self.factory._exit.assert_awaited()


class TestWebSocketTransport(unittest.IsolatedAsyncioTestCase):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)

    async def asyncSetUp(self):
        self.http_session = object()
        self.transport = WebSocketTransport(
            url="example.com/cometd",
            incoming_queue=None,
            http_session=self.http_session,
        )

    def test_connection_type(self):
        self.assertEqual(self.transport.connection_type, ConnectionType.WEBSOCKET)

    async def test_get_socket(self):
        expected_socket = object()
        self.transport._socket_factory = mock.AsyncMock(return_value=expected_socket)
        headers = object()

        result = await self.transport._get_socket(headers)

        self.assertIs(result, expected_socket)
        self.transport._socket_factory.assert_awaited_with(
            self.transport._url,
            ssl=self.transport.ssl,
            headers=headers,
            receive_timeout=self.transport.request_timeout,
            autoping=True,
        )

    @mock.patch("aiocometd.transports.websocket.asyncio")
    async def test_close(self, asyncio_obj):
        self.transport._socket_factory_short = mock.MagicMock()
        self.transport._socket_factory_short.close = mock.AsyncMock()
        self.transport._socket_factory = mock.MagicMock()
        self.transport._socket_factory.close = mock.AsyncMock()
        self.transport._close_http_session = mock.AsyncMock()
        self.transport._receive_task = mock.MagicMock()
        self.transport._receive_task.done.return_value = False
        asyncio_obj.wait = mock.AsyncMock()

        await self.transport.close()

        self.transport._receive_task.cancel.assert_called()
        asyncio_obj.wait.assert_awaited_with([self.transport._receive_task])
        self.transport._socket_factory.close.assert_awaited()

    @mock.patch("aiocometd.transports.websocket.asyncio")
    async def test_close_on_done_receive_task(self, asyncio_obj):
        self.transport._socket_factory_short = mock.MagicMock()
        self.transport._socket_factory_short.close = mock.AsyncMock()
        self.transport._socket_factory = mock.MagicMock()
        self.transport._socket_factory.close = mock.AsyncMock()
        self.transport._close_http_session = mock.AsyncMock()
        self.transport._receive_task = mock.MagicMock()
        self.transport._receive_task.done.return_value = True
        asyncio_obj.wait = mock.AsyncMock()

        await self.transport.close()

        self.transport._receive_task.cancel.assert_not_called()
        asyncio_obj.wait.assert_not_called()
        self.transport._socket_factory.close.assert_awaited()

    @mock.patch("aiocometd.transports.websocket.asyncio")
    async def test_close_on_no_receive_task(self, asyncio_obj):
        self.transport._socket_factory_short = mock.MagicMock()
        self.transport._socket_factory_short.close = mock.AsyncMock()
        self.transport._socket_factory = mock.MagicMock()
        self.transport._socket_factory.close = mock.AsyncMock()
        self.transport._close_http_session = mock.AsyncMock()
        self.transport._receive_task = None

        await self.transport.close()
        self.transport._socket_factory.close.assert_awaited()

    async def test_send_socket_payload(self):
        payload = [{"id": 42}]  # Use a valid payload to match _create_exchange_future
        socket = mock.MagicMock()
        socket.send_json = mock.AsyncMock()
        expected_result = {"id": 42, "data": "response"}
        fut = asyncio.Future()
        fut.set_result(expected_result)

        exchange_result = fut
        self.transport._create_exchange_future = mock.MagicMock(
            return_value=exchange_result
        )
        self.transport._start_receive_task = mock.MagicMock()

        result = await self.transport._send_socket_payload(socket, payload)

        self.transport._create_exchange_future.assert_called_with(payload)
        socket.send_json.assert_called_with(payload,
                                            dumps=self.transport._json_dumps)
        self.transport._start_receive_task.assert_called()
        self.assertEqual(result, expected_result)

    async def test_send_socket_payload_on_send_error(self):
            payload = [{"id": 0}]
            socket = mock.MagicMock()
            error = ValueError()
            socket.send_json = mock.MagicMock(side_effect=error)
            future = asyncio.Future()
            exchange_result = asyncio.Future()
            exchange_result.set_result(future)
            self.transport._create_exchange_future = mock.MagicMock(
                return_value=exchange_result
            )
            self.transport._start_receive_task = mock.MagicMock()
            self.transport._set_exchange_errors = mock.MagicMock()

            with self.assertRaises(ValueError):
                await self.transport._send_socket_payload(socket, payload)

            self.transport._create_exchange_future.assert_called_with(payload)
            socket.send_json.assert_called_with(payload,
                                                dumps=self.transport._json_dumps)
            self.transport._set_exchange_errors.assert_called_with(error)
            self.transport._start_receive_task.assert_not_called()

    async def test_send_final_payload_transport_error(self):
        payload = object()
        socket = object()
        exc = client_exceptions.ClientError("message")
        self.transport._get_socket = mock.AsyncMock(return_value=socket)
        self.transport._send_socket_payload = mock.AsyncMock(side_effect=exc)
        headers = object()

        with self.assertLogs(WebSocketTransport.__module__, "DEBUG") as log:
            with self.assertRaisesRegex(TransportError, str(exc)):
                await self.transport._send_final_payload(payload, headers=headers)

        expected = f"WARNING:{WebSocketTransport.__module__}:Failed to send payload: {exc}"
        self.assertEqual(log.output, [expected])
        self.transport._get_socket.assert_awaited_with(headers)
        self.transport._send_socket_payload.assert_awaited_with(socket, payload)

    async def test_send_final_payload_connection_closed_error(self):
        payload = object()
        socket1 = object()
        socket2 = object()
        resp = object()
        self.transport._get_socket = mock.AsyncMock(side_effect=[socket1, socket2])
        err = TransportConnectionClosed()
        self.transport._send_socket_payload = mock.AsyncMock(side_effect=[err, resp])
        headers = object()

        result = await self.transport._send_final_payload(payload, headers=headers)
        self.assertEqual(result, resp)
        self.transport._get_socket.assert_has_awaits(
            [mock.call(headers), mock.call(headers)]
        )

    async def test_send_final_payload_connection_timeout_error(self):
        payload = object()
        socket = object()
        self.transport._get_socket = mock.AsyncMock(return_value=socket)
        err = asyncio.TimeoutError()
        self.transport._send_socket_payload = mock.AsyncMock(side_effect=err)
        self.transport._reset_socket = mock.AsyncMock()
        headers = object()

        with self.assertRaises(asyncio.TimeoutError):
            await self.transport._send_final_payload(payload, headers=headers)

        self.transport._reset_socket.assert_awaited()

    @mock.patch("aiocometd.transports.websocket.WebSocketFactory")
    async def test_reset_socket(self, ws_factory_cls):
        socket_factory = object()
        ws_factory_cls.return_value = socket_factory
        old = mock.MagicMock()
        old.close = mock.AsyncMock()
        self.transport._socket_factory = old

        await self.transport._reset_socket()

        old.close.assert_awaited()
        self.assertIs(self.transport._socket_factory, socket_factory)
        ws_factory_cls.assert_called_with(self.http_session)

    def test_set_exchange_errors(self):
        err = ValueError()
        fut = asyncio.Future()
        self.transport._pending_exchanges = {0: fut}
        self.transport._set_exchange_errors(err)
        self.assertEqual(fut.exception(), err)
        self.assertEqual(self.transport._pending_exchanges, {})

    def test_set_exchange_errors_skips_completed(self):
        err = ValueError()
        fut = asyncio.Future()
        fut.set_result("ok")
        self.transport._pending_exchanges = {0: fut}
        self.transport._set_exchange_errors(err)
        self.assertEqual(fut.result(), "ok")
        self.assertEqual(self.transport._pending_exchanges, {})

    def test_set_exchange_results(self):
        fut1 = asyncio.Future()
        fut2 = asyncio.Future()
        fut2.set_result("done")
        fut3 = asyncio.Future()
        self.transport._pending_exchanges = {0: fut1, 1: fut2, 3: fut3}
        payload = [{"id": 0}, {"id": 1}, {"id": 2}, {}]

        self.transport._set_exchange_results(payload)

        self.assertEqual(fut1.result(), payload[0])
        self.assertEqual(fut2.result(), "done")
        self.assertEqual(self.transport._pending_exchanges, {3: fut3})

    async def test_receive_done_with_result(self):
        future = mock.MagicMock()
        result = object()
        future.result.return_value = result
        self.transport._receive_task = object()

        with self.assertLogs("aiocometd.transports.websocket", "DEBUG") as log:
            self.transport._receive_done(future)

        self.transport._receive_task = None
        self.assertEqual(log.output, [
            f"DEBUG:aiocometd.transports.websocket:"
            f"Receive task finished with: {result!r}"
        ])

    async def test_receive_done_with_error(self):
        future = mock.MagicMock()
        result = ValueError()
        future.result.side_effect = result
        self.transport._receive_task = object()

        with self.assertLogs("aiocometd.transports.websocket", "DEBUG") as log:
            self.transport._receive_done(future)

        self.transport._receive_task = None
        self.assertEqual(log.output, [
            f"DEBUG:aiocometd.transports.websocket:"
            f"Receive task finished with: {result!r}"
        ])

    async def test_receive(self):
        # Prepare mock WebSocket message and payload

        response = mock.MagicMock()
        response_payload = object()
        response.json = mock.Mock(return_value=response_payload)

        # Simulate socket.receive() returning response, then raising CancelledError
        socket = mock.MagicMock()
        socket.receive = mock.AsyncMock(
            side_effect=[response, asyncio.CancelledError()]
        )

        # Patch internal async methods
        self.transport._consume_payload = mock.AsyncMock()
        self.transport._set_exchange_results = mock.MagicMock()

        # Expect CancelledError after consuming the first message
        with self.assertRaises(asyncio.CancelledError):
            await self.transport._receive(socket)


        socket.receive.assert_awaited()
        response.json.assert_called_with(loads=self.transport._json_loads)
        self.transport._consume_payload.assert_awaited_with(response_payload)
        self.transport._set_exchange_results(response_payload)

    async def test_receive_socket_closed(self):

        response = mock.MagicMock()
        response.type = WSMsgType.CLOSE
        response_payload = object()
        response.json.return_value = response_payload
        socket = mock.MagicMock()
        socket.receive = mock.AsyncMock(
            side_effect=[response, asyncio.CancelledError()]
        )
        self.transport._consume_payload = mock.AsyncMock()
        self.transport._set_exchange_results = mock.AsyncMock()

        with self.assertRaisesRegex(TransportConnectionClosed,
                                    "Received CLOSE message from server."):
            await self.transport._receive(socket)

        socket.receive.assert_called()
        response.json.assert_not_called()
        self.transport._consume_payload.assert_not_called()
        self.transport._set_exchange_results.assert_not_called()

    async def test_receive_parse_type_error(self):
        response = mock.MagicMock()
        response.json.side_effect = TypeError()

        socket = mock.MagicMock()
        socket.receive = mock.AsyncMock(
            side_effect=[response, asyncio.CancelledError()]
        )

        self.transport._consume_payload = mock.AsyncMock()
        self.transport._set_exchange_results = mock.MagicMock()
        with self.assertRaisesRegex(TransportError, "Received invalid JSON payload from server."):
            await self.transport._receive(socket)

        socket.receive.assert_called()
        response.json.assert_called_with(loads=self.transport._json_loads)
        self.transport._consume_payload.assert_not_called()
        self.transport._set_exchange_results.assert_not_called()

    async def test_receive_any_error(self):
        response = mock.MagicMock()
        response_payload = object()
        response.json = mock.AsyncMock(return_value=response_payload)

        socket = mock.MagicMock()
        socket.receive = mock.AsyncMock(side_effect=ValueError())

        self.transport._consume_payload = mock.AsyncMock()
        self.transport._set_exchange_results = mock.MagicMock()
        self.transport._set_exchange_errors = mock.MagicMock()

        with self.assertRaises(ValueError):
            await self.transport._receive(socket)

        socket.receive.assert_awaited()
        response.json.assert_not_awaited()
        self.transport._consume_payload.assert_not_awaited()
        self.transport._set_exchange_results.assert_not_called()
        self.transport._set_exchange_errors.assert_called_with(mock.ANY)

        err_arg = self.transport._set_exchange_errors.call_args.args[0]
        self.assertIsInstance(err_arg, ValueError)
