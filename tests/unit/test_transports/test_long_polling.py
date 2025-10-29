import unittest
from unittest import mock
from aiohttp import client_exceptions

from aiocometd.transports.long_polling import LongPollingTransport
from aiocometd.constants import ConnectionType
from aiocometd.exceptions import TransportError


from unittest import mock
from typing import Any
from aiohttp import client_exceptions


def make_aiohttp_post_raises(exc: Exception) -> mock.MagicMock:
    """Cria um mock de `aiohttp.ClientSession.post` que lança uma exceção simulada.

    Args:
        exc: Exceção a ser lançada ao tentar fazer POST.

    Returns:
        Mock configurado que simula o comportamento de `session.post` com falha.
    """
    post_ctx = mock.MagicMock()
    post_ctx.__aenter__ = mock.AsyncMock(side_effect=exc)
    post_ctx.__aexit__ = mock.AsyncMock(return_value=None)

    session_post = mock.MagicMock(return_value=post_ctx)
    return session_post

def make_aiohttp_post_mock(response_mock) -> mock.MagicMock:
    """Cria um mock de `aiohttp.ClientSession.post` que retorna um contexto assíncrono.

    Args:
        response_mock: Objeto simulando a resposta (`aiohttp.ClientResponse`).

    Returns:
        Mock configurado que suporta `async with`.
    """
    post_ctx = mock.MagicMock()
    post_ctx.__aenter__ = mock.AsyncMock(return_value=response_mock)
    post_ctx.__aexit__ = mock.AsyncMock(return_value=None)
    session_post = mock.MagicMock(return_value=post_ctx)
    return session_post

def make_aiohttp_post_mock(response: Any) -> mock.MagicMock:
    """Cria um mock compatível com o contexto `async with session.post()` do aiohttp.

    Args:
        response: Objeto a ser retornado ao entrar no contexto (geralmente um mock de resposta).

    Returns:
        Mock configurado para substituir `aiohttp.ClientSession.post`.
    """
    post_ctx = mock.MagicMock()
    post_ctx.__aenter__ = mock.AsyncMock(return_value=response)
    post_ctx.__aexit__ = mock.AsyncMock(return_value=None)

    session_post = mock.MagicMock(return_value=post_ctx)
    return session_post

def make_async_semaphore_mock() -> mock.AsyncMock:
    """Cria um mock de semáforo assíncrono (`asyncio.Semaphore`) compatível com `async with`.

    Returns:
        Mock de semáforo com `__aenter__`/`__aexit__` assíncronos.
    """
    sem = mock.AsyncMock()
    sem.__aenter__.return_value = None
    sem.__aexit__.return_value = None
    return sem


class TestLongPollingTransport(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.transport = LongPollingTransport(
            url="example.com/cometd",
            incoming_queue=None,
            http_session=None,
        )

    def test_connection_type(self):
        self.assertEqual(
            self.transport.connection_type,
            ConnectionType.LONG_POLLING,
        )

    async def test_send_payload_final_payload(self):
        resp_data = [{"channel": "test/channel3", "data": {}, "id": 4}]

        response_mock = mock.MagicMock()
        response_mock.json = mock.AsyncMock(return_value=resp_data)
        response_mock.headers = object()
        session = mock.MagicMock()
        session.post = mock.AsyncMock(return_value=response_mock)  # Direct AsyncMock

        self.transport._http_session = session
        self.transport._http_semaphore = mock.MagicMock()
        self.transport.ssl = object()
        self.transport._consume_payload = mock.AsyncMock(return_value=resp_data[0])
        self.transport._json_loads = mock.Mock()

        payload = [object(), object()]
        headers = {"key": "value"}

        response = await self.transport._send_final_payload(payload, headers=headers)

        self.assertEqual(response, resp_data[0])
        session.post.assert_called_once_with(
            self.transport._url, json=payload, ssl=self.transport.ssl,
            headers=headers, timeout=self.transport.request_timeout
        )
        response_mock.json.assert_called_once_with(loads=self.transport._json_loads)

    async def test_send_payload_final_payload_client_error(self):
        post_exception = client_exceptions.ClientError("client error")
        session = mock.MagicMock()
        session.post = mock.AsyncMock(side_effect=post_exception)

        self.transport._http_session = session
        self.transport._http_semaphore = mock.MagicMock()
        payload = [object(), object()]
        self.transport.ssl = object()
        self.transport._consume_payload = mock.AsyncMock()
        headers = {"key": "value"}

        with self.assertLogs(LongPollingTransport.__module__, level="WARNING") as log:
            with self.assertRaisesRegex(TransportError, "client error"):
                await self.transport._send_final_payload(payload, headers=headers)

        assert any("Failed to send payload" in entry for entry in log.output)
        self.transport._consume_payload.assert_not_awaited()

    async def test_send_payload_final_payload_missing_response(self):
        """Ensure TransportError is raised when response is missing for first message."""
        resp_data = [{"channel": "test/channel3", "data": {}, "id": 4}]
        response_mock = mock.MagicMock()
        response_mock.json = mock.AsyncMock(return_value=resp_data)
        response_mock.headers = {"X-Test": "ok"}
        response_mock.raise_for_status = mock.MagicMock()

        session = mock.MagicMock()
        session.post = mock.AsyncMock(return_value=response_mock)
        self.transport._http_session = session
        self.transport._http_semaphore = make_async_semaphore_mock()

        payload = [object(), object()]
        self.transport.ssl = object()
        self.transport._consume_payload = mock.AsyncMock(return_value=None)
        headers = {"key": "value"}

        error_message = "No response message received for the first message in the payload"

        with self.assertLogs(LongPollingTransport.__module__, level="WARNING") as log:
            with self.assertRaisesRegex(TransportError, error_message):
                await self.transport._send_final_payload(payload, headers=headers)

        # check log output
        assert any(error_message in entry for entry in log.output)
        # ensure expected methods were awaited
        response_mock.json.assert_awaited_with(loads=self.transport._json_loads)
        self.transport._consume_payload.assert_awaited_with(
            resp_data, headers=response_mock.headers, find_response_for=payload[0]
        )
