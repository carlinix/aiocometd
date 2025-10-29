import asyncio
import unittest
from unittest import mock

from aiocometd.utils import (
    get_error_message,
    get_error_code,
    get_error_args,
    defer,
    is_auth_error_message,
    is_event_message,
    is_server_error_message,
    is_matching_response,
)
from aiocometd.constants import MetaChannel, SERVICE_CHANNEL_PREFIX


class TestGetErrorCode(unittest.TestCase):
    def test_get_error_code(self):
        self.assertEqual(get_error_code("123::"), 123)

    def test_get_error_code_none_field(self):
        self.assertIsNone(get_error_code(None))

    def test_get_error_code_empty_field(self):
        self.assertIsNone(get_error_code(""))

    def test_get_error_code_invalid_field(self):
        self.assertIsNone(get_error_code("invalid"))

    def test_get_error_code_short_invalid_field(self):
        self.assertIsNone(get_error_code("12::"))

    def test_get_error_code_empty_code_field(self):
        self.assertIsNone(get_error_code("::"))


class TestGetErrorMessage(unittest.TestCase):
    def test_get_error_message(self):
        self.assertEqual(get_error_message("::message"), "message")

    def test_get_error_message_none_field(self):
        self.assertIsNone(get_error_message(None))

    def test_get_error_message_empty_field(self):
        self.assertIsNone(get_error_message(""))

    def test_get_error_message_invalid_field(self):
        self.assertIsNone(get_error_message("invalid"))

    def test_get_error_message_empty_code_field(self):
        self.assertEqual(get_error_message("::"), "")


class TestGetErrorArgs(unittest.TestCase):
    def test_get_error_args(self):
        field = "403:xj3sjdsjdsjad,/foo/bar:Subscription denied"
        self.assertEqual(get_error_args(field), ["xj3sjdsjdsjad", "/foo/bar"])

    def test_get_error_args_none_field(self):
        self.assertIsNone(get_error_args(None))

    def test_get_error_args_empty_field(self):
        self.assertIsNone(get_error_args(""))

    def test_get_error_args_invalid_field(self):
        self.assertIsNone(get_error_args("invalid"))

    def test_get_error_args_empty_code_field(self):
        self.assertEqual(get_error_args("::"), [])


class TestDefer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        async def coro_func(value):
            return value

        self.coro_func = coro_func
        self.loop = asyncio.get_event_loop()

    @mock.patch("aiocometd.utils.asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_defer(self, sleep):
        arg, delay = object(), 10
        wrapper = defer(self.coro_func, delay)
        result = await wrapper(arg)
        self.assertIs(result, arg)
        sleep.assert_awaited_with(delay)

    @mock.patch("aiocometd.utils.asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_defer_no_loop(self, sleep):
        arg, delay = object(), 10
        wrapper = defer(self.coro_func, delay)
        result = await wrapper(arg)
        self.assertIs(result, arg)
        sleep.assert_awaited_with(delay)

    @mock.patch("aiocometd.utils.asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_defer_none_delay(self, sleep):
        arg = object()
        wrapper = defer(self.coro_func)
        result = await wrapper(arg)
        self.assertIs(result, arg)
        sleep.assert_not_called()

    @mock.patch("aiocometd.utils.asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_defer_zero_delay(self, sleep):
        arg, delay = object(), 0
        wrapper = defer(self.coro_func, delay)
        result = await wrapper(arg)
        self.assertIs(result, arg)
        sleep.assert_not_called()

    @mock.patch("aiocometd.utils.asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_defer_sleep_canceled(self, sleep):
        arg, delay = object(), 10
        sleep.side_effect = asyncio.CancelledError()
        wrapper = defer(self.coro_func, delay)
        with self.assertRaises(asyncio.CancelledError):
            await wrapper(arg)
        sleep.assert_awaited_with(delay)


class TestIsAuthErrorMessage(unittest.TestCase):
    @mock.patch("aiocometd.utils.get_error_code")
    def test_is_auth_error_message(self, get_error_code):
        get_error_code.return_value = 401
        self.assertTrue(is_auth_error_message({"error": "err"}))

    @mock.patch("aiocometd.utils.get_error_code")
    def test_is_auth_error_message_forbidden(self, get_error_code):
        get_error_code.return_value = 403
        self.assertTrue(is_auth_error_message({"error": "err"}))

    @mock.patch("aiocometd.utils.get_error_code")
    def test_is_auth_error_message_not_auth(self, get_error_code):
        get_error_code.return_value = 400
        self.assertFalse(is_auth_error_message({"error": "err"}))

    @mock.patch("aiocometd.utils.get_error_code")
    def test_is_auth_error_message_no_error(self, get_error_code):
        get_error_code.return_value = None
        self.assertFalse(is_auth_error_message({}))


class TestIsEventMessage(unittest.TestCase):
    def assert_event(self, channel, has_data, has_id, expected):
        msg = {"channel": channel}
        if has_data:
            msg["data"] = None
        if has_id:
            msg["id"] = None
        self.assertEqual(is_event_message(msg), expected)

    def test_cases(self):
        for ch in [
            MetaChannel.SUBSCRIBE,
            MetaChannel.UNSUBSCRIBE,
            MetaChannel.HANDSHAKE,
            MetaChannel.CONNECT,
            MetaChannel.DISCONNECT,
        ]:
            self.assert_event(ch, False, False, False)
            self.assert_event(ch, True, False, False)
            self.assert_event(ch, False, True, False)
            self.assert_event(ch, True, True, False)

        ch = "/test/channel"
        self.assert_event(ch, False, False, False)
        self.assert_event(ch, True, False, True)
        self.assert_event(ch, False, True, False)
        self.assert_event(ch, True, True, True)

        ch = SERVICE_CHANNEL_PREFIX + "svc"
        self.assert_event(ch, False, False, False)
        self.assert_event(ch, True, False, True)
        self.assert_event(ch, False, True, False)
        self.assert_event(ch, True, True, False)


class TestIsServerErrorMessage(unittest.TestCase):
    def test_successful(self):
        self.assertFalse(is_server_error_message({"successful": True}))

    def test_not_successful(self):
        self.assertTrue(is_server_error_message({"successful": False}))

    def test_missing_flag(self):
        self.assertFalse(is_server_error_message({}))


class TestIsMatchingResponse(unittest.TestCase):
    def test_full_match(self):
        msg = {"channel": "/a", "data": {}, "clientId": "c", "id": "1"}
        resp = {"channel": "/a", "successful": True, "clientId": "c", "id": "1"}
        self.assertTrue(is_matching_response(resp, msg))

    def test_response_none(self):
        msg = {"channel": "/a", "data": {}, "clientId": "c", "id": "1"}
        self.assertFalse(is_matching_response(None, msg))

    def test_message_none(self):
        resp = {"channel": "/a", "successful": True, "clientId": "c", "id": "1"}
        self.assertFalse(is_matching_response(resp, None))

    def test_without_id(self):
        msg = {"channel": "/a", "data": {}, "clientId": "c"}
        resp = {"channel": "/a", "successful": True, "clientId": "c"}
        self.assertTrue(is_matching_response(resp, msg))

    def test_different_id(self):
        msg = {"channel": "/a", "data": {}, "clientId": "c", "id": "1"}
        resp = {"channel": "/a", "successful": True, "clientId": "c", "id": "2"}
        self.assertFalse(is_matching_response(resp, msg))

    def test_different_channel(self):
        msg = {"channel": "/a", "data": {}, "clientId": "c", "id": "1"}
        resp = {"channel": "/b", "successful": True, "clientId": "c", "id": "1"}
        self.assertFalse(is_matching_response(resp, msg))

    def test_without_successful_flag(self):
        msg = {"channel": "/a", "data": {}, "clientId": "c", "id": "1"}
        resp = {"channel": "/a", "clientId": "c", "id": "1"}
        self.assertFalse(is_matching_response(resp, msg))
