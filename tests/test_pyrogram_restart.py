import asyncio
import unittest
from unittest.mock import patch

import pyrogram.client as pyrogram_client_module
from pyrogram.session.session import Session as PyrogramSession, SessionState

from embykeeper.telegram.pyrogram import ManagedSession


class ManagedSessionTest(unittest.IsolatedAsyncioTestCase):
    def make_session(self):
        session = object.__new__(ManagedSession)
        session._restarts_enabled = True
        session._restart_scheduled = False
        session._restart_task = None
        session._state = SessionState.STOPPED
        session.restart_lock = asyncio.Lock()
        session.stored_msg_ids = []
        session.recent_msg_ids = []
        session.is_started = asyncio.Event()
        session.RETRY_DELAY = 0
        return session

    async def test_restart_requests_are_coalesced(self):
        session = self.make_session()
        start_entered = asyncio.Event()
        release_start = asyncio.Event()
        start_calls = 0

        async def fake_stop(_):
            return None

        async def fake_start(instance):
            nonlocal start_calls
            start_calls += 1
            start_entered.set()
            await release_start.wait()
            instance._state = SessionState.STARTED
            instance.is_started.set()

        with patch.object(PyrogramSession, "stop", fake_stop), patch.object(
            PyrogramSession, "start", fake_start
        ):
            tasks = [asyncio.create_task(session.restart()) for _ in range(1000)]
            await start_entered.wait()
            await asyncio.sleep(0)

            self.assertEqual(1, start_calls)
            self.assertEqual(1, sum(not task.done() for task in tasks))

            release_start.set()
            await asyncio.gather(*tasks)

        self.assertFalse(session._restart_scheduled)

    async def test_failed_start_retries_in_the_same_task(self):
        session = self.make_session()
        start_calls = 0

        async def fake_stop(instance):
            instance._state = SessionState.STOPPED

        async def fake_start(instance):
            nonlocal start_calls
            start_calls += 1
            if start_calls == 1:
                instance._state = SessionState.STARTING
                return
            instance._state = SessionState.STARTED
            instance.is_started.set()

        with patch.object(PyrogramSession, "stop", fake_stop), patch.object(
            PyrogramSession, "start", fake_start
        ):
            await session.restart()

        self.assertEqual(2, start_calls)
        self.assertFalse(session._restart_scheduled)

    async def test_disable_restarts_cancels_the_worker(self):
        session = self.make_session()
        start_entered = asyncio.Event()

        async def fake_stop(_):
            return None

        async def fake_start(_):
            start_entered.set()
            await asyncio.Event().wait()

        with patch.object(PyrogramSession, "stop", fake_stop), patch.object(
            PyrogramSession, "start", fake_start
        ):
            task = asyncio.create_task(session.restart())
            await start_entered.wait()
            await session.disable_restarts()

        self.assertTrue(task.cancelled())
        self.assertFalse(session._restart_scheduled)

    def test_client_uses_managed_sessions(self):
        self.assertIs(ManagedSession, pyrogram_client_module.Session)


if __name__ == "__main__":
    unittest.main()
