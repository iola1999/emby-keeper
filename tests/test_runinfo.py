import asyncio
import unittest

from loguru import logger

from embykeeper import runinfo
from embykeeper.runinfo import RunContext, RunStatus


class RunContextTest(unittest.TestCase):
    def setUp(self):
        runinfo._running_runs.clear()
        runinfo._completed_runs.clear()
        runinfo._children.clear()

    def tearDown(self):
        runinfo._running_runs.clear()
        runinfo._completed_runs.clear()
        runinfo._children.clear()

    def test_prepare_does_not_add_a_log_handler(self):
        handler_ids = set(logger._core.handlers)

        run = RunContext.prepare("test")

        self.assertEqual(handler_ids, set(logger._core.handlers))
        self.assertEqual([], run.log)

    def test_get_or_create_reuses_the_requested_id(self):
        first = RunContext.get_or_create("category.test")
        second = RunContext.get_or_create("category.test")

        self.assertIs(first, second)
        self.assertEqual("category.test", first.id)
        self.assertEqual(1, len(runinfo._running_runs))

    def test_run_releases_context_after_success(self):
        async def operation(ctx):
            ctx.start()
            return 42

        result = asyncio.run(RunContext.run(operation, description="test"))

        self.assertEqual(42, result)
        self.assertEqual({}, runinfo._running_runs)
        self.assertEqual(1, len(runinfo._completed_runs))
        completed = next(iter(runinfo._completed_runs.values()))
        self.assertEqual(RunStatus.SUCCESS, completed.status)
        self.assertEqual([], completed.log)


if __name__ == "__main__":
    unittest.main()
