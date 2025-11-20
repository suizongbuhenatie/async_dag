import asyncio
import logging
import unittest

from async_dag import DagPipeline, PipelineBuilder, PipelineContext, run_function_pipeline


class TestFunctionalPipeline(unittest.IsolatedAsyncioTestCase):
    async def test_function_pipeline_collects_results(self):
        async def handler(ctx: PipelineContext, value: int):
            await asyncio.sleep(0)
            if value % 2 == 0:
                return [value, value * 2]
            return value * 3

        results = await run_function_pipeline([1, 2, 3], handler, concurrency=2, run_id="run")

        self.assertCountEqual(results, [3, 2, 4, 9])


class TestDagPipeline(unittest.IsolatedAsyncioTestCase):
    async def test_branching_routes_to_expected_tasks(self):
        builder = PipelineBuilder()

        async def entry(ctx: PipelineContext, value: int):
            return value + 1

        async def even_handler(ctx: PipelineContext, value: int):
            return f"even:{value}"

        async def odd_handler(ctx: PipelineContext, value: int):
            return f"odd:{value}"

        builder.task("incr", entry)
        builder.task("even", even_handler)
        builder.task("odd", odd_handler)

        builder.edge("incr", "even", predicate=lambda _ctx, v: v % 2 == 0)
        builder.edge("incr", "odd", predicate=lambda _ctx, v: v % 2 != 0)

        pipeline = builder.build(run_id="branch-test")

        routed: list[str] = []
        await pipeline.run_stream([1, 2, 3], result_handler=lambda v, _ctx: routed.append(v))

        self.assertCountEqual(routed, ["even:2", "odd:3", "even:4"])

    async def test_self_loop_routes_until_guard_then_exits(self):
        async def loop_handler(ctx: PipelineContext, value: int):
            ctx.meta["seen"] = ctx.meta.get("seen", 0) + 1
            return value

        async def sink_handler(ctx: PipelineContext, value: int):
            return f"done:{value}:seen-{ctx.meta.get('seen')}"

        builder = PipelineBuilder()
        builder.task("loop", loop_handler)
        builder.task("sink", sink_handler)

        # Re-enqueue the payload twice before sending it to the sink.
        builder.edge("loop", "loop", predicate=lambda ctx, _v: ctx.meta.get("seen", 0) < 3)
        builder.edge("loop", "sink", predicate=lambda ctx, _v: ctx.meta.get("seen", 0) >= 3)

        pipeline = builder.build(run_id="loop-guard")

        outputs: list[str] = []
        await pipeline.run_stream([1], result_handler=lambda v, _ctx: outputs.append(v))

        self.assertEqual(outputs, ["done:1:seen-3"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
