from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from typing import Any, AsyncIterable, Awaitable, Callable, Iterable, Mapping

from .context import PipelineContext
from .logging import get_task_logger, log_with_context

logger = logging.getLogger(__name__)

Handler = Callable[[PipelineContext, Any], Awaitable[Any] | Any]
ResultHandler = Callable[[Any, PipelineContext], Awaitable[None] | None]


async def run_function_pipeline(
    source: AsyncIterable[Any] | Iterable[Any],
    handler: Handler,
    *,
    result_handler: ResultHandler | None = None,
    concurrency: int = 1,
    run_id: str | None = None,
) -> list[Any]:
    """Stream data through a plain async function pipeline.

    ``handler`` is a user-defined coroutine that can use normal Python control
    flow (``if``/``for``/``while``) and ``await`` without any DAG declarations.
    The function receives a :class:`PipelineContext` for tracing and should
    return either a single value, an iterable, or an async iterable. Returned
    values that are not consumed by ``result_handler`` are collected and
    returned at the end of the run.
    """

    sentinel = object()
    run_results: list[Any] = []
    run_id = run_id or str(uuid.uuid4())

    queue: asyncio.Queue = asyncio.Queue()

    async def handle_result(output: Any, ctx: PipelineContext) -> None:
        if result_handler:
            maybe = result_handler(output, ctx)
            if inspect.isawaitable(maybe):
                await maybe
        else:
            run_results.append(output)

    async def worker(worker_id: int) -> None:
        while True:
            item = await queue.get()
            if item is sentinel:
                queue.task_done()
                break

            idx, payload = item
            ctx = PipelineContext(run_id=run_id).with_data_id(str(idx)).for_node(
                f"worker-{worker_id}"
            )
            task_logger = get_task_logger(ctx, name=logger.name)
            task_logger.info("processing")
            try:
                result = handler(ctx, payload)
                if inspect.isawaitable(result):
                    result = await result

                async for value in _iterate_outputs(result):
                    task_logger.info("emitting result")
                    await handle_result(value, ctx)
            except Exception:
                task_logger.exception("Unhandled exception in pipeline function")
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker(i)) for i in range(max(1, concurrency))]

    async for idx, payload in _with_index(source):
        await queue.put((idx, payload))

    for _ in workers:
        await queue.put(sentinel)

    await queue.join()
    await asyncio.gather(*workers)
    return run_results


async def _with_index(source: AsyncIterable[Any] | Iterable[Any]):
    if inspect.isasyncgen(source) or inspect.isawaitable(source):
        async_iter = source
    elif inspect.isasyncgenfunction(source):
        async_iter = source()
    elif inspect.iscoroutinefunction(source):
        async_iter = source()
    elif hasattr(source, "__aiter__"):
        async_iter = source
    else:
        async def async_iterable():
            for item in source:
                yield item

        async_iter = async_iterable()

    idx = 0
    async for item in async_iter:
        yield idx, item
        idx += 1


async def _iterate_outputs(result: Any) -> AsyncIterable[Any]:
    if result is None:
        return
    if hasattr(result, "__aiter__"):
        async for item in result:
            yield item
        return
    if inspect.isasyncgen(result):
        async for item in result:
            yield item
        return
    if isinstance(result, Iterable) and not isinstance(result, (str, bytes, Mapping)):
        for item in result:
            yield item
        return
    yield result
