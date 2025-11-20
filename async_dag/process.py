from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
from multiprocessing.context import SpawnProcess
from queue import Empty
from typing import Any, Awaitable, Callable, Iterable, List, Sequence

from .context import PipelineContext
from .dag import DagPipeline
from .functional import run_function_pipeline
from .logging import setup_logging

logger = logging.getLogger(__name__)


def run_pipeline_in_subprocess(
    pipeline_factory: Callable[[], DagPipeline],
    data_batches: Sequence[Iterable[Any]],
    *,
    processes: int = 1,
) -> List[Any]:
    """Spawn worker processes, each running a pipeline over a data batch.

    ``pipeline_factory`` should build a fresh :class:`DagPipeline` (to avoid sharing
    non-picklable objects across forks). Each process streams its batch through the
    pipeline and writes sink outputs into a :class:`multiprocessing.Queue`. This
    function blocks until all processes finish and returns a consolidated list of
    outputs.
    """

    ctx = mp.get_context("spawn")
    output_queue: mp.Queue = ctx.Queue()
    batch_queue: mp.Queue = ctx.Queue()

    for batch in data_batches:
        batch_queue.put(batch)
    worker_count = max(1, min(processes, len(data_batches)))
    for _ in range(worker_count):
        batch_queue.put(None)

    procs: List[SpawnProcess] = []
    for _ in range(worker_count):
        proc = ctx.Process(
            target=_process_worker,
            args=(pipeline_factory, batch_queue, output_queue),
        )
        proc.daemon = True
        procs.append(proc)
        proc.start()

    results: List[Any] = []
    alive = len(procs)
    while alive:
        try:
            item = output_queue.get(timeout=0.5)
            if item is None:
                alive -= 1
                continue
            results.append(item)
        except Exception:
            alive = sum(1 for p in procs if p.is_alive())

    for proc in procs:
        proc.join()

    return results


def run_function_pipeline_in_subprocess(
    handler: Callable[[PipelineContext, Any], Awaitable[Any] | Any],
    data_batches: Sequence[Iterable[Any]],
    *,
    processes: int = 1,
) -> List[Any]:
    """Spawn worker processes for a plain function-based pipeline."""

    ctx = mp.get_context("spawn")
    output_queue: mp.Queue = ctx.Queue()
    batch_queue: mp.Queue = ctx.Queue()

    for batch in data_batches:
        batch_queue.put(batch)
    worker_count = max(1, min(processes, len(data_batches)))
    for _ in range(worker_count):
        batch_queue.put(None)

    procs: List[SpawnProcess] = []
    for _ in range(worker_count):
        proc = ctx.Process(
            target=_function_worker,
            args=(handler, batch_queue, output_queue),
        )
        proc.daemon = True
        procs.append(proc)
        proc.start()

    results: List[Any] = []
    alive = len(procs)
    while alive:
        try:
            item = output_queue.get(timeout=0.5)
            if item is None:
                alive -= 1
                continue
            results.append(item)
        except Exception:
            alive = sum(1 for p in procs if p.is_alive())

    for proc in procs:
        proc.join()

    return results


def _process_worker(
    pipeline_factory: Callable[[], DagPipeline],
    batch_queue: mp.Queue,
    output_queue: mp.Queue,
) -> None:
    setup_logging()

    while True:
        try:
            batch = batch_queue.get(timeout=0.5)
        except Empty:
            continue

        if batch is None:
            break

        pipeline = pipeline_factory()

        async def emit(value: Any, ctx: PipelineContext) -> None:
            output_queue.put({
                "run_id": ctx.run_id,
                "data_id": ctx.data_id,
                "node": ctx.current_node,
                "value": value,
            })

        asyncio.run(pipeline.run_stream(batch, result_handler=emit))

    output_queue.put(None)


def _function_worker(
    handler: Callable[[PipelineContext, Any], Awaitable[Any] | Any],
    batch_queue: mp.Queue,
    output_queue: mp.Queue,
) -> None:
    setup_logging()

    while True:
        try:
            batch = batch_queue.get(timeout=0.5)
        except Empty:
            continue

        if batch is None:
            break

        async def emit(value: Any, ctx: PipelineContext) -> None:
            output_queue.put(
                {
                    "run_id": ctx.run_id,
                    "data_id": ctx.data_id,
                    "node": ctx.current_node,
                    "value": value,
                }
            )

        asyncio.run(run_function_pipeline(batch, handler, result_handler=emit))

    output_queue.put(None)
