from typing import Callable, Dict, Iterator, Tuple, List, Optional, Awaitable
from multiprocessing import Queue, Process
from async_dag.logger import logger
from async_dag.context import Context
import asyncio
from async_dag.sync_pipeline import producer_process
import time


type PipelineContexts = List[Context]
type APipelineFunc = Callable[[Context], Awaitable[None]]
type SaveOpFunc = Callable[[Context], None]
type LoadOpFunc = Callable[[], Iterator[Tuple[int, Dict]]]


def worker_process(
    task_queue: Queue[Optional[PipelineContexts]],
    result_queue: Queue[Optional[PipelineContexts]],
    pipeline_func: APipelineFunc,
) -> None:
    loop = asyncio.get_event_loop()

    async def timed_pipeline_func(context: Context) -> None:
        context.start()
        await pipeline_func(context)
        context.finish()
        context.info(f"[task:{context.idx}] use {context.usage_time():.2f}s")

    while True:
        contexts: Optional[PipelineContexts] = task_queue.get()
        if contexts is None:
            result_queue.put(None)
            break

        tasks: List[Awaitable[None]] = []
        for context in contexts:
            tasks.append(timed_pipeline_func(context))

        loop.run_until_complete(asyncio.gather(*tasks))
        result_queue.put(contexts)


def run_async_pipeline(
    load_func: LoadOpFunc,
    save_func: SaveOpFunc,
    pipeline_func: APipelineFunc,
    worker: int = 4,
    buffer_size: int = 100,
    batch_size: int = 4,
) -> None:
    t0 = time.time()

    task_queue: Queue[Optional[PipelineContexts]] = Queue(maxsize=buffer_size)
    result_queue: Queue[Optional[PipelineContexts]] = Queue(maxsize=buffer_size)

    producer = Process(
        target=producer_process, args=(load_func, task_queue, worker, batch_size)
    )
    producer.start()

    workers: List[Process] = []
    for _ in range(worker):
        p = Process(
            target=worker_process, args=(task_queue, result_queue, pipeline_func)
        )
        p.start()
        workers.append(p)

    done_workers = 0
    while True:
        contexts = result_queue.get()
        if contexts is None:
            done_workers += 1
            if done_workers >= worker:
                break
            continue
        for context in contexts:
            save_func(context)

    # Clean up worker processes
    for p in workers:
        p.join()

    producer.join()

    logger.info(f"Pipeline processing complete. cost {time.time() - t0:.2f}s")
