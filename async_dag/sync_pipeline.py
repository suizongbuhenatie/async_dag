from typing import Callable, Dict, Iterable, Iterator, Tuple, List
import time
from multiprocessing import Queue, Process
from concurrent.futures import ThreadPoolExecutor
from async_dag.logger import logger
from async_dag.context import Context
from typing import Optional

type PipelineContexts = Iterable[Context]
type PipelineFunc = Callable[Context, None]
type SaveOpFunc = Callable[Context, None]
type LoadOpFunc = Callable[[], Iterator[Tuple[int, Dict]]]


def producer_process(
    load_func: LoadOpFunc,
    task_queue: Queue,
    worker_cnt: int,
    batch_size: int,
):
    iterator = load_func()
    batch: PipelineContexts = []
    for idx, doc in iterator:
        context: Context = Context(idx=idx, payload=doc)
        batch.append(context)
        logger.debug(f"[load] idx={context.idx} load finish")
        if len(batch) >= batch_size:
            logger.debug(f"[load] idx={context.idx} send")
            task_queue.put(batch)
            batch = []

    if len(batch) > 0:
        task_queue.put(batch)

    for _ in range(worker_cnt):
        task_queue.put(None)


def worker_process(
    task_queue: Queue, result_queue: Queue, pipeline_func: PipelineFunc, thread_cnt: int
):
    def timed_pipeline_func(context: Context):
        context.start()
        logger.debug(f"[task:{context.idx}] start")
        pipeline_func(context)
        context.finish()
        context.info(f"[task:{context.idx}] use {context.usage_time():.2f}s")

    with ThreadPoolExecutor(max_workers=thread_cnt) as executor:
        while True:
            contexts: Optional[PipelineContexts] = task_queue.get()
            if contexts is None:
                result_queue.put(None)
                break
            logger.debug(f"[worker] get {len(contexts)} tasks")

            tasks = []
            for context in contexts:
                tasks.append(executor.submit(timed_pipeline_func, context))

            for task in tasks:
                task.result()

            logger.debug(f"[worker] finish {len(contexts)} tasks")
            result_queue.put(contexts)


def run_sync_pipeline(
    load_func: LoadOpFunc,
    save_func: SaveOpFunc,
    pipeline_func: PipelineFunc,
    process_cnt: int = 1,
    thread_cnt: int = 1,
    buffer_size: int = 100,
    batch_size: int = 4,
):
    assert batch_size >= thread_cnt, (
        f"batch_size {batch_size} must be greater than or equal to thread_cnt {thread_cnt}"
    )
    t0 = time.time()
    task_queue = Queue(maxsize=buffer_size)
    result_queue = Queue(maxsize=buffer_size)

    producer = Process(
        target=producer_process, args=(load_func, task_queue, process_cnt, batch_size)
    )
    producer.start()

    workers: List[Process] = []
    for _ in range(process_cnt):
        p = Process(
            target=worker_process,
            args=(task_queue, result_queue, pipeline_func, thread_cnt),
        )
        p.start()
        workers.append(p)

    done_workers = 0
    while True:
        contexts = result_queue.get()
        if contexts is None:
            done_workers += 1
            if done_workers >= process_cnt:
                break
            continue
        for context in contexts:
            save_func(context)

    # Clean up worker processes
    for p in workers:
        p.join()

    producer.join()

    logger.info(f"Pipeline processing complete. cost {time.time() - t0:.2f}s")
