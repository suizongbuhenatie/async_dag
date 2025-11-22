from typing import Callable, Dict, Iterable, Iterator, Any, Tuple, List
from multiprocessing import Queue, Process, current_process
import time
from async_dag.logger import logger
import multiprocessing
from async_dag.context import Context

multiprocessing.set_start_method("fork")

type PipelineContexts = Iterable[Context]
type PipelineFunc = Callable[PipelineContexts, None]
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
        if len(batch) >= batch_size:
            task_queue.put(batch)
            batch = []

    if len(batch) > 0:
        task_queue.put(batch)

    for _ in range(worker_cnt):
        task_queue.put(None)


def worker_process(
    task_queue: Queue, result_queue: Queue, pipeline_func: PipelineFunc
):
    while True:
        context = task_queue.get()
        if context is None:
            result_queue.put(None)
            break

        pipeline_func(context)
        result_queue.put(context)


def run_pipeline(
    load_func: LoadOpFunc,
    save_func: SaveOpFunc,
    pipeline_func: PipelineFunc,
    worker: int = 4,
    buffer_size: int = 100,
    batch_size: int = 4,
):
    task_queue = Queue(maxsize=buffer_size)
    result_queue = Queue(maxsize=buffer_size)

    producer = Process(target=producer_process, args=(load_func, task_queue, worker, batch_size))
    producer.start()

    workers: List[Process] = []
    for _ in range(worker):
        p = Process(
            target=worker_process, args=(task_queue, result_queue, pipeline_func)
        )
        p.start()
        workers.append(p)

    producer.join()

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

    logger.info("Pipeline processing complete.")
