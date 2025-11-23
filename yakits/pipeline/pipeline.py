from typing import (
    Callable,
    Dict,
    Iterator,
    Tuple,
    List,
    Optional,
    Awaitable,
    Union,
    Set,
)
import os
import time
from multiprocessing import Queue, Process
from concurrent.futures import ThreadPoolExecutor, Future
from yakits import logger
from yakits.pipeline.context import Context
import inspect
import asyncio
import json
from functools import partial

type PipelineContexts = List[Context]
type PipelineFunc = Callable[[Context], None]
type SaveOpFunc = Callable[[Context], None]
type LoadOpFunc = Callable[[], Iterator[Tuple[int, Dict]]]
type PipelineContexts = List[Context]
type APipelineFunc = Callable[[Context], Awaitable[None]]
type SaveOpFunc = Callable[[Context], None]
type LoadOpFunc = Callable[[], Iterator[Tuple[int, Dict]]]


def async_worker_process(
    task_queue: Queue,
    result_queue: Queue,
    pipeline_func: APipelineFunc,
    *,
    max_retries: int = 3,
    batch_size: int = 10,
) -> None:
    loop = asyncio.get_event_loop()

    async def timed_pipeline_func(context: Context) -> None:
        context.start()
        await pipeline_func(context)
        context.finish()
        context.info(f"[task:{context.idx}] use {context.usage_time():.2f}s")

    pendding_contexts: PipelineContexts = []
    is_ready_to_finish = False
    while True:
        running_contexts: PipelineContexts = []
        tasks: List[Awaitable[None]] = []

        if not is_ready_to_finish and len(pendding_contexts) < batch_size:
            # if pendding contexts is more than batch_size, stop get data from task_queue
            try:
                contexts: Optional[PipelineContexts] = task_queue.get(timeout=3)
            except Exception as e:
                logger.error(
                    "get data from queue timeout, please check if producer is finished."
                )
                logger.exception(e)
                break

            if contexts is None:
                is_ready_to_finish = True
            else:
                for context in contexts:
                    tasks.append(timed_pipeline_func(context))
                    running_contexts.append(context)

        for context in pendding_contexts:
            tasks.append(timed_pipeline_func(context))
            running_contexts.append(context)

        logger.debug(
            f"[worker] ready to run {len(tasks)} tasks, {len(running_contexts)} contexts"
        )
        if is_ready_to_finish and len(tasks) == 0:
            break

        results = loop.run_until_complete(
            asyncio.gather(*tasks, return_exceptions=True)
        )

        finish_context: PipelineContexts = []
        pendding_contexts: PipelineContexts = []
        for context, result in zip(running_contexts, results):
            if isinstance(result, Exception):
                context.error(f"[task:{context.idx}] error: {result}")
                if context.retry_count < max_retries:
                    context.retry_count += 1
                    context.warning(
                        f"[task:{context.idx}] retry {context.retry_count} times"
                    )
                    pendding_contexts.append(context)
                else:
                    context.error(f"[task:{context.idx}] max retries reached")
            else:
                finish_context.append(context)

        logger.debug(
            f"[worker] finish {len(finish_context)}/{len(running_contexts)} tasks, pendding {len(pendding_contexts)}/{len(running_contexts)} tasks"
        )
        if len(finish_context) > 0:
            result_queue.put(finish_context)

    result_queue.put(None)


def producer_process(
    load_func: LoadOpFunc,
    task_queue: Queue,
    processed_ids: Set[int],
    worker_cnt: int,
    batch_size: int,
    run_debug: bool,
) -> None:
    iterator = load_func()
    batch: PipelineContexts = []
    for idx, doc in iterator:
        if idx in processed_ids:
            continue

        context: Context = Context(idx=idx, payload=doc, run_debug=run_debug)
        batch.append(context)
        logger.debug(f"[load] idx={context.idx} load finish")
        if len(batch) >= batch_size:
            logger.debug(f"[load] idx={context.idx} send")
            task_queue.put(batch)
            batch = []

        if run_debug:
            # in debug mode, only run one batch
            break

    if len(batch) > 0:
        task_queue.put(batch)

    for _ in range(worker_cnt):
        task_queue.put(None)


def sync_worker_process(
    task_queue: Queue,
    result_queue: Queue,
    pipeline_func: PipelineFunc,
    *,
    thread_cnt: int = 1,
    max_retries: int = 3,
    batch_size: int = 10,
) -> None:
    def timed_pipeline_func(context: Context) -> None:
        context.start()
        logger.debug(f"[task:{context.idx}] start")
        pipeline_func(context)
        context.finish()
        context.info(f"[task:{context.idx}] use {context.usage_time():.2f}s")

    with ThreadPoolExecutor(max_workers=thread_cnt) as executor:
        pendding_contexts: PipelineContexts = []
        is_ready_to_finish = False
        while True:
            tasks: List[Future[None]] = []
            if not is_ready_to_finish and len(pendding_contexts) < batch_size:
                try:
                    contexts: Optional[PipelineContexts] = task_queue.get(timeout=3)
                except Exception as e:
                    logger.error(
                        "get data from queue timeout, please check if producer is finished."
                    )
                    logger.exception(e)
                    break

                if contexts is None:
                    is_ready_to_finish = True
                else:
                    for context in contexts:
                        tasks.append(
                            (context, executor.submit(timed_pipeline_func, context))
                        )

            for context in pendding_contexts:
                tasks.append((context, executor.submit(timed_pipeline_func, context)))

            logger.debug(f"[worker] running {len(tasks)} tasks")

            if is_ready_to_finish and len(tasks) == 0:
                break

            finish_context: PipelineContexts = []
            pendding_contexts: PipelineContexts = []
            for context, task in tasks:
                try:
                    task.result()
                    finish_context.append(context)
                except Exception as e:
                    context.error(f"[task:{context.idx}] error: {e}")
                    if context.retry_count < max_retries:
                        context.retry_count += 1
                        context.warning(
                            f"[task:{context.idx}] retry {context.retry_count} times"
                        )
                        pendding_contexts.append(context)
                    else:
                        context.error(f"[task:{context.idx}] max retries reached")

            logger.debug(f"[worker] finish {len(contexts)} tasks")
            result_queue.put(finish_context)

    result_queue.put(None)


def _default_save_func(context: Context, output_path: str) -> None:
    task_info_path = f"{output_path}.task_info"
    with (
        open(task_info_path, "a+", buffering=1) as task_info_f,
        open(output_path, "a+", buffering=1) as f,
    ):
        f.write(f"{context.idx}\t{json.dumps(context.payload)}\n")
        task_info_f.write(f"{context.idx}\t{context.usage_time():.2f}s\n")


def resume_from_task_info(output_path: str) -> Set[int]:
    task_info_path = f"{output_path}.task_info"

    if not os.path.exists(task_info_path):
        return set()

    try:
        with open(task_info_path, "r") as f:
            processed_ids = {int(line.split("\t")[0]) for line in f}
        logger.info(f"resume from {task_info_path}, get {len(processed_ids)} records")
        return processed_ids
    except Exception as e:
        logger.error(f"error while resume from {task_info_path}: {e}")
        raise e


def run_pipeline(
    load_func: LoadOpFunc,
    pipeline_func: Union[PipelineFunc, APipelineFunc],
    *,
    save_func: Optional[SaveOpFunc] = None,
    output_path: Optional[str] = None,
    process_cnt: int = 1,
    thread_cnt: int = 1,
    buffer_size: int = 100,
    batch_size: int = 4,
    max_retries: int = 3,
    resume: bool = True,
    run_debug: bool = False,
) -> None:
    """
    Execute a data-processing pipeline with configurable parallelism.

    Parameters
    ----------
    load_func : LoadOpFunc
        A generator that yields (index, payload) pairs. Each payload becomes
        the input document for one Context.
    save_func : SaveOpFunc, optional
        A callable that receives a finished Context and persists its results.
        Exactly one of `save_func` or `output_path` must be provided.
    output_path : str, optional
        If given, results are automatically appended to this file (and a
        side-car *.task_info file) using the built-in `_default_save_func`.
        Exactly one of `save_func` or `output_path` must be provided.
    pipeline_func : Union[PipelineFunc, APipelineFunc]
        The user-supplied transformation logic. It can be a regular function
        (runs in thread pool) or an async coroutine function (runs in an event
        loop inside each worker process).
    process_cnt : int, default 1
        Number of worker processes to spawn. Each process hosts either an async
        event loop or a thread pool.
    thread_cnt : int, default 1
        Threads per worker process when `pipeline_func` is *not* a coroutine
        function. Ignored for async functions.
    buffer_size : int, default 100
        Maximum number of batches that can be queued between producer and
        workers (and between workers and consumer). Helps to bound memory.
    batch_size : int, default 4
        Number of Context objects sent to a worker in a single batch.
        Must be ≥ thread_cnt for sync functions.
    max_retries : int, default 3
        Maximum retry attempts for a single Context before it is dropped.
    resume : bool, default True
        If True, skip indices already recorded in the task-info file
        (created when `output_path` is used).

    Returns
    -------
    None
        The function blocks until all data is processed and all worker processes
        have terminated.

    Notes
    -----
    - When `pipeline_func` is an async coroutine function, each worker process
      runs its own asyncio event loop; `thread_cnt` is ignored.
    - When `pipeline_func` is a regular function, each worker process spawns
      `thread_cnt` threads to execute tasks concurrently.
    - Retries are handled locally inside each worker; failed records after
      exceeding `max_retries` are permanently dropped.
    """
    assert batch_size >= thread_cnt, (
        f"batch_size {batch_size} must be greater than or equal to thread_cnt {thread_cnt}"
    )
    assert save_func is not None or output_path is not None, (
        "save_func or output_path must be provided"
    )
    if output_path is not None:
        assert save_func is None, (
            "save_func and output_path cannot be provided at the same time"
        )
        save_func = partial(_default_save_func, output_path=output_path)

    if resume and run_debug is False:
        processed_ids = resume_from_task_info(output_path)
    else:
        processed_ids = set()

    if run_debug:
        logger.info("run in debug mode, only process one batch")
        process_cnt = 1
        thread_cnt = 1
        batch_size = 1

    t0 = time.time()
    task_queue: Queue[Optional[PipelineContexts]] = Queue(maxsize=buffer_size)
    result_queue: Queue[Optional[PipelineContexts]] = Queue(maxsize=buffer_size)

    producer = Process(
        target=producer_process,
        args=(load_func, task_queue, processed_ids, process_cnt, batch_size, run_debug),
    )
    producer.start()

    workers: List[Process] = []
    for _ in range(process_cnt):
        if inspect.iscoroutinefunction(pipeline_func):
            p = Process(
                target=async_worker_process,
                args=(task_queue, result_queue, pipeline_func),
                kwargs={"max_retries": max_retries, "batch_size": batch_size},
            )
        else:
            p = Process(
                target=sync_worker_process,
                args=(task_queue, result_queue, pipeline_func),
                kwargs={
                    "max_retries": max_retries,
                    "batch_size": batch_size,
                    "thread_cnt": thread_cnt,
                },
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
