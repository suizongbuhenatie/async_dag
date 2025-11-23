import asyncio
import logging
from typing import Iterable

from yakits import (
    PipelineContext,
    run_function_pipeline,
    run_function_pipeline_in_subprocess,
    setup_logging,
    get_task_logger,
)


async def simple_pipeline(ctx: PipelineContext, payload: str):
    task_logger = get_task_logger(ctx, name="demo.task")
    value = payload.strip()
    task_logger.info("start %s", value)

    if len(value) <= 4:
        await asyncio.sleep(0)
        return f"short::{value}"

    attempts = []
    for attempt in range(3):
        await asyncio.sleep(0)
        attempts.append(f"long::{value}#retry-{attempt}")

    return attempts


async def run_single_pipeline(source: Iterable[str]):
    results = await run_function_pipeline(source, simple_pipeline, concurrency=2)
    for line in results:
        logging.info("single process result :: %s", line)


def run_multi_process(batches: list[list[str]]):
    aggregated = run_function_pipeline_in_subprocess(
        simple_pipeline, batches, processes=len(batches)
    )
    for item in aggregated:
        logging.info("multiprocess result :: %s", item)


def main():
    setup_logging()
    asyncio.run(run_single_pipeline(["alpha", "beta", "elephant"]))
    run_multi_process([["cat", "dog"], ["eel", "fox", "giraffe"]])


if __name__ == "__main__":
    main()
