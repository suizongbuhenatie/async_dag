# async_dag

An asyncio-first pipeline toolkit for building streaming data refresh jobs. You
can write pipelines as plain async functions that use normal ``await`` +
``if``/``for`` control flow, or drop down to a DAG builder when you want
explicit branching graphs.

## Features
- **Async by default**: Each node runs as an asyncio worker with configurable concurrency.
- **Queue-driven streaming**: Inputs and outputs flow through `asyncio.Queue` to keep
  memory usage bounded while processing large streams.
- **Context + tracing**: A `PipelineContext` carries run IDs, data IDs, and node paths
  so logs can be correlated across a run.
- **Plain async pipelines**: Define pipelines as a single async function with normal
  Python ``if``/``for`` logic—no DAG classes required.
- **Branching + loops**: When you need explicit graphs, add conditional edges and
  bounded loop edges directly in code to express `if`/`else` logic and retry-like
  behavior.
- **Multiprocess ready**: Launch multiple processes, each running its own pipeline
  instance over a batch of data using `run_function_pipeline_in_subprocess` or
  `run_pipeline_in_subprocess`.
- **Composable DAG definition**: When you want graph-style fan-out/fan-in, declare
  tasks and connect edges with predicates via the imperative `PipelineBuilder`.

## Quick start
Install dependencies (Python 3.12+):

```bash
pip install -e .
```

Run the demo pipeline:

```bash
python main.py
```

Example **plain async** pipeline definition (see `main.py`):

```python
import asyncio

from async_dag import PipelineContext, run_function_pipeline

async def pipeline(ctx: PipelineContext, payload: str):
    value = payload.strip()
    if len(value) <= 4:
        return f"short::{value}"

    for attempt in range(3):
        await asyncio.sleep(0)
        if attempt >= 1:
            yield f"long::{value}#retry-{attempt}"

results = await run_function_pipeline(["alpha", "beta", "gamma"], pipeline)
```

Or fan out batches across processes (each process builds its own pipeline):

```python
from async_dag import run_function_pipeline_in_subprocess

batches = [["cat", "dog"], ["eel", "fox"]]
outputs = run_function_pipeline_in_subprocess(pipeline, batches, processes=len(batches))
```

## Concepts
- **`DagTask`**: Node in the graph. Provide an ID, a handler (sync or async), and
  optional per-node concurrency.
- **`TaskTransition`**: Directed edge between tasks with optional predicate and loop
  guards.
- **`DagPipeline`**: Owns the DAG topology and orchestrates queue-based execution.
- **`PipelineContext`**: Metadata that flows with each payload for logging and tracing.
- **Result handling**: If a node has no downstream dependencies, its outputs are sinks.
  Pass a `result_handler` to `run_stream` to consume sink outputs as they arrive; otherwise
  they are collected and returned.

## Logging
Use `setup_logging()` for a simple formatter. The `log_with_context` helper attaches
`run_id`, `data_id`, and the processed node path to log records. Additional debug logs
are emitted when payloads are routed to specific branches or looped for retries.
