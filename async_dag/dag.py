from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, AsyncIterable, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional

from .context import PipelineContext
from .logging import log_with_context

logger = logging.getLogger(__name__)


Payload = Any
Handler = Callable[[PipelineContext, Payload], Awaitable[Any] | Any]
Predicate = Callable[[PipelineContext, Payload], Awaitable[bool] | bool]


@dataclass
class TaskTransition:
    """Describes a routed edge from one task to another.

    ``predicate`` allows if/else style branching. ``max_loops`` provides
    a guardrail for looped edges to prevent runaway retries.
    """

    target: str
    predicate: Optional[Predicate] = None
    name: Optional[str] = None
    max_loops: Optional[int] = None


@dataclass
class DagTask:
    """Represents a processing step in the DAG."""

    id: str
    handler: Handler
    transitions: List[TaskTransition] = field(default_factory=list)
    concurrency: int = 1


class DagPipeline:
    """Async DAG pipeline with queue-based streaming semantics."""

    def __init__(self, tasks: Iterable[DagTask], run_id: Optional[str] = None):
        self.tasks: Dict[str, DagTask] = {task.id: task for task in tasks}
        self.run_id = run_id or str(uuid.uuid4())

        if len(self.tasks) == 0:
            raise ValueError("At least one task is required")
        if len(self.tasks) != len(set(self.tasks.keys())):
            raise ValueError("Task ids must be unique")

        self._downstream: Dict[str, List[str]] = defaultdict(list)
        self._upstream: Dict[str, List[str]] = defaultdict(list)
        self._expected_completions: Dict[str, int] = {}

        for task in self.tasks.values():
            for transition in task.transitions:
                if transition.target not in self.tasks:
                    raise ValueError(
                        f"Unknown transition target '{transition.target}' from '{task.id}'"
                    )
                if transition.target != task.id:
                    self._downstream[task.id].append(transition.target)
                    self._upstream[transition.target].append(task.id)
            self._expected_completions[task.id] = len(self._upstream[task.id]) or 1

        for task_id in self.tasks:
            if task_id not in self._expected_completions:
                self._expected_completions[task_id] = len(self._upstream.get(task_id, [])) or 1

    @property
    def sources(self) -> List[str]:
        return [task_id for task_id in self.tasks if len(self._upstream.get(task_id, [])) == 0]

    @property
    def sinks(self) -> List[str]:
        return [task_id for task_id in self.tasks if len(self._downstream.get(task_id, [])) == 0]

    async def run_stream(
        self,
        source: AsyncIterable[Any] | Iterable[Any],
        *,
        result_handler: Callable[[Payload, PipelineContext], Awaitable[None] | None] | None = None,
    ) -> List[Any]:
        """Execute the pipeline with a streaming source."""

        sentinel = object()
        run_results: List[Any] = []

        queues: Dict[str, asyncio.Queue] = {
            task_id: asyncio.Queue() for task_id in self.tasks
        }
        upstream_done_counts: Dict[str, int] = {task_id: 0 for task_id in self.tasks}

        async def handle_result(output: Payload, ctx: PipelineContext) -> None:
            if result_handler:
                maybe_awaitable = result_handler(output, ctx)
                if inspect.isawaitable(maybe_awaitable):
                    await maybe_awaitable
            else:
                run_results.append(output)

        async def maybe_finish(task: DagTask) -> bool:
            """Propagate completion once upstream sentinels have arrived and the queue is drained."""

            queue = queues[task.id]
            if (
                upstream_done_counts[task.id] >= self._expected_completions[task.id]
                and queue.empty()
            ):
                for downstream_id in self._downstream.get(task.id, []):
                    if downstream_id == task.id:
                        continue
                    await queues[downstream_id].put(sentinel)
                return True
            return False

        async def worker(task: DagTask) -> None:
            queue = queues[task.id]
            while True:
                item = await queue.get()
                if item is sentinel:
                    upstream_done_counts[task.id] += 1
                    queue.task_done()
                    if await maybe_finish(task):
                        break
                    continue

                payload, ctx = item
                node_ctx = ctx.for_node(task.id)
                log_with_context(logger, "processing", node_ctx)
                try:
                    result = task.handler(node_ctx, payload)
                    if inspect.isawaitable(result):
                        result = await result

                    async for value in self._iterate_outputs(result):
                        transitions = await self._select_transitions(task, value, node_ctx)
                        if not transitions:
                            await handle_result(value, node_ctx)
                            continue

                        for transition in transitions:
                            if transition.max_loops is not None:
                                key = (task.id, transition.name or transition.target)
                                loops = node_ctx.meta.setdefault("loop_counts", {})
                                current = loops.get(key, 0)
                                if current >= transition.max_loops:
                                    log_with_context(
                                        logger,
                                        "loop limit reached; skipping",
                                        node_ctx,
                                        extra={"edge": key, "max_loops": transition.max_loops},
                                    )
                                    continue
                                loops[key] = current + 1

                            log_with_context(
                                logger,
                                "routing payload",
                                node_ctx,
                                extra={
                                    "target": transition.target,
                                    "edge": transition.name or "direct",
                                },
                            )
                            await queues[transition.target].put((value, node_ctx))
                except Exception:
                    logger.exception("Unhandled exception in task %s", task.id, extra=node_ctx.log_details())
                finally:
                    queue.task_done()
                    if await maybe_finish(task):
                        break

        workers: List[asyncio.Task] = []
        for task in self.tasks.values():
            for _ in range(max(1, task.concurrency)):
                workers.append(asyncio.create_task(worker(task)))

        await self._enqueue_source(source, queues, sentinel)

        await asyncio.gather(*(q.join() for q in queues.values()))
        await asyncio.gather(*workers)
        return run_results

    async def _select_transitions(
        self, task: DagTask, payload: Payload, ctx: PipelineContext
    ) -> List[TaskTransition]:
        """Evaluate predicates to choose downstream targets."""

        selected: List[TaskTransition] = []
        for transition in task.transitions:
            if transition.predicate is None:
                selected.append(transition)
                continue

            decision = transition.predicate(ctx, payload)
            if inspect.isawaitable(decision):
                decision = await decision

            if decision:
                selected.append(transition)
                log_with_context(
                    logger,
                    "predicate matched",
                    ctx,
                    extra={"edge": transition.name or transition.target},
                )
        return selected

    async def _enqueue_source(
        self,
        source: AsyncIterable[Any] | Iterable[Any],
        queues: Dict[str, asyncio.Queue],
        sentinel: object,
    ) -> None:
        entrypoints = self.sources or [task_id for task_id in self.tasks if task_id not in self._upstream]
        if not entrypoints:
            raise ValueError("No source nodes defined (tasks without upstream)")

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

        async for idx, payload in self._with_index(async_iter):
            ctx = PipelineContext(run_id=self.run_id).with_data_id(str(idx))
            for entry_id in entrypoints:
                await queues[entry_id].put((payload, ctx))

        for entry_id in entrypoints:
            await queues[entry_id].put(sentinel)

    async def _iterate_outputs(self, result: Any) -> AsyncIterable[Any]:
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

    async def _with_index(self, source: AsyncIterable[Any]) -> AsyncIterable[tuple[int, Any]]:
        idx = 0
        async for item in source:
            yield idx, item
            idx += 1


class PipelineBuilder:
    """Imperative builder to configure DAG tasks and edges in code."""

    def __init__(self):
        self._tasks: Dict[str, DagTask] = {}
        self._links: List[tuple[str, TaskTransition]] = []

    def task(self, id: str, handler: Handler, *, concurrency: int = 1) -> "PipelineBuilder":
        if id in self._tasks:
            raise ValueError(f"Task '{id}' already registered")
        self._tasks[id] = DagTask(id=id, handler=handler, concurrency=concurrency)
        return self

    def edge(
        self,
        source: str,
        target: str,
        *,
        predicate: Optional[Predicate] = None,
        name: Optional[str] = None,
        max_loops: Optional[int] = None,
    ) -> "PipelineBuilder":
        if source not in self._tasks:
            raise ValueError(f"Edge refers to unknown source task '{source}'")
        if target not in self._tasks:
            raise ValueError(f"Edge refers to unknown target task '{target}'")
        self._links.append(
            (source, TaskTransition(target=target, predicate=predicate, name=name, max_loops=max_loops))
        )
        return self

    def build(self, *, run_id: Optional[str] = None) -> DagPipeline:
        for source, transition in self._links:
            self._tasks[source].transitions.append(transition)
        return DagPipeline(self._tasks.values(), run_id=run_id)
