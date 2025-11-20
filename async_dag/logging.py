from __future__ import annotations

import logging
from typing import Any, Mapping

from .context import PipelineContext


class ContextLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that injects pipeline context metadata."""

    def process(self, msg: str, kwargs: dict[str, Any]):  # type: ignore[override]
        extra: Mapping[str, Any] = kwargs.get("extra", {})
        ctx: PipelineContext | None = self.extra
        if ctx:
            extra = {**extra, **ctx.log_details()}
        kwargs = {**kwargs, "extra": dict(extra)}
        return msg, kwargs


def setup_logging(level: int = logging.INFO) -> None:
    """Configure a sensible default logger for demos/tests."""

    class ContextualFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            for key in ("run_id", "data_id", "task_id", "node", "path"):
                if not hasattr(record, key):
                    setattr(record, key, "-")
            return super().format(record)

    handler = logging.StreamHandler()
    handler.setFormatter(
        ContextualFormatter(
            "%(asctime)s [%(levelname)s] %(name)s "
            "[task=%(task_id)s node=%(node)s path=%(path)s] :: %(message)s"
        )
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)


def log_with_context(
    logger: logging.Logger,
    message: str,
    ctx: PipelineContext | None = None,
    **kwargs: Any,
) -> None:
    """Helper to emit structured logs with context metadata."""

    ContextLoggerAdapter(logger, ctx).info(message, **kwargs)


def get_task_logger(ctx: PipelineContext, *, name: str | None = None) -> logging.LoggerAdapter:
    """Return a logger adapter bound to the given task context."""

    base = logging.getLogger(name) if name else logging.getLogger(__name__)
    return ContextLoggerAdapter(base, ctx)
