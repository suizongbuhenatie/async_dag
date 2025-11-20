from __future__ import annotations

import copy
import time
import uuid
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional


@dataclass
class PipelineContext:
    """Context object propagated through the pipeline.

    This captures tracing information such as the current run id, the
    lineage of nodes that have processed a payload, and any metadata callers
    want to thread through the DAG.
    """

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    data_id: Optional[str] = None
    node_path: List[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    meta: Dict[str, Any] = field(default_factory=dict)

    def _clone(self, **updates: Any) -> "PipelineContext":
        base = replace(self)
        base.meta = copy.deepcopy(self.meta)
        for key, value in updates.items():
            setattr(base, key, value)
        return base

    def for_node(self, node_id: str) -> "PipelineContext":
        """Return a copy of the context scoped to a downstream node."""

        return self._clone(node_path=[*self.node_path, node_id])

    def with_data_id(self, data_id: str) -> "PipelineContext":
        """Attach or replace the per-payload identifier."""

        return self._clone(data_id=data_id)

    @property
    def current_node(self) -> Optional[str]:
        return self.node_path[-1] if self.node_path else None

    def log_details(self) -> Dict[str, Any]:
        """Key/value map to include in structured logs."""

        return {
            "run_id": self.run_id,
            "data_id": self.data_id,
            "task_id": f"{self.run_id}:{self.data_id}" if self.data_id else self.run_id,
            "node": self.current_node,
            "path": "->".join(self.node_path),
        }
