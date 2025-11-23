from pydantic import BaseModel, Field
from typing import Dict, List, Any
from yakits import logger
import uuid
import time


class Context(BaseModel):
    idx: int
    log_id: str = ""
    logs: List[Dict[str, str]] = Field(default_factory=list)
    payload: Dict = Field(default_factory=dict)
    start_time: float = Field(default_factory=time.time)
    end_time: float = Field(default_factory=time.time)
    retry_count: int = 0

    def model_post_init(self, context: Any, /) -> None:
        self.log_id = f"{self.idx}-{uuid.uuid4().hex}"

    def start(self) -> None:
        self.start_time = time.time()

    def finish(self) -> None:
        self.end_time = time.time()

    def usage_time(self) -> float:
        return self.end_time - self.start_time

    def error(self, msg: str) -> None:
        self._log("error", msg)

    def info(self, msg: str) -> None:
        self._log("info", msg)

    def warning(self, msg: str) -> None:
        self._log("warning", msg)

    def _log(self, level: str, msg: str) -> None:
        self.logs.append({"level": level, "msg": msg})
        logger.debug(f"[logid:{self.idx}] {level} {msg}")

    def print_logs(self) -> None:
        for log in self.logs:
            if log["level"] == "error":
                logger.error(f"[logid:{self.idx}] {log['msg']}")
            elif log["level"] == "warning":
                logger.warning(f"[logid:{self.idx}] {log['msg']}")
            else:
                logger.info(f"[logid:{self.idx}] {log['msg']}")
