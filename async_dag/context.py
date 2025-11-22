from pydantic import BaseModel
import uuid
from typing import Dict, List
from async_dag.logger import logger


class Context(BaseModel):
    idx: int
    logs: List[Dict[str, str]] = []
    payload: Dict = {}

    def error(self, msg: str) -> None:
        self._log("error", msg)

    def info(self, msg: str) -> None:
        self._log("info", msg)

    def warning(self, msg: str) -> None:
        self._log("warning", msg)
    
    def _log(self, level: str, msg: str) -> None:
        self.logs.append({"level": level, "msg": msg})

    def print_logs(self) -> None:
        for log in self.logs:
            if log["level"] == "error":
                logger.error(f"[logid:{self.idx}] {log['msg']}")
            elif log["level"] == "warning":
                logger.warning(f"[logid:{self.idx}] {log['msg']}")
            else:
                logger.info(f"[logid:{self.idx}] {log['msg']}")
