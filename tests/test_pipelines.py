from async_dag import run_sync_pipeline, Context
from typing import Dict, Iterator, Tuple, List
import time
import pytest


@pytest.fixture
def small_batch() -> List[Dict]:
    return [
        {"id": 0, "text": "document 0"},
        {"id": 1, "text": "document 1"},
    ]


@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda *_args, **_kwargs: None)


def test_run_sync_pipeline_processes_payloads(small_batch):
    results: List[Context] = []

    def mock_load() -> Iterator[Tuple[int, Dict]]:
        for idx, doc in enumerate(small_batch):
            yield (idx, doc)

    def mock_pipeline(context: Context):
        context.payload["text"] = context.payload["text"].upper() + " PROCESSED"
        time.sleep(0.1)

    def collect_save(context: Context) -> None:
        results.append(context)

    run_sync_pipeline(
        load_func=mock_load,
        save_func=collect_save,
        pipeline_func=mock_pipeline,
        process_cnt=1,
        thread_cnt=1,
        batch_size=len(small_batch),
    )

    assert [ctx.payload["text"] for ctx in results] == [
        "DOCUMENT 0 PROCESSED",
        "DOCUMENT 1 PROCESSED",
    ]

    for ctx in results:
        assert ctx.start_time <= ctx.end_time
        assert any(log["level"] == "info" and "use" in log["msg"] for log in ctx.logs)
