from yakits import run_pipeline, Context
from typing import Dict, Iterator, Tuple, List
import asyncio
import pytest


@pytest.fixture
def async_batch() -> List[Dict]:
    return [
        {"id": 0, "text": "doc-0"},
        {"id": 1, "text": "doc-1"},
    ]


@pytest.fixture(autouse=True)
def fast_asyncio_sleep(monkeypatch):
    async def _immediate_sleep(*_args, **_kwargs):
        return None

    monkeypatch.setattr(asyncio, "sleep", _immediate_sleep)


def test_run_async_pipeline_processes_payloads(async_batch):
    results: List[Context] = []

    def mock_load() -> Iterator[Tuple[int, Dict]]:
        for idx, doc in enumerate(async_batch):
            yield (idx, doc)

    async def mock_pipeline(context: Context):
        await asyncio.sleep(0.1)
        context.payload["text"] += f"|step1-{context.idx}"
        await asyncio.sleep(0.1)
        context.payload["text"] += "|done"

    def collect_save(context: Context) -> None:
        results.append(context)

    run_pipeline(
        load_func=mock_load,
        save_func=collect_save,
        pipeline_func=mock_pipeline,
        process_cnt=1,
        buffer_size=len(async_batch),
        batch_size=len(async_batch),
    )

    assert [ctx.payload["text"] for ctx in results] == [
        "doc-0|step1-0|done",
        "doc-1|step1-1|done",
    ]

    for ctx in results:
        assert ctx.start_time <= ctx.end_time
        assert any(log["level"] == "info" and "use" in log["msg"] for log in ctx.logs)


if __name__ == "__main__":
    pytest.main()
