from async_dag import run_sync_pipeline, Context
from typing import Dict, Iterator, Tuple
import time


def mock_load() -> Iterator[Tuple[int, Dict]]:
    """模拟加载函数：产生 10 条文档"""
    for i in range(10):
        yield (i, {"id": i, "text": f"document {i}"})


def mock_pipeline(context: Context):
    """模拟管道函数：在文本后追加 ' processed'"""
    context.payload["text"] += " processed"
    time.sleep(1)


def mock_save(context: Context) -> None:
    """模拟保存函数：直接打印结果"""
    context.print_logs()
    print(f"[SAVE] idx={context.idx} -> {context.payload}")


# 运行测试
run_sync_pipeline(
    load_func=mock_load,
    save_func=mock_save,
    pipeline_func=mock_pipeline,
    process_cnt=2,
    thread_cnt=5,
    batch_size=5,
)
