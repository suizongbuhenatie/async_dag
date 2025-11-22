from async_dag import run_pipeline, Context
from typing import Dict, Iterable, Iterator, Tuple


def mock_load() -> Iterator[Tuple[int, Dict]]:
    """模拟加载函数：产生 10 条文档"""
    for i in range(10):
        yield (i, {"id": i, "text": f"document {i}"})


def mock_pipeline(contexts: Iterable[Context]):
    """模拟管道函数：在文本后追加 ' processed'"""
    for ctx in contexts:
        ctx.payload["text"] += " processed"
        ctx.info("processed text")
        ctx.warning("warning message")
        ctx.error("error message")


def mock_save(context: Context) -> None:
    """模拟保存函数：直接打印结果"""
    context.print_logs()
    print(f"[SAVE] idx={context.idx} -> {context.payload}")


# 运行测试
run_pipeline(
    load_func=mock_load,
    save_func=mock_save,
    pipeline_func=mock_pipeline,
    worker=3,
    buffer_size=5,
)
