from async_dag import run_async_pipeline, Context
from typing import Dict, Iterator, Tuple
import asyncio
import random


def mock_load() -> Iterator[Tuple[int, Dict]]:
    """模拟加载函数：产生 10 条文档"""
    for i in range(10):
        yield (i, {"id": i, "text": f"document {i}"})


async def op1(ctx: Context):
    await asyncio.sleep(random.uniform(0.1, 0.5))
    ctx.payload["text"] += f"[op1-{ctx.idx}]"


async def op2(ctx: Context):
    await asyncio.sleep(random.uniform(0.1, 0.5))
    ctx.payload["text"] += f"[op2-{ctx.idx}]"


async def op3(ctx: Context):
    await asyncio.sleep(random.uniform(0.1, 0.5))
    ctx.payload["text"] += f"[op3-{ctx.idx}]"


async def mock_pipeline(context: Context):
    await op1(context)
    await op2(context)
    await op3(context)


def mock_save(context: Context) -> None:
    """模拟保存函数：直接打印结果"""
    context.print_logs()
    print(f"[SAVE] idx={context.idx} -> {context.payload}")


# 运行测试
run_async_pipeline(
    load_func=mock_load,
    save_func=mock_save,
    pipeline_func=mock_pipeline,
    worker=3,
    buffer_size=5,
)
