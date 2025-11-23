# yakits

一个轻量级的数据处理管道工具，统一支持同步函数和异步函数，提供多进程与线程池并发、批量处理、自动重试、断点续跑与结构化日志。适合用最少的代码搭建稳定的批处理与流式刷新任务。

**核心思想**
- 只需要三个钩子：`load_func` 产生输入、`pipeline_func` 处理每条数据、`save_func` 或 `output_path` 持久化输出。
- 同步函数用线程池执行，异步函数在每个进程内启用事件循环执行；两者共享同一套接口。

## 安装
- 要求 `Python >= 3.12`
- 从源码安装：

```bash
pip install -e .
```

## 快速上手
下面给出同步与异步的最小可运行示例。

**同步示例**

```python
from typing import Dict, Iterator, Tuple, List
from async_dag import run_pipeline, Context

def load() -> Iterator[Tuple[int, Dict]]:
    for i, text in enumerate(["a", "b", "c"]):
        yield i, {"text": text}

def pipeline(ctx: Context) -> None:
    ctx.payload["text"] = ctx.payload["text"].upper()

results: List[Context] = []
def save(ctx: Context) -> None:
    results.append(ctx)

run_pipeline(
    load_func=load,
    pipeline_func=pipeline,
    save_func=save,
    process_cnt=1,
    thread_cnt=2,
    batch_size=3,
)

print([c.payload["text"] for c in results])  # ["A", "B", "C"]
```

**异步示例**

```python
import asyncio
from typing import Dict, Iterator, Tuple, List
from async_dag import run_pipeline, Context

def load() -> Iterator[Tuple[int, Dict]]:
    for i, text in enumerate(["x", "y"]):
        yield i, {"text": text}

async def pipeline(ctx: Context) -> None:
    await asyncio.sleep(0.1)
    ctx.payload["text"] += "|done"

results: List[Context] = []
def save(ctx: Context) -> None:
    results.append(ctx)

run_pipeline(
    load_func=load,
    pipeline_func=pipeline,
    save_func=save,
    process_cnt=1,
    batch_size=2,
)

print([c.payload["text"] for c in results])  # ["x|done", "y|done"]
```

## 文件输出与断点续跑
如果不提供 `save_func`，可以通过 `output_path` 直接写出结果，且自动生成同名的 `*.task_info` 文件用于断点续跑。

```python
from async_dag import run_pipeline, Context

def load():
    for i in range(5):
        yield i, {"value": i}

def pipeline(ctx: Context):
    ctx.payload["value"] *= 2

run_pipeline(
    load_func=load,
    pipeline_func=pipeline,
    output_path="outputs.tsv",
    process_cnt=1,
    thread_cnt=1,
    resume=True,
)
```

运行后会生成两个文件：
- `outputs.tsv`：逐行写入 `idx\tJSON(payload)` 结果
- `outputs.tsv.task_info`：记录已完成的 `idx` 与耗时，用于下次跳过已处理数据

## 参数速览
- `load_func`：生成器，逐条 `yield (idx, payload)`
- `pipeline_func`：处理逻辑，支持同步函数或 `async def` 协程
- `save_func` / `output_path`：二选一；未提供 `save_func` 时必须提供 `output_path`
- `process_cnt`：工作进程数；异步与同步均可按进程并发
- `thread_cnt`：每个进程的线程数，仅在同步函数时生效
- `buffer_size`：内部队列的缓冲上限，控制内存占用
- `batch_size`：单次发送到 worker 的上下文数量；同步函数需满足 `batch_size >= thread_cnt`
- `max_retries`：失败自动重试次数上限
- `resume`：为真时会读取 `*.task_info` 跳过已完成的 `idx`
- `run_debug`：为真时只处理一个小批次，便于本地调试

## 日志与追踪
- 统一使用 `Context.info / warning / error` 记录结构化日志，可在处理完后调用 `Context.print_logs()` 输出。
- 若安装了 `colorlog`，日志会以彩色格式输出；否则退化为标准控制台格式。

## 代码结构
- `async_dag/context.py`：上下文模型与日志辅助
- `async_dag/pipeline.py`：生产者/消费者、进程与线程并发、重试与持久化
- `async_dag/logger.py`：彩色与普通日志初始化

## 运行测试
仓库内提供了简单的单元测试，示例参见 `tests/`。若已安装 `pytest`，可运行：

```bash
pytest -q
```

## 设计细节
- 异步函数在每个进程内通过事件循环批量执行；同步函数通过线程池并发执行。
- 失败重试在 worker 内部进行，超过 `max_retries` 的任务会被丢弃并记录日志。
- 当提供 `output_path` 时，内置保存器会同时写出 `*.task_info` 以支持断点续跑。
