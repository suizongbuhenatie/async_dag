from .sync_pipeline import run_sync_pipeline
from .async_pipeline import run_async_pipeline
from .context import Context
from .logger import logger
import multiprocessing

multiprocessing.set_start_method("fork")
