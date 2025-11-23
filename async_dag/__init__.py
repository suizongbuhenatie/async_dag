from .pipeline import run_pipeline
from .context import Context
from .logger import logger
import multiprocessing

multiprocessing.set_start_method("fork")
