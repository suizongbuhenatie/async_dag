from .pipeline import run_pipeline
from .context import Context
import multiprocessing

multiprocessing.set_start_method("fork")
