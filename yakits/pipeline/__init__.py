from .pipeline import run_pipeline
from .context import Context
import multiprocessing
import platform

_method = "spawn" if platform.system() == "Darwin" else "fork"
try:
    multiprocessing.set_start_method(_method)
except RuntimeError:
    pass
