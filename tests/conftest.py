import sys
from pathlib import Path
from types import SimpleNamespace


try:  # Provide a light-weight pydantic stub if dependency isn't installed
    import pydantic  # type: ignore
except ImportError:  # pragma: no cover

    class _FieldDefault:
        def __init__(self, default=None, default_factory=None):
            self.default = default
            self.default_factory = default_factory

    class BaseModel:
        def __init__(self, **kwargs):
            for name, value in self.__class__.__dict__.items():
                if isinstance(value, _FieldDefault) and name not in kwargs:
                    if value.default_factory is not None:
                        kwargs[name] = value.default_factory()
                    else:
                        kwargs[name] = value.default
            for key, value in kwargs.items():
                setattr(self, key, value)
            post_init = getattr(self, "model_post_init", None)
            if callable(post_init):
                post_init(None)

    def Field(*, default=None, default_factory=None):
        return _FieldDefault(default, default_factory)

    sys.modules["pydantic"] = SimpleNamespace(BaseModel=BaseModel, Field=Field)


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
