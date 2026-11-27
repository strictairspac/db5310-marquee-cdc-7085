"""compat alias to src.services.renumber."""
from src.services.renumber import *  # noqa
from src.services import renumber as _r
globals().update({n: getattr(_r, n) for n in dir(_r) if not n.startswith("__")})
