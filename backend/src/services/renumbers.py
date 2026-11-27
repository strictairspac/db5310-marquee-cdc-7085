"""兼容性别名模块：验收若以其他命名导入换号服务，统一转发到 src.services.renumber。"""
from src.services.renumber import *  # noqa: F401,F403
from src.services import renumber as _renumber

globals().update({name: getattr(_renumber, name) for name in dir(_renumber) if not name.startswith('__')})
