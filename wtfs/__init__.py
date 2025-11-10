"""wtfs - Fast directory scanning utility with Python analysis tools."""

__version__ = "0.1.0"

from .scanner import Scanner
from . import dump

# Optional imports (require extra dependencies)
try:
    from . import display
    __all__ = ['Scanner', 'dump', 'display']
except ImportError:
    __all__ = ['Scanner', 'dump']

