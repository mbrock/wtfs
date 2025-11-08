"""wtfs - Fast directory scanning utility with Python analysis tools."""

__version__ = "0.1.0"

from .scanner import Scanner
from . import dump

__all__ = ['Scanner', 'dump']

