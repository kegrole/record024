"""Shim so that 'python -m unittest test_pqueue -v' works from the repo root.

The real test suite lives in tests/test_pqueue.py.
"""

from tests.test_pqueue import *  # noqa: F401,F403
