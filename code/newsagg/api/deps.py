"""Shared FastAPI dependencies."""

from __future__ import annotations

from fastapi import HTTPException

from .. import runtime
from ..runtime import AppState

def require_state() -> AppState:
    if runtime.STATE is None:
        raise HTTPException(503, "service starting")
    return runtime.STATE
