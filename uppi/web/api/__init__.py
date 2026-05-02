"""Routers for the additive UPPI web shell."""

from fastapi import APIRouter

from .attestazioni import router as attestazioni_router
from .clients import router as clients_router
from .auth import router as auth_router
from .health import router as health_router

api_router = APIRouter()
api_router.include_router(attestazioni_router)
api_router.include_router(clients_router)
api_router.include_router(auth_router)
api_router.include_router(health_router)

__all__ = ["api_router"]
