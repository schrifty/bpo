"""Starlette application factory for the Cortex KPI web view + maintain UI."""

from __future__ import annotations

import logging

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import Mount, Route

from src.kpi_web import api
from src.kpi_web.settings import KPIWebSettings, load_kpi_web_settings

logger = logging.getLogger(__name__)


def create_app(
    *,
    settings: KPIWebSettings | None = None,
    environ: dict[str, str] | None = None,
) -> Starlette:
    """Build the KPI web Starlette app (catalog view + CRUD maintain)."""
    cfg = settings if settings is not None else load_kpi_web_settings(environ=environ)
    routes = [
        Route("/", api.index_page, methods=["GET"]),
        Route("/api/health", api.api_health, methods=["GET"]),
        Route("/api/me", api.api_me, methods=["GET"]),
        Route("/api/meta", api.api_meta, methods=["GET"]),
        Route("/api/situation", api.api_kpi_situation, methods=["GET"]),
        Route("/api/situation/stream", api.api_kpi_situation_stream, methods=["GET"]),
        Route("/api/situation/chat", api.api_kpi_situation_chat, methods=["POST"]),
        Route("/api/kpis", api.api_list_kpis, methods=["GET"]),
        Route("/api/kpis", api.api_kpi_add, methods=["POST"]),
        Route("/api/kpis/{name:path}/value", api.api_kpi_set_value, methods=["PUT"]),
        Route("/api/kpis/{name:path}", api.api_kpi_detail, methods=["GET"]),
        Route("/api/kpis/{name:path}", api.api_kpi_edit, methods=["PATCH"]),
        Route("/api/kpis/{name:path}", api.api_kpi_delete, methods=["DELETE"]),
        Route("/auth/status", api.auth_status, methods=["GET"]),
        Route("/auth/login", api.auth_login, methods=["GET"]),
        Route("/auth/callback", api.auth_callback, methods=["GET"]),
        Route("/auth/logout", api.auth_logout, methods=["GET"]),
        Route("/auth/dev-login", api.auth_dev_login, methods=["GET"]),
        Mount("/static", app=api.static_mount(), name="static"),
    ]
    middleware = [
        Middleware(
            CORSMiddleware,
            allow_origins=[cfg.base_url],
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
            allow_headers=["*"],
        ),
    ]
    app = Starlette(debug=False, routes=routes, middleware=middleware)
    app.state.settings = cfg
    return app


def run(*, settings: KPIWebSettings | None = None) -> None:
    """Run uvicorn for local development."""
    import uvicorn

    cfg = settings if settings is not None else load_kpi_web_settings()
    app = create_app(settings=cfg)
    logger.info(
        "Starting Cortex KPI web on http://%s:%s (base_url=%s, google=%s, dev_auth=%s)",
        cfg.host,
        cfg.port,
        cfg.base_url,
        cfg.google_oauth_configured,
        cfg.allow_dev_auth,
    )
    uvicorn.run(app, host=cfg.host, port=cfg.port, log_level="info")
