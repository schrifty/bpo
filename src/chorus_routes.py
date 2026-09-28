"""Documented Chorus REST routes.

Paths are relative to ``https://chorus.ai``. ``{placeholders}`` are path
parameters. Browser session, OAuth connect, and PagerDuty relay routes are
omitted; an API token cannot use them.

Source: Chorus API OpenAPI collection (conversations spec 26.33.08),
https://api-docs.chorus.ai/. ``GET /api/v1/conversations`` is included
because the live API routes it (a token without that permission gets 403,
not 404).
"""

from __future__ import annotations

CHORUS_OPENAPI_VERSION = "26.33.08"

CHORUS_ROUTES: dict[str, tuple[str, ...]] = {
    "GET": (
        "/api/v1/conversations",
        "/api/v1/conversations/live",
        "/api/v1/conversations/{id}",
        "/api/v1/conversations/{id}/media",
        "/api/v1/email_threads/{id}",
        "/api/v1/emails",
        "/api/v1/emails/{id}",
        "/api/v1/filters",
        "/api/v1/filters/{id}",
        "/api/v1/moments",
        "/api/v1/playlists",
        "/api/v1/playlists/{id}",
        "/api/v1/sales-qualifications/{recording_id}",
        "/api/v1/sales_qualification_configuration",
        "/api/v1/scorecards",
        "/api/v1/teams",
        "/api/v1/teams/{id}",
        "/api/v1/users/me",
        "/api/v1/users/me/settings",
        "/v3/engagements",
        "/v3/users",
        "/v3/webhook",
    ),
    "POST": (
        "/api/v1/conversations/{id}/actions/disconnect",
        "/api/v1/conversations:bulk",
        "/api/v1/conversations:export",
        "/api/v1/conversations:validate",
        "/api/v1/filters",
        "/api/v1/filters/actions/replace",
        "/api/v1/join",
        "/api/v1/moments",
        "/api/v1/playlists",
        "/api/v1/playlists/moments",
        "/api/v1/reports/{id}/actions/export",
        "/api/v1/sales-qualifications",
        "/api/v1/sales-qualifications/actions/writeback-crm",
        "/api/v1/saved_searches/default/actions/reset",
        "/api/v1/saved_searches/{id}/actions/set_default",
        "/api/v1/scorecards:export",
        "/api/v1/smart_playlists",
        "/api/v1/users/me/actions/deregister_token",
        "/api/v1/users/me/actions/register_token",
        "/api/v1/video_conferences",
        "/v3/upload",
        "/v3/webhook",
    ),
    "PUT": (
        "/api/v1/filters/{id}",
        "/api/v1/moments/{id}",
        "/api/v1/playlists/{id}",
        "/api/v1/sales_qualification_configuration/{framework_id}",
        "/api/v1/smart_playlists/{id}",
        "/api/v1/users/me/settings",
    ),
    "PATCH": (
        "/api/v1/playlists/moments/{id}",
        "/api/v1/video_conferences/{go_link}",
    ),
    "DELETE": (
        "/api/v1/conversations/{id}",
        "/api/v1/email_threads/{id}",
        "/api/v1/filters/{id}",
        "/api/v1/moments/{id}",
        "/api/v1/playlists/{id}",
        "/api/v1/video_conferences/{go_link}",
        "/v3/engagements",
        "/v3/engagements/{engagement_id}",
        "/v3/webhook",
    ),
}
