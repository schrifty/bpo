"""KPI ownership principals, topic packs, and edit authorization.

Source of truth: ``config/kpi_owners.yaml`` (Marc-maintained). Salesforce is not
used for KPI ownership. Callers pass an actor email (CLI ``--as-user`` /
``CORTEX_KPI_ACTOR``, or the authenticated Google Workspace user for the KPI web).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .config_paths import KPI_OWNERS_FILE
from .metrics_registry import (
    normalize_owner_email,
    normalize_tag,
    registry_metric_owner,
    registry_metric_tags,
)

_OWNERS_CACHE: KPIOwnersConfig | None = None
_OWNERS_CACHE_PATH: Path | None = None


class KPIOwnershipError(ValueError):
    """Invalid owner, unknown principal, pack violation, or unauthorized edit."""


@dataclass(frozen=True)
class KPILead:
    email: str
    display_name: str | None
    packs: tuple[str, ...]


@dataclass(frozen=True)
class TopicPack:
    id: str
    description: str
    required_any_tags: tuple[str, ...]


@dataclass(frozen=True)
class KPIOwnersConfig:
    catalog_admin: str
    leads: tuple[KPILead, ...]
    topic_packs: dict[str, TopicPack]
    path: Path

    def known_owner_emails(self) -> frozenset[str]:
        emails = {self.catalog_admin}
        emails.update(lead.email for lead in self.leads)
        return frozenset(emails)

    def lead_for(self, email: str) -> KPILead | None:
        needle = normalize_owner_email(email)
        for lead in self.leads:
            if lead.email == needle:
                return lead
        return None

    def is_catalog_admin(self, email: str | None) -> bool:
        if email is None:
            return False
        return normalize_owner_email(email) == self.catalog_admin

    def is_known_owner(self, email: str | None) -> bool:
        if email is None:
            return False
        return normalize_owner_email(email) in self.known_owner_emails()


def reset_for_tests() -> None:
    """Clear in-process owners-config cache (test isolation)."""
    global _OWNERS_CACHE, _OWNERS_CACHE_PATH
    _OWNERS_CACHE = None
    _OWNERS_CACHE_PATH = None


def _parse_topic_packs(raw: Any) -> dict[str, TopicPack]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise KPIOwnershipError("kpi_owners.yaml topic_packs must be a mapping")
    out: dict[str, TopicPack] = {}
    for pack_id, body in raw.items():
        pid = normalize_tag(pack_id) or str(pack_id).strip().lower()
        if not pid:
            raise KPIOwnershipError("topic pack id cannot be empty")
        if not isinstance(body, dict):
            raise KPIOwnershipError(f"topic pack {pid!r} must be a mapping")
        tags_raw = body.get("required_any_tags")
        if not isinstance(tags_raw, (list, tuple)) or not tags_raw:
            raise KPIOwnershipError(
                f"topic pack {pid!r} requires a non-empty required_any_tags list"
            )
        tags: list[str] = []
        for item in tags_raw:
            tag = normalize_tag(item)
            if tag and tag not in tags:
                tags.append(tag)
        if not tags:
            raise KPIOwnershipError(f"topic pack {pid!r} has no usable required_any_tags")
        desc = str(body.get("description") or "").strip()
        out[pid] = TopicPack(id=pid, description=desc, required_any_tags=tuple(tags))
    return out


def _parse_leads(raw: Any, *, packs: dict[str, TopicPack]) -> tuple[KPILead, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise KPIOwnershipError("kpi_owners.yaml leads must be a list")
    leads: list[KPILead] = []
    seen: set[str] = set()
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise KPIOwnershipError(f"leads[{i}] must be a mapping")
        email = normalize_owner_email(item.get("email"))
        if not email:
            raise KPIOwnershipError(f"leads[{i}].email is required")
        if email in seen:
            raise KPIOwnershipError(f"duplicate lead email: {email!r}")
        seen.add(email)
        pack_ids_raw = item.get("packs")
        if not isinstance(pack_ids_raw, (list, tuple)) or not pack_ids_raw:
            raise KPIOwnershipError(f"lead {email!r} requires a non-empty packs list")
        pack_ids: list[str] = []
        for p in pack_ids_raw:
            pid = normalize_tag(p) or str(p).strip().lower()
            if not pid:
                continue
            if pid not in packs:
                raise KPIOwnershipError(
                    f"lead {email!r} references unknown topic pack {pid!r} "
                    f"(known: {sorted(packs)})"
                )
            if pid not in pack_ids:
                pack_ids.append(pid)
        if not pack_ids:
            raise KPIOwnershipError(f"lead {email!r} has no usable packs")
        display = item.get("display_name")
        display_name = str(display).strip() if display is not None else None
        leads.append(
            KPILead(
                email=email,
                display_name=display_name or None,
                packs=tuple(pack_ids),
            )
        )
    return tuple(leads)


def load_kpi_owners_config(*, path: Path | None = None) -> KPIOwnersConfig:
    """Parse ``config/kpi_owners.yaml`` (cached per path)."""
    global _OWNERS_CACHE, _OWNERS_CACHE_PATH
    owners_path = Path(path) if path is not None else KPI_OWNERS_FILE
    if (
        _OWNERS_CACHE is not None
        and _OWNERS_CACHE_PATH == owners_path
        and path is None
    ):
        return _OWNERS_CACHE
    if not owners_path.is_file():
        raise FileNotFoundError(f"KPI owners config not found: {owners_path}")
    data = yaml.safe_load(owners_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise KPIOwnershipError(f"Expected mapping at root of {owners_path}")
    admin = normalize_owner_email(data.get("catalog_admin"))
    if not admin:
        raise KPIOwnershipError("kpi_owners.yaml catalog_admin is required")
    packs = _parse_topic_packs(data.get("topic_packs"))
    leads = _parse_leads(data.get("leads"), packs=packs)
    if any(lead.email == admin for lead in leads):
        raise KPIOwnershipError(
            f"catalog_admin {admin!r} must not also appear under leads"
        )
    cfg = KPIOwnersConfig(
        catalog_admin=admin,
        leads=leads,
        topic_packs=packs,
        path=owners_path,
    )
    if path is None:
        _OWNERS_CACHE = cfg
        _OWNERS_CACHE_PATH = owners_path
    return cfg


def resolve_kpi_actor(
    explicit: str | None = None,
    *,
    owners: KPIOwnersConfig | None = None,
    environ: dict[str, str] | None = None,
) -> str:
    """Resolve the acting user for write authorization.

    Order: explicit argument → ``CORTEX_KPI_ACTOR`` → catalog_admin (local default).
    """
    cfg = owners if owners is not None else load_kpi_owners_config()
    if explicit is not None and str(explicit).strip():
        return normalize_owner_email(explicit)
    env = environ if environ is not None else os.environ
    from_env = normalize_owner_email(env.get("CORTEX_KPI_ACTOR", ""))
    if from_env:
        return from_env
    return cfg.catalog_admin


# CLI token for "acting user" on ``--owner`` / ``--me`` (case-insensitive).
KPI_OWNER_ME_TOKEN = "me"


def is_owner_me_token(raw: Any) -> bool:
    """True when *raw* is the CLI ``me`` sentinel (acting user)."""
    if raw is None:
        return False
    return str(raw).strip().lower() == KPI_OWNER_ME_TOKEN


def resolve_owner_cli_value(
    raw: Any,
    *,
    actor: str | None = None,
    owners: KPIOwnersConfig | None = None,
    environ: dict[str, str] | None = None,
    default_to_me: bool = False,
) -> str | None:
    """Resolve a CLI ``--owner`` value to a canonical email (or ``None``).

    - ``None`` / blank → ``None``, unless *default_to_me* (then acting user).
    - ``me`` (any case) → acting user via :func:`resolve_kpi_actor`.
    - otherwise → normalized email/id string (not validated against known owners;
      callers that write the registry still run :func:`validate_owner_value`).
    """
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        if default_to_me:
            return resolve_kpi_actor(actor, owners=owners, environ=environ)
        return None
    if is_owner_me_token(raw):
        return resolve_kpi_actor(actor, owners=owners, environ=environ)
    return normalize_owner_email(raw)


def metric_satisfies_topic_pack(entry: Any, pack: TopicPack) -> bool:
    """True when *entry* tags include any of the pack's ``required_any_tags``."""
    tags = set(registry_metric_tags(entry))
    return any(tag in tags for tag in pack.required_any_tags)


def lead_packs_satisfied(entry: Any, lead: KPILead, *, owners: KPIOwnersConfig) -> bool:
    """True when *entry* satisfies at least one of the lead's allowed packs."""
    for pack_id in lead.packs:
        pack = owners.topic_packs.get(pack_id)
        if pack is not None and metric_satisfies_topic_pack(entry, pack):
            return True
    return False


def validate_owner_value(
    owner: str | None,
    *,
    owners: KPIOwnersConfig | None = None,
) -> str | None:
    """Return an error string when *owner* is missing or not a known principal."""
    cfg = owners if owners is not None else load_kpi_owners_config()
    email = normalize_owner_email(owner)
    if not email:
        return "owner is required (email of catalog_admin or a configured lead)"
    if not cfg.is_known_owner(email):
        known = ", ".join(sorted(cfg.known_owner_emails()))
        return f"unknown KPI owner {email!r}; known owners: {known}"
    return None


def validate_entry_ownership(
    entry: Any,
    *,
    owners: KPIOwnersConfig | None = None,
) -> str | None:
    """Validate owner field and lead topic-pack constraints; return error or ``None``."""
    if not isinstance(entry, dict):
        return "invalid registry row"
    cfg = owners if owners is not None else load_kpi_owners_config()
    owner = registry_metric_owner(entry)
    err = validate_owner_value(owner, owners=cfg)
    if err:
        return err
    assert owner is not None
    if cfg.is_catalog_admin(owner):
        return None
    lead = cfg.lead_for(owner)
    if lead is None:
        return f"unknown KPI owner {owner!r}"
    if not lead_packs_satisfied(entry, lead, owners=cfg):
        pack_desc = []
        for pack_id in lead.packs:
            pack = cfg.topic_packs[pack_id]
            pack_desc.append(f"{pack_id} (tags any of {list(pack.required_any_tags)})")
        return (
            f"owner {owner!r} may only own KPIs in packs {list(lead.packs)}; "
            f"metric tags {registry_metric_tags(entry)} do not satisfy: {', '.join(pack_desc)}"
        )
    return None


def assert_actor_may_view_catalog(
    actor: str,
    *,
    owners: KPIOwnersConfig | None = None,
) -> None:
    """Fail loud when *actor* may not browse the KPI catalog (web view / read APIs).

    Catalog admin and configured leads: **read-all**. Unknown emails are denied
    (no silent empty catalog).
    """
    cfg = owners if owners is not None else load_kpi_owners_config()
    actor_email = normalize_owner_email(actor)
    if not actor_email:
        raise KPIOwnershipError("actor is required for KPI catalog access")
    if not cfg.is_known_owner(actor_email):
        raise KPIOwnershipError(
            f"unauthorized KPI viewer {actor_email!r}; not catalog_admin or a configured lead"
        )


def assert_actor_may_mutate_metric(
    actor: str,
    *,
    existing_owner: str | None,
    new_owner: str | None,
    action: str,
    owners: KPIOwnersConfig | None = None,
) -> None:
    """Fail loud when *actor* may not add/edit/delete the metric under ownership rules.

    Catalog admin: full catalog. Leads: edit-own only (cannot reassign ownership away
    from themselves or edit another owner's KPI).
    """
    cfg = owners if owners is not None else load_kpi_owners_config()
    actor_email = normalize_owner_email(actor)
    if not actor_email:
        raise KPIOwnershipError("actor is required for KPI catalog mutations")
    if not cfg.is_known_owner(actor_email):
        raise KPIOwnershipError(
            f"unauthorized KPI actor {actor_email!r}; not catalog_admin or a configured lead"
        )
    if cfg.is_catalog_admin(actor_email):
        return
    # Lead: edit-own only.
    existing = normalize_owner_email(existing_owner) if existing_owner else ""
    new = normalize_owner_email(new_owner) if new_owner else ""
    if action == "add":
        if new != actor_email:
            raise KPIOwnershipError(
                f"lead {actor_email!r} may only create KPIs owned by themselves "
                f"(got owner={new!r})"
            )
        return
    if action in ("edit", "delete"):
        if existing != actor_email:
            raise KPIOwnershipError(
                f"lead {actor_email!r} may not {action} KPI owned by {existing or '(missing)'!r}"
            )
        if action == "edit" and new and new != actor_email:
            raise KPIOwnershipError(
                f"lead {actor_email!r} may not reassign ownership to {new!r}"
            )
        return
    raise KPIOwnershipError(f"unknown ownership action {action!r}")
