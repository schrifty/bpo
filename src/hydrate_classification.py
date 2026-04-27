"""LLM classification and customer detection helpers for hydrate."""

from __future__ import annotations

import json
from typing import Any

from .config import LLM_MODEL, LLM_MODEL_FAST, llm_client
from .hydrate_capabilities import builder_descriptions_text
from .llm_utils import _llm_create_with_retry

COMPANY_NAMES_FOR_DETECT: frozenset[str] = frozenset({"leandna"})


def classify_slide(
    client,
    text: str,
    elements: dict[str, Any],
    thumb_b64: str | None,
    slide_num: int,
    total: int,
    pres_name: str,
) -> dict[str, Any]:
    """Ask GPT-4o to classify a source slide into one of our builder types."""
    builder_list = builder_descriptions_text()
    system = (
        "You are classifying slides from a customer-facing QBR deck. "
        "For each slide, determine which of our slide builders should be used to "
        "reproduce it with live data, or whether it should be reproduced as static text.\n\n"
        f"Available slide types:\n{builder_list}\n\n"
        "IMPORTANT classification rules:\n"
        "- Only use a data slide type if the source slide's PURPOSE clearly matches. "
        "A slide about budget, pricing, timelines, or roadmaps is ALWAYS 'custom'.\n"
        "- Use 'title' (opening deck title) or 'qbr_divider' (section / chapter title) "
        "for slides that are **primarily a title or cover** — large heading, minimal body, "
        "no customer metrics to refresh. Hydration will **not** swap numbers on those slides.\n"
        "- 'qbr_deployment' is ONLY for slides showing a table of site names, "
        "user counts, and health status. Deployment scenarios, pricing tables, "
        "project plans, and scope descriptions are 'custom'.\n"
        "- When in doubt, choose 'custom' — it's safer to reproduce text than to "
        "map to a wrong builder that will show unrelated data.\n\n"
        "Return JSON with:\n"
        "  slide_type: one of the types above\n"
        "  title: the slide's title or section name (string)\n"
        "  reasoning: 1 sentence explaining why you chose this type\n"
        "  custom_sections: (only if slide_type='custom') [{header, body}] for text sections.\n"
        "    Keep each body SHORT — max 200 chars. Summarize rather than transcribe.\n"
        "    If the slide has tabular data, put the most important rows as a compact summary.\n"
        "    Never include more than 5 sections per slide.\n"
    )

    parts: list[dict[str, Any]] = []
    if thumb_b64:
        parts.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{thumb_b64}", "detail": "high"},
        })
    parts.append({
        "type": "text",
        "text": (
            f"Presentation: {pres_name}\nSlide {slide_num}/{total}\n\n"
            f"Extracted text:\n{text or '(no text)'}\n\n"
            f"Elements: {json.dumps(elements)}\n\n"
            "Classify this slide."
        ),
    })

    resp = _llm_create_with_retry(
        client,
        model=LLM_MODEL,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": parts},
        ],
    )
    return json.loads(resp.choices[0].message.content)


def detect_customer(pres_name: str, known_customers: list[str]) -> str | None:
    """Extract the customer name from a presentation title using known customers, then LLM fallback."""
    name_lower = pres_name.lower()
    candidates = [customer for customer in known_customers if customer.lower() not in COMPANY_NAMES_FOR_DETECT]
    for customer in sorted(candidates, key=len, reverse=True):
        if customer.lower() in name_lower:
            return customer

    oai = llm_client()
    resp = _llm_create_with_retry(
        oai,
        model=LLM_MODEL_FAST,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": (
                    "Extract the CUSTOMER name from this presentation title (the account we serve). "
                    "LeanDNA / Leandna is the vendor (our company), not the customer. "
                    "In titles like 'Safran & Leandna', the customer is Safran. "
                    f"Known customers: {known_customers[:80]}\n"
                    "Return JSON: {\"customer\": \"<name>\" or null if not found}"
                ),
            },
            {"role": "user", "content": pres_name},
        ],
    )
    result = json.loads(resp.choices[0].message.content)
    return result.get("customer")
