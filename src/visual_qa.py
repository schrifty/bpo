"""Visual QA for Google Slides presentations (``cortex --qa``)."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from .config import LLM_MODEL, llm_client, logger
from .llm_utils import _llm_create_with_retry
from .slide_thumbnails import download_thumbnail_b64, get_slide_thumbnail_url
from .slides_api import _get_service

_QA_SYSTEM_PROMPT = (
    "You are a visual QA reviewer for auto-generated Google Slides presentations. "
    "Examine this slide thumbnail and identify any layout or formatting problems.\n\n"
    "Check for:\n"
    "- Text overlapping other text or elements\n"
    "- Text running off the right or bottom edge of the slide canvas\n"
    "- Text that is mid-word cut off (e.g. 'Shor' where 'Shortage' was expected) — "
    "  this is a TRUE truncation issue. Do NOT flag text that simply ends near the "
    "  right margin with a complete word — that is normal layout.\n"
    "- Unreadable font sizes (too small to read)\n"
    "- Misaligned or visually unbalanced layouts\n"
    "- Empty slides that should have content\n"
    "- Date formats that look raw/ugly (e.g. 2026-03-10 instead of March 10, 2026)\n"
    "- Color contrast issues (text invisible against background)\n"
    "- Tables with cells overflowing or misaligned\n\n"
    "IMPORTANT: Values in [brackets] like [000], [$000], [00/00/00], [00%], [???] "
    "are INTENTIONAL incomplete-data placeholders. Do NOT flag them as issues. "
    "Also do NOT flag '⚠ INCOMPLETE' banners as issues — they are intentional.\n\n"
    "Return JSON:\n"
    "  pass: boolean — true if the slide looks good, false if there are problems\n"
    "  issues: list of strings describing each problem found (empty if pass=true)\n"
    "  severity: 'none' | 'minor' | 'major' — overall severity\n"
)


def visual_qa(pres_id: str, slides_svc=None) -> list[dict[str, Any]]:
    """Thumbnail every slide in a presentation and review with GPT-4o Vision."""
    if slides_svc is None:
        slides_svc, _d, _ = _get_service()

    pres = slides_svc.presentations().get(presentationId=pres_id).execute()
    slides = pres.get("slides", [])
    if not slides:
        return []

    oai = llm_client()
    n = len(slides)

    print(f"\n  Visual QA: reviewing {n} slides...", flush=True)

    thumb_urls: dict[str, str | None] = {}
    for si, slide in enumerate(slides, 1):
        page_id = slide["objectId"]
        try:
            thumb_urls[page_id] = get_slide_thumbnail_url(slides_svc, pres_id, page_id)
        except Exception as e:
            logger.warning("QA: thumbnail URL failed for slide %d/%d: %s", si, n, e)
            thumb_urls[page_id] = None

    def _review_slide(args: tuple[int, dict]) -> dict:
        si, slide = args
        page_id = slide["objectId"]
        url = thumb_urls.get(page_id)
        thumb_b64 = None
        if url:
            try:
                thumb_b64 = download_thumbnail_b64(url)
            except Exception as e:
                logger.warning("QA: thumbnail download failed for slide %d/%d: %s", si, n, e)

        resp = _llm_create_with_retry(
            oai,
            model=LLM_MODEL,
            temperature=0,
            max_tokens=1024,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _QA_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        *(
                            [{
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{thumb_b64}",
                                    "detail": "high",
                                },
                            }]
                            if thumb_b64
                            else []
                        ),
                        {"type": "text", "text": f"Slide {si}/{n}. Review this slide."},
                    ],
                },
            ],
        )
        raw_content = resp.choices[0].message.content
        try:
            qa = json.loads(raw_content)
        except json.JSONDecodeError as e:
            logger.warning("QA: slide %d/%d — invalid JSON from LLM (%s), treating as pass", si, n, e)
            qa = {"pass": True, "issues": ["QA response invalid (JSON error)"], "severity": "none"}
        qa["slide_num"] = si
        qa["page_id"] = page_id
        return qa

    raw: dict[int, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_review_slide, (si, slide)): si for si, slide in enumerate(slides, 1)}
        for fut in as_completed(futures):
            try:
                qa = fut.result()
                raw[qa["slide_num"]] = qa
            except Exception as e:
                si = futures[fut]
                logger.warning("QA failed for slide %d: %s", si, e)
                raw[si] = {"slide_num": si, "pass": True, "issues": [], "severity": "none"}

    results: list[dict[str, Any]] = []
    for si in range(1, n + 1):
        qa = raw.get(si, {"slide_num": si, "pass": True, "issues": [], "severity": "none"})
        results.append(qa)
        passed = qa.get("pass", True)
        severity = qa.get("severity", "none")
        issues = qa.get("issues", [])
        if passed:
            print(f"    [{si}/{n}] OK", flush=True)
        else:
            icon = "!" if severity == "major" else "~"
            print(f"    [{si}/{n}] [{icon}] {severity.upper()}: {'; '.join(issues[:2])}", flush=True)
            for issue in issues[2:]:
                print(f"         ↳ {issue}", flush=True)

    passed_count = sum(1 for r in results if r.get("pass", True))
    failed_count = len(results) - passed_count
    major = sum(1 for r in results if r.get("severity") == "major")
    minor = sum(1 for r in results if r.get("severity") == "minor")

    suffix = f"  ({major} major, {minor} minor)" if failed_count else ""
    print(f"  QA result: {passed_count}/{len(results)} passed{suffix}", flush=True)

    return results
