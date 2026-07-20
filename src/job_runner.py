"""Execute declarative batch jobs from ``config/jobs/*.yaml``."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .config import CORTEX_FAIL_ON_INTEGRATION_WARNINGS, CORTEX_JOB_TIMEOUT_SECONDS, logger
from .data_source_health import check_all_required, check_jira_backed_deck_required, integration_freshness_metadata
from .run_context import init_run_context, set_run_phase
from .run_diagnostics import run_diagnostics_scope, run_phase

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_JOBS_DIR = _PROJECT_ROOT / "config" / "jobs"
_CORTEX_PY = _PROJECT_ROOT / "cortex.py"
_STEP_OUTPUT_TAIL_CHARS = 6000
_MAX_FAILURE_MESSAGES = 20


@dataclass
class StepResult:
    name: str
    command: str
    success: bool
    exit_code: int
    duration_s: float
    error: str | None = None
    detail_messages: list[str] = field(default_factory=list)
    stdout_tail: str | None = None
    stderr_tail: str | None = None


@dataclass
class JobSpec:
    name: str
    stop_on_failure: bool = True
    fail_on_warnings: bool = False
    require_fresh_salesforce: bool = False
    steps: list[dict[str, Any]] = field(default_factory=list)


def resolve_job_path(job: str) -> Path:
    raw = job.strip()
    if not raw:
        raise ValueError("job name must be non-empty")
    if raw.endswith(".yaml") or raw.endswith(".yml"):
        path = Path(raw)
        if not path.is_absolute():
            path = (_PROJECT_ROOT / path).resolve()
        return path
    return (_JOBS_DIR / f"{raw}.yaml").resolve()


def load_job_spec(job: str) -> JobSpec:
    path = resolve_job_path(job)
    if not path.is_file():
        raise FileNotFoundError(f"Job spec not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid job spec (expected mapping): {path}")
    steps = payload.get("steps") or payload.get("jobs")
    if not isinstance(steps, list) or not steps:
        raise ValueError(f"Job spec must include non-empty 'steps' or 'jobs': {path}")
    name = str(payload.get("name") or path.stem)
    return JobSpec(
        name=name,
        stop_on_failure=bool(payload.get("stop_on_failure", True)),
        fail_on_warnings=bool(payload.get("fail_on_warnings", False)),
        require_fresh_salesforce=bool(payload.get("require_fresh_salesforce", False)),
        steps=[dict(s) for s in steps if isinstance(s, dict)],
    )


def build_step_argv(step: dict[str, Any]) -> list[str]:
    command = str(step.get("command") or "").strip()
    if not command:
        raise ValueError(f"Job step {step.get('name')!r} missing 'command'")
    if command == "deck":
        deck_id = str(step.get("deck_id") or "").strip()
        if not deck_id:
            raise ValueError("deck command requires deck_id")
        if deck_id == "engineering-portfolio":
            return ["engineering-portfolio"]
        if deck_id == "implementations_review":
            return ["implementations-review"]
        argv = ["run", "--deck", deck_id]
        if step.get("all_customers"):
            argv.append("--all-customers")
        for cust in step.get("customers") or []:
            argv.extend(["--customer", str(cust)])
        if step.get("days") is not None:
            argv.extend(["--days", str(int(step["days"]))])
        if step.get("max_customers") is not None:
            argv.extend(["--max-customers", str(int(step["max_customers"]))])
        if step.get("workers") is not None:
            argv.extend(["--workers", str(int(step["workers"]))])
        if step.get("quarter"):
            argv.extend(["--quarter", str(step["quarter"])])
        if step.get("thumbnails"):
            argv.append("--thumbnails")
        if step.get("csm"):
            argv.extend(["--csm", str(step["csm"])])
        return argv
    if command == "portfolio":
        argv = ["--portfolio"]
        if step.get("days") is not None:
            argv.extend(["--days", str(int(step["days"]))])
        if step.get("max_customers") is not None:
            argv.extend(["--max-customers", str(int(step["max_customers"]))])
        if step.get("quarter"):
            argv.extend(["--quarter", str(step["quarter"])])
        if step.get("thumbnails"):
            argv.append("--thumbnails")
        if step.get("csm"):
            argv.extend(["--csm", str(step["csm"])])
        return argv
    if command == "upload-portfolio-snapshot":
        argv = ["--upload-portfolio-snapshot"]
        if step.get("days") is not None:
            argv.extend(["--days", str(int(step["days"]))])
        if step.get("max_customers") is not None:
            argv.extend(["--max-customers", str(int(step["max_customers"]))])
        return argv
    if command in ("export", "export-all"):
        argv = ["export-all"]
        if step.get("days") is not None:
            argv.extend(["--days", str(int(step["days"]))])
        if step.get("max_bytes") is not None:
            argv.extend(["--max-bytes", str(int(step["max_bytes"]))])
        if step.get("signals_cap") is not None:
            argv.extend(["--signals-cap", str(int(step["signals_cap"]))])
        if step.get("skip_risk_insights"):
            argv.append("--skip-risk-insights")
        return argv
    if command == "export-pendo":
        customer = str(step.get("customer") or "").strip()
        if not customer:
            raise ValueError("export-pendo command requires customer")
        argv = ["--export-pendo", "--customer", customer]
        if step.get("days") is not None:
            argv.extend(["--days", str(int(step["days"]))])
        if step.get("compare_days") is not None:
            argv.extend(["--compare-days", str(int(step["compare_days"]))])
        if step.get("no_drive"):
            argv.append("--no-drive")
        if step.get("out"):
            argv.extend(["-o", str(step["out"])])
        return argv
    if command == "export-pendo-detailed":
        customer = str(step.get("customer") or "").strip()
        if not customer:
            raise ValueError("export-pendo-detailed command requires customer")
        argv = ["--export-pendo-detailed", "--customer", customer]
        if step.get("days") is not None:
            argv.extend(["--days", str(int(step["days"]))])
        if step.get("compare_days") is not None:
            argv.extend(["--compare-days", str(int(step["compare_days"]))])
        if step.get("no_drive"):
            argv.append("--no-drive")
        if step.get("out"):
            argv.extend(["-o", str(step["out"])])
        return argv
    if command == "export-pendo-top-arr":
        argv = ["--export-pendo-top-arr"]
        if step.get("top_n") is not None:
            argv.extend(["--top-n", str(int(step["top_n"]))])
        if step.get("days") is not None:
            argv.extend(["--days", str(int(step["days"]))])
        if step.get("compare_days") is not None:
            argv.extend(["--compare-days", str(int(step["compare_days"]))])
        if step.get("no_drive"):
            argv.append("--no-drive")
        if step.get("out_dir"):
            argv.extend(["--out-dir", str(step["out_dir"])])
        return argv
    if command == "metrics-upsert":
        argv = ["metrics-upsert"]
        if step.get("metric"):
            argv.extend(["--metric", str(step["metric"])])
        if step.get("date"):
            argv.extend(["--date", str(step["date"])])
        if step.get("days") is not None:
            argv.extend(["--days", str(int(step["days"]))])
        if step.get("requested_sites"):
            argv.extend(["--requested-sites", str(step["requested_sites"])])
        if step.get("dry_run"):
            argv.append("--dry-run")
        return argv
    raise ValueError(f"Unsupported job command: {command!r}")


def _tail_text(text: str, *, max_chars: int = _STEP_OUTPUT_TAIL_CHARS) -> str:
    raw = text or ""
    if len(raw) <= max_chars:
        return raw
    omitted = len(raw) - max_chars
    return f"... ({omitted} chars omitted)\n{raw[-max_chars:]}"


def _extract_step_failure_messages(stdout: str, stderr: str) -> list[str]:
    """Pull human-readable failure lines from a subprocess step."""
    messages: list[str] = []
    seen: set[str] = set()

    def _add(msg: str) -> None:
        s = (msg or "").strip()
        if not s or s in seen:
            return
        seen.add(s)
        messages.append(s)

    for line in "\n".join(filter(None, [stdout, stderr])).splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("CORTEX_RUN_SUMMARY="):
            try:
                payload = json.loads(stripped[len("CORTEX_RUN_SUMMARY=") :])
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                for item in payload.get("failures") or []:
                    _add(str(item))
                for item in payload.get("warnings") or []:
                    if messages:
                        break
            continue
        if stripped.startswith("• "):
            _add(stripped[2:].strip())
            continue
        if stripped.startswith("FAIL:"):
            _add(stripped)
            continue
        lower = stripped.lower()
        if lower.startswith("error:") or "data source check failed" in lower:
            _add(stripped)
    return messages[:_MAX_FAILURE_MESSAGES]


def _summarize_step_error(
    *,
    exit_code: int,
    detail_messages: list[str],
    stderr_tail: str,
    stdout_tail: str,
) -> str:
    if detail_messages:
        head = "; ".join(detail_messages[:3])
        if len(detail_messages) > 3:
            head += f" (+{len(detail_messages) - 3} more)"
        return head
    for blob in (stderr_tail, stdout_tail):
        for line in reversed((blob or "").splitlines()):
            s = line.strip()
            if s and not s.startswith("CORTEX_RUN_SUMMARY="):
                return s[:240]
    return f"exit code {exit_code}"


def _step_result_to_dict(result: StepResult) -> dict[str, Any]:
    row: dict[str, Any] = {
        "name": result.name,
        "command": result.command,
        "exit_code": result.exit_code,
        "duration_s": round(result.duration_s, 1),
        "success": result.success,
        "error": result.error,
    }
    if result.detail_messages:
        row["detail_messages"] = list(result.detail_messages)
    if result.stdout_tail:
        row["stdout_tail"] = result.stdout_tail
    if result.stderr_tail:
        row["stderr_tail"] = result.stderr_tail
    return row


def _build_failures_payload(
    job_name: str,
    run_id: str,
    *,
    failures: list[str],
    step_results: list[StepResult] | None = None,
    preflight_errors: list[str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "job": job_name,
        "run_id": run_id,
        "failures": failures,
    }
    if preflight_errors:
        payload["preflight_errors"] = list(preflight_errors)
    if step_results:
        failed_steps = [r for r in step_results if not r.success]
        if failed_steps:
            payload["steps"] = [_step_result_to_dict(r) for r in failed_steps]
    return payload


def _step_env(run_id: str, job_name: str, step_name: str) -> dict[str, str]:
    env = dict(os.environ)
    env["CORTEX_RUN_ID"] = run_id
    env["CORTEX_JOB_NAME"] = job_name
    env["CORTEX_STEP_NAME"] = step_name
    if CORTEX_FAIL_ON_INTEGRATION_WARNINGS:
        env.setdefault("CORTEX_FAIL_ON_INTEGRATION_WARNINGS", "1")
    return env


def run_step_subprocess(
    step: dict[str, Any],
    *,
    run_id: str,
    job_name: str,
    timeout_seconds: int = 0,
) -> StepResult:
    name = str(step.get("name") or step.get("command") or "step")
    command = str(step.get("command") or "")
    argv = build_step_argv(step)
    set_run_phase(name)
    env = _step_env(run_id, job_name, name)
    t0 = time.monotonic()
    logger.info("job step start: %s (%s)", name, " ".join(argv))
    try:
        # Stream child stdout/stderr to CloudWatch (via cortex logger) while also
        # capturing for failure extraction. ``capture_output=True`` alone swallows
        # successful-run detail (e.g. Slack join/match logs).
        proc = subprocess.Popen(
            [sys.executable, str(_CORTEX_PY), *argv],
            cwd=str(_PROJECT_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        collected: list[str] = []
        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                collected.append(line)
                stripped = line.rstrip("\n")
                if stripped:
                    # Child already emits JSON/text logs; forward as-is at INFO.
                    logger.info("%s", stripped)
            proc.wait(timeout=timeout_seconds if timeout_seconds > 0 else None)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            elapsed = time.monotonic() - t0
            logger.error("job step timeout: %s after %ds", name, timeout_seconds)
            return StepResult(
                name=name,
                command=command,
                success=False,
                exit_code=124,
                duration_s=elapsed,
                error=f"timeout after {timeout_seconds}s",
            )
        elapsed = time.monotonic() - t0
        combined = "".join(collected)
        ok = (proc.returncode or 0) == 0
        detail_messages = [] if ok else _extract_step_failure_messages(combined, "")
        err = None
        if not ok:
            err = _summarize_step_error(
                exit_code=int(proc.returncode or 1),
                detail_messages=detail_messages,
                stderr_tail=_tail_text(combined),
                stdout_tail=_tail_text(combined),
            )
            logger.error("job step output (%s):\n%s", name, _tail_text(combined, max_chars=2000))
        logger.info("job step end: %s success=%s duration_s=%.1f", name, ok, elapsed)
        return StepResult(
            name=name,
            command=command,
            success=ok,
            exit_code=int(proc.returncode or 0),
            duration_s=elapsed,
            error=err,
            detail_messages=detail_messages,
            stdout_tail=None if ok else _tail_text(combined),
            stderr_tail=None if ok else _tail_text(combined),
        )
    except Exception as exc:
        elapsed = time.monotonic() - t0
        logger.error("job step failed to start/run: %s: %s", name, exc)
        return StepResult(
            name=name,
            command=command,
            success=False,
            exit_code=1,
            duration_s=elapsed,
            error=str(exc)[:400],
        )


def _write_failures_artifact(
    job_name: str,
    run_id: str,
    failures: list[str],
    *,
    step_results: list[StepResult] | None = None,
    preflight_errors: list[str] | None = None,
) -> str | None:
    if not failures:
        return None
    body = json.dumps(
        _build_failures_payload(
            job_name,
            run_id,
            failures=failures,
            step_results=step_results,
            preflight_errors=preflight_errors,
        ),
        indent=2,
        default=str,
    )
    if _truthy_env("CORTEX_FAILURES_JSON_LOCAL"):
        path = _PROJECT_ROOT / "output" / f"failures-{job_name}-{run_id[:8]}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
        logger.info("Wrote failures artifact: %s", path)
        return str(path)
    try:
        from .export_drive_layout import ensure_historical_data_folder, ensure_historical_day_folder, historical_day_folder_label
        from .drive_config import get_qbr_output_root_folder_id, upload_text_file_to_drive_folder

        root_id = get_qbr_output_root_folder_id()
        if not root_id:
            return None
        historical_id = ensure_historical_data_folder(root_id)
        day_folder_id = ensure_historical_day_folder(historical_id)
        fname = f"failures-{job_name}-{run_id[:8]}.json"
        fid = upload_text_file_to_drive_folder(fname, body, day_folder_id, mime_type="application/json")
        logger.info(
            "Uploaded failures artifact to Drive Historical Data/%s/%s (id=%s)",
            historical_day_folder_label(),
            fname,
            fid,
        )
        return fid
    except Exception as exc:
        logger.warning("Could not upload failures.json to Drive: %s", exc)
        return None


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def run_job(
    job: str,
    *,
    dry_run: bool = False,
    json_summary: bool = True,
) -> int:
    spec = load_job_spec(job)
    run_id = init_run_context(run_id=os.environ.get("CORTEX_RUN_ID") or None, job_name=spec.name)
    fail_on_warnings = spec.fail_on_warnings or CORTEX_FAIL_ON_INTEGRATION_WARNINGS
    timeout = CORTEX_JOB_TIMEOUT_SECONDS

    if dry_run:
        print(f"Job: {spec.name} (run_id={run_id})")
        for step in spec.steps:
            argv = build_step_argv(step)
            print(f"  - {step.get('name', step.get('command'))}: python3 cortex.py {' '.join(argv)}")
        return 0

    preflight_errors = (
        check_jira_backed_deck_required()
        if spec.name == "engineering-portfolio"
        else check_all_required()
    )
    if preflight_errors:
        for msg in preflight_errors:
            print(f"  • {msg}", file=sys.stderr)
        _write_failures_artifact(
            spec.name,
            run_id,
            [f"preflight: {msg}" for msg in preflight_errors],
            preflight_errors=preflight_errors,
        )
        return 1

    meta = integration_freshness_metadata()
    if spec.require_fresh_salesforce:
        age_h = meta.get("salesforce_cache_age_h")
        ttl_h = meta.get("salesforce_cache_ttl_h")
        if age_h is not None and ttl_h is not None and float(age_h) >= float(ttl_h):
            print(
                f"Salesforce cache stale ({age_h}h >= TTL {ttl_h}h); "
                "refresh CRM or disable require_fresh_salesforce",
                file=sys.stderr,
            )
            return 1

    with run_diagnostics_scope(scope=f"job:{spec.name}") as diag:
        diag.set_integration_meta(meta)
        step_results: list[StepResult] = []
        for step in spec.steps:
            with run_phase(diag, str(step.get("name") or step.get("command") or "step")):
                result = run_step_subprocess(step, run_id=run_id, job_name=spec.name, timeout_seconds=timeout)
            step_results.append(result)
            if not result.success:
                diag.add_failure(f"{result.name}: {result.error or 'failed'}")
                if spec.stop_on_failure:
                    break

        summary = diag.emit_run_summary(
            job_name=spec.name,
            fail_on_warnings=fail_on_warnings,
            json_summary=json_summary,
        )
        diag.emit_stderr_summary()
        if summary.get("failures"):
            _write_failures_artifact(
                spec.name,
                run_id,
                list(summary["failures"]),
                step_results=step_results,
            )

    return 0 if summary.get("success") else 1
