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
from .data_source_health import check_all_required, integration_freshness_metadata
from .run_context import init_run_context, set_run_phase
from .run_diagnostics import run_diagnostics_scope, run_phase

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_JOBS_DIR = _PROJECT_ROOT / "config" / "jobs"
_CORTEX_PY = _PROJECT_ROOT / "cortex.py"


@dataclass
class StepResult:
    name: str
    command: str
    success: bool
    exit_code: int
    duration_s: float
    error: str | None = None


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
        proc = subprocess.run(
            [sys.executable, str(_CORTEX_PY), *argv],
            cwd=str(_PROJECT_ROOT),
            env=env,
            timeout=timeout_seconds if timeout_seconds > 0 else None,
        )
        elapsed = time.monotonic() - t0
        ok = proc.returncode == 0
        err = None if ok else f"exit code {proc.returncode}"
        logger.info("job step end: %s success=%s duration_s=%.1f", name, ok, elapsed)
        return StepResult(
            name=name,
            command=command,
            success=ok,
            exit_code=int(proc.returncode),
            duration_s=elapsed,
            error=err,
        )
    except subprocess.TimeoutExpired:
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


def _write_failures_artifact(job_name: str, run_id: str, failures: list[str]) -> str | None:
    if not failures:
        return None
    body = json.dumps(
        {"job": job_name, "run_id": run_id, "failures": failures},
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

    preflight_errors = check_all_required()
    if preflight_errors:
        for msg in preflight_errors:
            print(f"  • {msg}")
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
            _write_failures_artifact(spec.name, run_id, list(summary["failures"]))

    return 0 if summary.get("success") else 1
