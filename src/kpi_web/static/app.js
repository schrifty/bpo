(() => {
  const $ = (id) => document.getElementById(id);

  const state = {
    selected: null,
    mode: "stored",
    meta: null,
  };

  function esc(s) {
    return String(s ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function fmtValue(v) {
    if (v === null || v === undefined || v === "") return "—";
    if (typeof v === "number") {
      return Number.isInteger(v) ? String(v) : v.toFixed(4).replace(/\.?0+$/, "");
    }
    return String(v);
  }

  async function api(path) {
    const res = await fetch(path, { credentials: "same-origin" });
    let body = null;
    try {
      body = await res.json();
    } catch (_) {
      body = null;
    }
    if (!res.ok) {
      const msg = (body && body.error) || `${res.status} ${res.statusText}`;
      const err = new Error(msg);
      err.status = res.status;
      err.body = body;
      throw err;
    }
    return body;
  }

  function showLogin(status, errMsg) {
    $("login-panel").classList.remove("hidden");
    $("app-panel").classList.add("hidden");
    const actions = $("login-actions");
    actions.innerHTML = "";
    if (status.google_configured) {
      const a = document.createElement("a");
      a.href = "/auth/login";
      a.innerHTML = '<button type="button">Sign in with Google</button>';
      actions.appendChild(a);
    } else {
      const p = document.createElement("p");
      p.className = "muted";
      p.textContent =
        "Google OAuth is not configured. Set CORTEX_KPI_WEB_GOOGLE_CLIENT_ID / SECRET, or use local/dev auth.";
      actions.appendChild(p);
    }
    if (status.dev_auth_enabled) {
      const a = document.createElement("a");
      a.href = "/auth/dev-login";
      a.innerHTML = '<button type="button">Dev login</button>';
      actions.appendChild(a);
    }
    const err = $("login-error");
    if (errMsg) {
      err.hidden = false;
      err.textContent = errMsg;
    } else {
      err.hidden = true;
    }
  }

  function showApp(me) {
    $("login-panel").classList.add("hidden");
    $("app-panel").classList.remove("hidden");
    const role = me.is_catalog_admin ? "catalog admin" : "lead (read-all)";
    $("session").innerHTML =
      `${esc(me.name || me.email)} · ${esc(role)} · ` +
      `<a href="/auth/logout">Sign out</a>`;
  }

  function fillFilters(meta) {
    const ownerSel = $("filter-owner");
    const tagSel = $("filter-tag");
    ownerSel.innerHTML = '<option value="">All owners</option>';
    for (const o of meta.owners || []) {
      if (o.email === "(missing)") continue;
      const opt = document.createElement("option");
      opt.value = o.email;
      opt.textContent = `${o.email} (${o.count})`;
      ownerSel.appendChild(opt);
    }
    tagSel.innerHTML = '<option value="">All tags</option>';
    for (const t of meta.tags || []) {
      const opt = document.createElement("option");
      opt.value = t.tag;
      opt.textContent = `${t.tag} (${t.count})`;
      tagSel.appendChild(opt);
    }
  }

  function valueCell(kpi) {
    if (!kpi.observation) return '<span class="muted">—</span>';
    const obs = kpi.observation;
    if (obs.error) {
      return `<span class="value-error" title="${esc(obs.error)}">error</span>`;
    }
    if (!obs.ok) {
      const tip = (obs.warnings && obs.warnings[0]) || "no value";
      return `<span class="value-empty" title="${esc(tip)}">empty</span>`;
    }
    return `<span class="value-ok">${esc(fmtValue(obs.value))}</span>`;
  }

  function renderList(payload) {
    const tbody = $("kpi-table").querySelector("tbody");
    tbody.innerHTML = "";
    const items = payload.kpis || [];
    $("empty-state").classList.toggle("hidden", items.length > 0);
    $("list-status").textContent =
      `${items.length} KPI(s)` +
      (payload.resolved ? ` · values (${payload.mode})` : " · catalog only") +
      (payload.filters && payload.filters.owner ? ` · owner=${payload.filters.owner}` : "") +
      (payload.filters && payload.filters.tags && payload.filters.tags.length
        ? ` · tags=${payload.filters.tags.join(",")}`
        : "");

    for (const kpi of items) {
      const tr = document.createElement("tr");
      if (state.selected === kpi.name) tr.classList.add("active");
      tr.innerHTML = `
        <td>${esc(kpi.name)}</td>
        <td>${esc(kpi.owner || "—")}</td>
        <td>${(kpi.tags || []).map((t) => `<span class="pill">${esc(t)}</span>`).join("") || "—"}</td>
        <td>${kpi.target == null ? "—" : esc(fmtValue(kpi.target))}${kpi.direction ? ` <span class="muted">(${esc(kpi.direction)})</span>` : ""}</td>
        <td>${valueCell(kpi)}</td>`;
      tr.addEventListener("click", () => selectKpi(kpi.name));
      tbody.appendChild(tr);
    }
  }

  function renderDetail(payload) {
    const kpi = payload.kpi;
    const obs = kpi.observation || {};
    let valueHtml;
    if (obs.error) {
      valueHtml = `<div class="value-error">${esc(obs.error)}</div>`;
    } else if (!obs.ok) {
      const warns = (obs.warnings || []).join("; ") || "No value available";
      valueHtml = `<div class="value-empty">${esc(warns)}</div>`;
    } else {
      valueHtml = `<div class="value-ok">${esc(fmtValue(obs.value))}</div>`;
    }

    const hist = kpi.history || [];
    const histRows = hist.length
      ? hist
          .map(
            (h) =>
              `<tr><td>${esc(h.date || "—")}</td><td>${esc(fmtValue(h.value))}</td></tr>`
          )
          .join("")
      : `<tr><td colspan="2" class="muted">No stored history</td></tr>`;

    $("detail").innerHTML = `
      <h2>${esc(kpi.name)}</h2>
      <p class="muted">${esc(kpi.owner || "no owner")} · mode=${esc(payload.mode)} · status=${esc(kpi.value_status || "?")}</p>
      <dl>
        <dt>Description</dt>
        <dd>${esc(kpi.description || "—")}</dd>
        <dt>Management guidance</dt>
        <dd>${esc(kpi.mgmt_guidance || "—")}</dd>
        <dt>Tags</dt>
        <dd>${(kpi.tags || []).map((t) => `<span class="pill">${esc(t)}</span>`).join("") || "—"}</dd>
        <dt>Target</dt>
        <dd>${kpi.target == null ? "—" : esc(fmtValue(kpi.target))}
          ${kpi.direction ? `(${esc(kpi.direction)})` : ""}
          ${kpi.unit ? ` · ${esc(kpi.unit)}` : ""}
          ${kpi.target_error ? `<div class="error">${esc(kpi.target_error)}</div>` : ""}
        </dd>
        <dt>Value (${esc(obs.origin || payload.mode)})</dt>
        <dd>${valueHtml}
          ${obs.as_of ? `<div class="muted">as of ${esc(obs.as_of)}</div>` : ""}
        </dd>
        <dt>Generator / metric-id</dt>
        <dd><code>${esc(kpi.metric_generator || "—")}</code> · id=${esc(kpi.metric_id ?? "—")}</dd>
        <dt>Recent history</dt>
        <dd>
          ${kpi.history_warning ? `<div class="value-empty">${esc(kpi.history_warning)}</div>` : ""}
          <table class="history"><thead><tr><th>Date</th><th>Value</th></tr></thead>
          <tbody>${histRows}</tbody></table>
        </dd>
      </dl>`;
  }

  async function refreshList() {
    const errEl = $("list-error");
    errEl.hidden = true;
    const owner = $("filter-owner").value;
    const tag = $("filter-tag").value;
    const mode = $("filter-mode").value;
    const values = $("filter-values").checked;
    state.mode = mode;
    const qs = new URLSearchParams();
    if (owner) qs.set("owner", owner);
    if (tag) qs.set("tag", tag);
    qs.set("mode", mode);
    if (values) qs.set("values", "1");
    $("list-status").textContent = "Loading…";
    try {
      const data = await api(`/api/kpis?${qs.toString()}`);
      renderList(data);
      if (state.selected) {
        // Keep selection highlight after refresh.
        for (const tr of $("kpi-table").querySelectorAll("tbody tr")) {
          if (tr.children[0] && tr.children[0].textContent === state.selected) {
            tr.classList.add("active");
          }
        }
      }
    } catch (err) {
      errEl.hidden = false;
      errEl.textContent = err.message || String(err);
      $("list-status").textContent = "Failed to load KPIs";
    }
  }

  async function selectKpi(name) {
    state.selected = name;
    for (const tr of $("kpi-table").querySelectorAll("tbody tr")) {
      tr.classList.toggle("active", tr.children[0] && tr.children[0].textContent === name);
    }
    $("detail").innerHTML = `<p class="muted">Loading ${esc(name)}…</p>`;
    try {
      const qs = new URLSearchParams({ mode: state.mode, history: "12" });
      const data = await api(`/api/kpis/${encodeURIComponent(name)}?${qs.toString()}`);
      renderDetail(data);
    } catch (err) {
      $("detail").innerHTML = `<div class="error">${esc(err.message || String(err))}</div>`;
    }
  }

  async function boot() {
    let status;
    try {
      status = await api("/auth/status");
    } catch (err) {
      showLogin({ google_configured: false, dev_auth_enabled: false }, err.message);
      return;
    }
    if (!status.authenticated) {
      showLogin(status, status.error || null);
      return;
    }
    try {
      const me = await api("/api/me");
      showApp(me);
      const meta = await api("/api/meta");
      state.meta = meta;
      fillFilters(meta);
      await refreshList();
    } catch (err) {
      showLogin(status, err.message);
    }
  }

  $("btn-refresh").addEventListener("click", refreshList);
  $("filter-owner").addEventListener("change", refreshList);
  $("filter-tag").addEventListener("change", refreshList);
  $("filter-mode").addEventListener("change", () => {
    state.mode = $("filter-mode").value;
    if (state.selected) selectKpi(state.selected);
    if ($("filter-values").checked) refreshList();
  });
  $("filter-values").addEventListener("change", refreshList);

  boot();
})();
