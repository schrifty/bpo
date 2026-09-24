(() => {
  const $ = (id) => document.getElementById(id);

  const state = {
    selected: null,
    selectedTags: [],
    mode: "stored",
    meta: null,
    me: null,
    formMode: "add", // add | edit
    editName: null,
    pendingDelete: null,
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

  function fmtTarget(kpi) {
    if (kpi.target == null || kpi.target === "") return "—";
    const n = fmtValue(kpi.target);
    if (kpi.direction === "higher") return `>= ${n}`;
    if (kpi.direction === "lower") return `<= ${n}`;
    return n;
  }

  async function api(path, options = {}) {
    const res = await fetch(path, {
      credentials: "same-origin",
      ...options,
      headers: {
        ...(options.body ? { "Content-Type": "application/json" } : {}),
        ...(options.headers || {}),
      },
    });
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
    const role = me.is_catalog_admin
      ? "catalog admin (edit all)"
      : "lead (edit-own, read-all)";
    $("session").innerHTML =
      `${esc(me.name || me.email)} · ${esc(role)} · ` +
      `<a href="/auth/logout">Sign out</a>`;
  }

  function fillFilters(meta) {
    const ownerSel = $("filter-owner");
    const prevOwner = ownerSel.value;
    ownerSel.innerHTML = '<option value="">All owners</option>';
    for (const o of meta.owners || []) {
      if (o.email === "(missing)") continue;
      const opt = document.createElement("option");
      opt.value = o.email;
      opt.textContent = `${o.email} (${o.count})`;
      ownerSel.appendChild(opt);
    }
    if (prevOwner) ownerSel.value = prevOwner;
    renderTagFilter(meta.tags || []);
  }

  function renderTagFilter(tags) {
    const host = $("filter-tags");
    const known = new Set((tags || []).map((t) => t.tag));
    state.selectedTags = state.selectedTags.filter((t) => known.has(t));
    const ordered = [...(tags || [])].sort((a, b) => {
      const byCount = (b.count || 0) - (a.count || 0);
      if (byCount !== 0) return byCount;
      return String(a.tag).localeCompare(String(b.tag));
    });
    host.innerHTML = "";
    if (!ordered.length) {
      const empty = document.createElement("span");
      empty.className = "muted";
      empty.textContent = "No tags in catalog";
      host.appendChild(empty);
    }
    for (const t of ordered) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "tag-chip";
      btn.setAttribute("aria-pressed", state.selectedTags.includes(t.tag) ? "true" : "false");
      btn.dataset.tag = t.tag;
      btn.textContent = `${t.tag} (${t.count})`;
      btn.addEventListener("click", () => toggleTag(t.tag));
      host.appendChild(btn);
    }
    $("btn-clear-tags").classList.toggle("hidden", state.selectedTags.length === 0);
  }

  function toggleTag(tag) {
    const i = state.selectedTags.indexOf(tag);
    if (i >= 0) state.selectedTags.splice(i, 1);
    else state.selectedTags.push(tag);
    renderTagFilter((state.meta && state.meta.tags) || []);
    refreshList();
  }

  function clearTags() {
    if (!state.selectedTags.length) return;
    state.selectedTags = [];
    renderTagFilter((state.meta && state.meta.tags) || []);
    refreshList();
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

  function canEdit(kpi) {
    const me = state.me;
    if (!me) return false;
    if (me.is_catalog_admin) return true;
    return (kpi.owner || "").toLowerCase() === (me.email || "").toLowerCase();
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
        <td>${esc(fmtTarget(kpi))}</td>
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

    const actions = canEdit(kpi)
      ? `<div class="detail-actions">
           <button type="button" id="btn-edit" class="secondary">Edit</button>
           <button type="button" id="btn-delete" class="danger">Delete</button>
         </div>`
      : `<p class="muted">You can view this KPI but only its owner (or the catalog admin) may edit it.</p>`;

    $("detail").innerHTML = `
      <h2>${esc(kpi.name)}</h2>
      <p class="muted">${esc(kpi.owner || "no owner")} · mode=${esc(payload.mode)} · status=${esc(kpi.value_status || "?")}</p>
      ${actions}
      <dl>
        <dt>Description</dt>
        <dd>${esc(kpi.description || "—")}</dd>
        <dt>Management guidance</dt>
        <dd>${esc(kpi.mgmt_guidance || "—")}</dd>
        <dt>Tags</dt>
        <dd>${(kpi.tags || []).map((t) => `<span class="pill">${esc(t)}</span>`).join("") || "—"}</dd>
        <dt>Grain</dt>
        <dd>${esc(kpi.grain || "daily")}</dd>
        <dt>Target</dt>
        <dd>${esc(fmtTarget(kpi))}
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

    const editBtn = $("btn-edit");
    const delBtn = $("btn-delete");
    if (editBtn) editBtn.addEventListener("click", () => openEditForm(kpi));
    if (delBtn) delBtn.addEventListener("click", () => deleteKpi(kpi));
  }

  function showMutateStatus(msg, isError) {
    const el = $("mutate-status");
    el.classList.remove("hidden", "error", "ok-banner");
    el.classList.add(isError ? "error" : "ok-banner");
    el.textContent = msg;
  }

  function parseTags(raw) {
    return String(raw || "")
      .split(/[,;]/)
      .map((t) => t.trim())
      .filter(Boolean);
  }

  function optionalNumber(raw) {
    const text = String(raw ?? "").trim();
    if (!text) return undefined;
    const n = Number(text);
    if (Number.isNaN(n)) throw new Error(`Invalid number: ${text}`);
    return n;
  }

  function formPayload() {
    const payload = {
      description: $("f-description").value,
      mgmt_guidance: $("f-mgmt").value,
      tags: parseTags($("f-tags").value),
      grain: $("f-grain").value,
      dry_run: $("f-dry-run").checked,
    };
    const owner = $("f-owner").value.trim();
    if (owner) payload.owner = owner;
    const unit = $("f-unit").value;
    if (unit) payload.unit = unit;
    const direction = $("f-direction").value;
    if (direction) payload.direction = direction;
    const target = optionalNumber($("f-target").value);
    if (target !== undefined) payload.target = target;
    const metricId = optionalNumber($("f-metric-id").value);
    if (metricId !== undefined) payload.metric_id = metricId;
    const gen = $("f-generator").value.trim();
    if (gen) payload.metric_generator = gen;
    if (state.formMode === "add") {
      payload.name = $("f-name").value.trim();
    } else {
      const renamed = $("f-new-name").value.trim();
      if (renamed && renamed !== state.editName) payload.new_name = renamed;
      // Explicit clears when fields emptied on edit.
      if (!$("f-description").value.trim()) payload.clear_description = true;
      if (!$("f-mgmt").value.trim()) payload.clear_mgmt_guidance = true;
      if (!$("f-tags").value.trim()) payload.clear_tags = true;
      if (!$("f-target").value.trim()) payload.clear_target = true;
      if (!$("f-direction").value) payload.clear_direction = true;
      if (!$("f-unit").value) payload.clear_unit = true;
      if (!$("f-metric-id").value.trim()) payload.clear_metric_id = true;
      if (!$("f-generator").value.trim()) payload.clear_generator = true;
    }
    return payload;
  }

  function fillFormFromKpi(kpi) {
    $("f-name").value = kpi.name || "";
    $("f-new-name").value = kpi.name || "";
    $("f-description").value = kpi.description || "";
    $("f-mgmt").value = kpi.mgmt_guidance || "";
    $("f-owner").value = kpi.owner || "";
    $("f-tags").value = (kpi.tags || []).join(", ");
    $("f-grain").value = kpi.grain || "daily";
    $("f-target").value = kpi.target == null ? "" : String(kpi.target);
    $("f-direction").value = kpi.direction || "";
    $("f-unit").value = kpi.unit || "";
    $("f-metric-id").value = kpi.metric_id == null ? "" : String(kpi.metric_id);
    $("f-generator").value = kpi.metric_generator || "";
  }

  function resetForm() {
    $("kpi-form").reset();
    $("f-dry-run").checked = true;
    $("f-grain").value = "daily";
    $("form-error").hidden = true;
    $("form-preview").classList.add("hidden");
    $("form-preview").textContent = "";
  }

  function openAddForm() {
    state.formMode = "add";
    state.editName = null;
    resetForm();
    $("form-title").textContent = "Add KPI";
    $("f-name").disabled = false;
    $("f-new-name-wrap").hidden = true;
    $("f-owner").value = state.me && state.me.email ? state.me.email : "";
    $("f-owner").disabled = !state.me?.is_catalog_admin;
    $("btn-form-submit").textContent = "Add KPI";
    $("kpi-form-dialog").showModal();
  }

  function openEditForm(kpi) {
    state.formMode = "edit";
    state.editName = kpi.name;
    resetForm();
    fillFormFromKpi(kpi);
    $("form-title").textContent = `Edit: ${kpi.name}`;
    $("f-name").disabled = true;
    $("f-new-name-wrap").hidden = false;
    $("f-owner").disabled = !state.me?.is_catalog_admin;
    $("btn-form-submit").textContent = "Save changes";
    $("kpi-form-dialog").showModal();
  }

  async function submitForm(ev) {
    ev.preventDefault();
    const errEl = $("form-error");
    errEl.hidden = true;
    $("form-preview").classList.add("hidden");
    let payload;
    try {
      payload = formPayload();
    } catch (err) {
      errEl.hidden = false;
      errEl.textContent = err.message || String(err);
      return;
    }
    const dry = !!payload.dry_run;
    try {
      let data;
      if (state.formMode === "add") {
        data = await api("/api/kpis", {
          method: "POST",
          body: JSON.stringify(payload),
        });
      } else {
        data = await api(`/api/kpis/${encodeURIComponent(state.editName)}`, {
          method: "PATCH",
          body: JSON.stringify(payload),
        });
      }
      const change = data.change;
      $("form-preview").classList.remove("hidden");
      $("form-preview").textContent = JSON.stringify(change, null, 2);
      if (dry) {
        showMutateStatus(
          `Dry-run ${change.action}: ${change.name} (YAML not written)`,
          false
        );
        return;
      }
      $("kpi-form-dialog").close();
      showMutateStatus(
        `${change.action} ${change.name} by ${change.actor}`,
        false
      );
      const meta = await api("/api/meta");
      state.meta = meta;
      fillFilters(meta);
      state.selected = change.name;
      await refreshList();
      await selectKpi(change.name);
    } catch (err) {
      errEl.hidden = false;
      errEl.textContent = err.message || String(err);
    }
  }

  async function deleteKpi(kpi) {
    state.pendingDelete = kpi;
    $("confirm-delete-copy").textContent =
      `Delete “${kpi.name}”? This removes it from the catalog YAML.`;
    $("confirm-delete-error").hidden = true;
    $("confirm-delete-dialog").showModal();
  }

  async function confirmDelete(ev) {
    ev.preventDefault();
    const kpi = state.pendingDelete;
    const errEl = $("confirm-delete-error");
    errEl.hidden = true;
    if (!kpi) {
      $("confirm-delete-dialog").close();
      return;
    }
    const confirmBtn = $("btn-delete-confirm");
    confirmBtn.disabled = true;
    try {
      await api(`/api/kpis/${encodeURIComponent(kpi.name)}?dry_run=1`, {
        method: "DELETE",
      });
      const data = await api(`/api/kpis/${encodeURIComponent(kpi.name)}`, {
        method: "DELETE",
      });
      $("confirm-delete-dialog").close();
      state.pendingDelete = null;
      showMutateStatus(
        `Deleted ${data.change.name} by ${data.change.actor}`,
        false
      );
      state.selected = null;
      $("detail").innerHTML =
        '<p class="muted">KPI deleted. Select another or add a new one.</p>';
      const meta = await api("/api/meta");
      state.meta = meta;
      fillFilters(meta);
      await refreshList();
    } catch (err) {
      errEl.hidden = false;
      errEl.textContent = err.message || String(err);
    } finally {
      confirmBtn.disabled = false;
    }
  }

  async function refreshList() {
    const errEl = $("list-error");
    errEl.hidden = true;
    const owner = $("filter-owner").value;
    const tags = state.selectedTags;
    const mode = $("filter-mode").value;
    const values = $("filter-values").checked;
    state.mode = mode;
    const qs = new URLSearchParams();
    if (owner) qs.set("owner", owner);
    qs.set("mode", mode);
    if (values) qs.set("values", "1");
    $("list-status").textContent = "Loading…";
    try {
      const data = await api(`/api/kpis?${qs.toString()}`);
      if (tags.length) {
        const want = new Set(tags);
        data.kpis = (data.kpis || []).filter((k) =>
          (k.tags || []).some((t) => want.has(t))
        );
        data.count = data.kpis.length;
        data.filters = { ...(data.filters || {}), tags };
      }
      renderList(data);
      if (state.selected) {
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
      state.me = me;
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
  $("btn-add").addEventListener("click", openAddForm);
  $("btn-clear-tags").addEventListener("click", clearTags);
  $("btn-form-cancel").addEventListener("click", () => $("kpi-form-dialog").close());
  $("kpi-form").addEventListener("submit", submitForm);
  $("btn-delete-cancel").addEventListener("click", () => {
    state.pendingDelete = null;
    $("confirm-delete-dialog").close();
  });
  $("confirm-delete-form").addEventListener("submit", confirmDelete);
  $("confirm-delete-dialog").addEventListener("close", () => {
    state.pendingDelete = null;
    $("btn-delete-confirm").disabled = false;
  });
  $("filter-owner").addEventListener("change", refreshList);
  $("filter-mode").addEventListener("change", () => {
    state.mode = $("filter-mode").value;
    if (state.selected) selectKpi(state.selected);
    if ($("filter-values").checked) refreshList();
  });
  $("filter-values").addEventListener("change", refreshList);

  boot();
})();
