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
    editingValue: null,
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

  function _titleCaseLocalPart(part) {
    return String(part)
      .split(/[-_]+/)
      .filter(Boolean)
      .map((p) => p.charAt(0).toUpperCase() + p.slice(1).toLowerCase())
      .join("-");
  }

  /** first.last@leandna.com → "First Last"; anything else stays as stored. */
  function fmtOwner(owner) {
    const raw = String(owner || "").trim();
    if (!raw) return "—";
    const m = raw.match(/^([a-z][a-z0-9_-]*)\.([a-z][a-z0-9_-]*)@leandna\.com$/i);
    if (!m) return raw;
    return `${_titleCaseLocalPart(m[1])} ${_titleCaseLocalPart(m[2])}`;
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
    setUserMenuOpen(false);
    $("session").classList.add("hidden");
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

  function userInitials(me) {
    const name = String(me.name || "").trim();
    if (name) {
      const parts = name.split(/\s+/).filter(Boolean);
      if (parts.length >= 2) {
        return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
      }
      return name.slice(0, 2).toUpperCase();
    }
    const local = String(me.email || "").split("@")[0];
    const bits = local.split(/[._-]+/).filter(Boolean);
    if (bits.length >= 2) {
      return (bits[0][0] + bits[1][0]).toUpperCase();
    }
    return (local.slice(0, 2) || "?").toUpperCase();
  }

  function setUserMenuOpen(open) {
    const menu = $("user-menu");
    const badge = $("user-badge");
    if (!menu || !badge) return;
    menu.classList.toggle("hidden", !open);
    badge.setAttribute("aria-expanded", open ? "true" : "false");
  }

  function showApp(me) {
    $("login-panel").classList.add("hidden");
    $("app-panel").classList.remove("hidden");
    $("session").classList.remove("hidden");
    const role = me.is_catalog_admin
      ? "catalog admin (edit all)"
      : "lead (edit-own, read-all)";
    const display = me.name || me.email || "Signed in";
    $("user-menu-name").textContent = display;
    $("user-menu-email").textContent = me.email || "";
    $("user-menu-email").hidden = !me.email || display === me.email;
    $("user-menu-role").textContent = role;
    $("user-badge").title = display;
    $("user-badge").setAttribute("aria-label", `Account menu for ${display}`);
    const img = $("user-badge-img");
    const initials = $("user-badge-initials");
    if (me.picture) {
      img.src = me.picture;
      img.hidden = false;
      initials.hidden = true;
    } else {
      img.removeAttribute("src");
      img.hidden = true;
      initials.hidden = false;
      initials.textContent = userInitials(me);
    }
    setUserMenuOpen(false);
  }

  function fillFilters(meta) {
    const ownerSel = $("filter-owner");
    const prevOwner = ownerSel.value;
    ownerSel.innerHTML = '<option value="">All owners</option>';
    for (const o of meta.owners || []) {
      if (o.email === "(missing)") continue;
      const opt = document.createElement("option");
      opt.value = o.email;
      opt.textContent = `${fmtOwner(o.email)} (${o.count})`;
      ownerSel.appendChild(opt);
    }
    if (prevOwner) ownerSel.value = prevOwner;
  }

  function isImplementedKpi(kpi) {
    return Boolean(kpi.metric_generator || kpi.automated);
  }

  function kpiHasAllTags(kpi, tags) {
    if (!tags.length) return true;
    const have = new Set(kpi.tags || []);
    return tags.every((t) => have.has(t));
  }

  function facetTagsFromKpis(items, selected) {
    const matching = selected.length
      ? items.filter((k) => kpiHasAllTags(k, selected))
      : items;
    const counts = new Map();
    for (const kpi of matching) {
      const seen = new Set();
      for (const raw of kpi.tags || []) {
        const tag = String(raw || "").trim();
        if (!tag || seen.has(tag)) continue;
        seen.add(tag);
        counts.set(tag, (counts.get(tag) || 0) + 1);
      }
    }
    for (const tag of selected) {
      if (!counts.has(tag)) counts.set(tag, 0);
    }
    return [...counts.entries()]
      .map(([tag, count]) => ({ tag, count }))
      .sort((a, b) => {
        const byCount = (b.count || 0) - (a.count || 0);
        if (byCount !== 0) return byCount;
        return String(a.tag).localeCompare(String(b.tag));
      });
  }

  function renderTagFilter(tags) {
    const host = $("filter-tags");
    const ordered = tags || [];
    const known = new Set(ordered.map((t) => t.tag));
    state.selectedTags = state.selectedTags.filter((t) => known.has(t));
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
  }

  function toggleTag(tag) {
    const i = state.selectedTags.indexOf(tag);
    if (i >= 0) state.selectedTags.splice(i, 1);
    else state.selectedTags.push(tag);
    refreshList();
  }

  function targetOutcome(kpi) {
    if (kpi.target == null || kpi.target === "") return null;
    if (kpi.direction !== "higher" && kpi.direction !== "lower") return null;
    const obs = kpi.observation;
    if (!obs || !obs.ok || obs.value == null || obs.value === "") return null;
    const v = Number(obs.value);
    const t = Number(kpi.target);
    if (!Number.isFinite(v) || !Number.isFinite(t)) return null;
    if (kpi.direction === "higher") return v >= t ? "made" : "missed";
    return v <= t ? "made" : "missed";
  }

  function overrideTip(ov) {
    const generated =
      ov.generated_value == null || ov.generated_value === ""
        ? ov.generated_error
          ? `error: ${ov.generated_error}`
          : "none"
        : fmtValue(ov.generated_value);
    const who = ov.by ? ` by ${ov.by}` : "";
    const when = ov.at ? ` on ${String(ov.at).slice(0, 10)}` : "";
    return `Manual override${who}${when} — generated value: ${generated}`;
  }

  function valueCell(kpi) {
    if (!kpi.observation) return '<span class="muted">—</span>';
    const obs = kpi.observation;
    const ov = obs.override;
    if (ov) {
      const restore = canEdit(kpi)
        ? `<button type="button" class="value-restore" title="Show the stored generated value again">Restore</button>`
        : "";
      return `<span class="value-wrap"><span class="value-override" title="${esc(
        overrideTip(ov)
      )}">${esc(fmtValue(obs.value))}</span>${restore}</span>`;
    }
    if (obs.error) {
      return `<span class="value-error" title="${esc(obs.error)}">error</span>`;
    }
    if (!obs.ok) {
      const tip = (obs.warnings && obs.warnings[0]) || "no value";
      return `<span class="value-empty" title="${esc(tip)}">empty</span>`;
    }
    const outcome = targetOutcome(kpi);
    const cls =
      outcome === "made" ? "value-made" : outcome === "missed" ? "value-missed" : "value-ok";
    return `<span class="${cls}">${esc(fmtValue(obs.value))}</span>`;
  }

  function canEdit(kpi) {
    const me = state.me;
    if (!me) return false;
    if (me.is_catalog_admin) return true;
    return (kpi.owner || "").toLowerCase() === (me.email || "").toLowerCase();
  }

  function setListStatus(msg) {
    const el = $("list-status");
    const text = msg || "";
    el.textContent = text;
    el.classList.toggle("hidden", !text);
  }

  function currentValue(kpi) {
    const obs = kpi.observation;
    if (!obs || !obs.ok || obs.value == null || obs.value === "") return "";
    return String(obs.value);
  }

  function renderList(payload) {
    const tbody = $("kpi-table").querySelector("tbody");
    tbody.innerHTML = "";
    const items = payload.kpis || [];
    $("empty-state").classList.toggle("hidden", items.length > 0);
    setListStatus("");
    state.editingValue = null;

    for (const kpi of items) {
      const tr = document.createElement("tr");
      tr.dataset.name = kpi.name;
      if (state.selected === kpi.name) tr.classList.add("active");
      const ownerLabel = fmtOwner(kpi.owner);
      const ownerTitle = kpi.owner ? ` title="${esc(kpi.owner)}"` : "";
      const editable = canEdit(kpi);
      const del = editable
        ? `<button type="button" class="row-delete" title="Delete ${esc(kpi.name)}" aria-label="Delete ${esc(kpi.name)}">
             <svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M9 3h6l1 2h4v2H4V5h4l1-2zm1 6h2v9h-2V9zm4 0h2v9h-2V9zM7 9h2v9H7V9z"/></svg>
           </button>`
        : "";
      tr.innerHTML = `
        <td class="col-delete">${del}</td>
        <td>${esc(kpi.name)}</td>
        <td class="col-grain">${esc(kpi.grain || "daily")}</td>
        <td class="col-owner"${ownerTitle}>${esc(ownerLabel)}</td>
        <td>${(kpi.tags || []).map((t) => `<span class="pill">${esc(t)}</span>`).join("") || "—"}</td>
        <td>${esc(fmtTarget(kpi))}</td>
        <td class="value-cell${editable ? " editable" : ""}">${valueCell(kpi)}</td>`;
      const delBtn = tr.querySelector(".row-delete");
      if (delBtn) {
        delBtn.addEventListener("click", (ev) => {
          ev.stopPropagation();
          deleteKpi(kpi);
        });
      }
      const valueTd = tr.querySelector(".value-cell");
      const restoreBtn = tr.querySelector(".value-restore");
      if (restoreBtn) {
        restoreBtn.addEventListener("click", (ev) => {
          ev.stopPropagation();
          restoreOverride(kpi);
        });
      }
      if (editable) {
        valueTd.addEventListener("click", (ev) => {
          if (ev.target.closest(".value-restore")) return;
          ev.stopPropagation();
          beginValueEdit(valueTd, kpi);
        });
      }
      tr.addEventListener("click", () => selectKpi(kpi.name));
      tbody.appendChild(tr);
    }
  }

  function beginValueEdit(td, kpi) {
    if (state.editingValue === kpi.name) return;
    state.editingValue = kpi.name;
    const prior = currentValue(kpi);
    td.innerHTML = `<input class="value-input" type="text" inputmode="decimal" aria-label="Override value for ${esc(kpi.name)}" title="Clear the box to drop the override and show the generated value" />`;
    const input = td.querySelector("input");
    input.value = prior;
    input.focus();
    input.select();
    let done = false;
    const finish = async (save) => {
      if (done) return;
      done = true;
      const typed = input.value.trim();
      if (!save || typed === prior) {
        state.editingValue = null;
        td.innerHTML = valueCell(kpi);
        return;
      }
      // Empty clears the override so the generated reading shows again.
      let n = null;
      if (typed) {
        try {
          n = optionalNumber(typed);
        } catch (err) {
          state.editingValue = null;
          td.innerHTML = valueCell(kpi);
          showMutateStatus(err.message || String(err), true);
          return;
        }
      }
      try {
        await api(`/api/kpis/${encodeURIComponent(kpi.name)}/value`, {
          method: "PUT",
          body: JSON.stringify({ value: n }),
        });
        state.editingValue = null;
        await refreshList();
        if (state.selected === kpi.name) await selectKpi(kpi.name);
      } catch (err) {
        state.editingValue = null;
        td.innerHTML = valueCell(kpi);
        showMutateStatus(err.message || String(err), true);
      }
    };
    input.addEventListener("keydown", (ev) => {
      if (ev.key === "Enter") {
        ev.preventDefault();
        finish(true);
      } else if (ev.key === "Escape") {
        ev.preventDefault();
        finish(false);
      }
    });
    input.addEventListener("blur", () => finish(true));
  }

  function renderDetail(payload) {
    const kpi = payload.kpi;
    const hist = kpi.history || [];
    const histRows = hist.length
      ? hist
          .map(
            (h) =>
              `<tr><td>${esc(h.date || "—")}</td><td>${esc(fmtValue(h.value))}</td></tr>`
          )
          .join("")
      : `<tr><td colspan="2" class="muted">No stored history</td></tr>`;

    const editBtn = canEdit(kpi)
      ? `<button type="button" id="btn-detail-edit" class="icon-btn" title="Edit name, tags, and target" aria-label="Edit ${esc(kpi.name)}">
           <svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M3 17.25V21h3.75L17.8 9.94l-3.75-3.75L3 17.25zM20.7 7.04a1 1 0 0 0 0-1.41l-2.34-2.34a1 1 0 0 0-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg>
         </button>`
      : "";

    $("detail").innerHTML = `
      <div class="detail-head">
        <h2>${esc(kpi.name)}</h2>
        ${editBtn}
      </div>
      <p class="muted">${esc(fmtOwner(kpi.owner))}</p>
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
        <dt>Value</dt>
        <dd>${valueCell(kpi)}${overrideNote(kpi)}</dd>
        <dt>Recent history</dt>
        <dd>
          ${kpi.history_warning ? `<div class="value-empty">${esc(kpi.history_warning)}</div>` : ""}
          <table class="history"><thead><tr><th>Date</th><th>Value</th></tr></thead>
          <tbody>${histRows}</tbody></table>
        </dd>
      </dl>`;

    const detailEdit = $("btn-detail-edit");
    if (detailEdit) detailEdit.addEventListener("click", () => openEditForm(kpi));
    const restoreBtn = $("btn-restore-value");
    if (restoreBtn) {
      restoreBtn.addEventListener("click", () => restoreOverride(kpi));
    }
  }

  async function restoreOverride(kpi) {
    try {
      await api(`/api/kpis/${encodeURIComponent(kpi.name)}/value`, {
        method: "PUT",
        body: JSON.stringify({ value: null }),
      });
      await refreshList();
      if (state.selected === kpi.name) await selectKpi(kpi.name);
    } catch (err) {
      showMutateStatus(err.message || String(err), true);
    }
  }

  function overrideNote(kpi) {
    const ov = kpi.observation && kpi.observation.override;
    if (!ov) return "";
    const generated =
      ov.generated_value == null || ov.generated_value === ""
        ? "none"
        : fmtValue(ov.generated_value);
    const who = ov.by ? ` by ${esc(ov.by)}` : "";
    const restore = canEdit(kpi)
      ? ` <button type="button" id="btn-restore-value" class="linkish">Restore stored value</button>`
      : "";
    return `<div class="muted override-note">Manual override${who}; generated value ${esc(
      generated
    )}.${restore}</div>`;
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
    // Edit only carries the mutable fields; description, guidance, owner,
    // grain, metric ID, and generator are fixed once a KPI exists.
    if (state.formMode === "edit") {
      const payload = {
        tags: parseTags($("f-tags").value),
        dry_run: $("f-dry-run").checked,
      };
      const renamed = $("f-new-name").value.trim();
      if (renamed && renamed !== state.editName) payload.new_name = renamed;
      const target = optionalNumber($("f-target").value);
      if (target !== undefined) payload.target = target;
      const direction = $("f-direction").value;
      if (direction) payload.direction = direction;
      const unit = $("f-unit").value;
      if (unit) payload.unit = unit;
      if (!$("f-tags").value.trim()) payload.clear_tags = true;
      if (!$("f-target").value.trim()) payload.clear_target = true;
      if (!direction) payload.clear_direction = true;
      if (!unit) payload.clear_unit = true;
      return payload;
    }

    const payload = {
      name: $("f-name").value.trim(),
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
    return payload;
  }

  function setCreateOnlyFieldsVisible(visible) {
    for (const el of document.querySelectorAll("#kpi-form .create-only")) {
      el.hidden = !visible;
    }
    $("f-edit-note").hidden = visible;
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
    setCreateOnlyFieldsVisible(true);
    $("f-name").parentElement.hidden = false;
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
    setCreateOnlyFieldsVisible(false);
    $("f-name").parentElement.hidden = true;
    $("f-name").disabled = true;
    $("f-new-name-wrap").hidden = false;
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

  function paneFiltersActive() {
    return Boolean(
      $("filter-owner").value ||
      $("filter-grain").value ||
      $("filter-target").value ||
      ($("filter-mode").value && $("filter-mode").value !== "stored")
    );
  }

  function setFilterMenuOpen(open) {
    const menu = $("filter-menu");
    const btn = $("btn-filters");
    if (!menu || !btn) return;
    menu.classList.toggle("hidden", !open);
    btn.setAttribute("aria-expanded", open ? "true" : "false");
  }

  function syncFilterBadge() {
    $("btn-filters").classList.toggle("has-filters", paneFiltersActive());
  }

  async function refreshList() {
    const errEl = $("list-error");
    errEl.hidden = true;
    const owner = $("filter-owner").value;
    const tags = state.selectedTags;
    const grain = $("filter-grain").value;
    const targetFilter = $("filter-target").value;
    const mode = $("filter-mode").value;
    state.mode = mode;
    const qs = new URLSearchParams();
    if (owner) qs.set("owner", owner);
    qs.set("mode", mode);
    qs.set("values", "1");
    setListStatus("Loading…");
    try {
      const data = await api(`/api/kpis?${qs.toString()}`);
      let items = data.kpis || [];
      if (grain) {
        items = items.filter((k) => (k.grain || "daily") === grain);
      }
      if (targetFilter) {
        items = items.filter((k) => targetOutcome(k) === targetFilter);
      }
      const implemented = items.filter(isImplementedKpi);
      const tagUniverse = implemented.length ? implemented : items;
      renderTagFilter(facetTagsFromKpis(tagUniverse, tags));
      if (tags.length) {
        items = tagUniverse.filter((k) => kpiHasAllTags(k, tags));
      }
      data.kpis = items;
      data.count = items.length;
      data.filters = {
        ...(data.filters || {}),
        tags,
        grain: grain || null,
        target: targetFilter || null,
      };
      renderList(data);
      syncFilterBadge();
      if (state.selected) {
        for (const tr of $("kpi-table").querySelectorAll("tbody tr")) {
          if (tr.dataset.name === state.selected) tr.classList.add("active");
        }
      }
    } catch (err) {
      errEl.hidden = false;
      errEl.textContent = err.message || String(err);
      setListStatus("");
    }
  }

  async function selectKpi(name) {
    state.selected = name;
    for (const tr of $("kpi-table").querySelectorAll("tbody tr")) {
      tr.classList.toggle("active", tr.dataset.name === name);
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

  $("user-badge").addEventListener("click", (ev) => {
    ev.stopPropagation();
    setFilterMenuOpen(false);
    setUserMenuOpen($("user-menu").classList.contains("hidden"));
  });
  $("user-menu").addEventListener("click", (ev) => ev.stopPropagation());
  $("btn-filters").addEventListener("click", (ev) => {
    ev.stopPropagation();
    setUserMenuOpen(false);
    setFilterMenuOpen($("filter-menu").classList.contains("hidden"));
  });
  $("filter-menu").addEventListener("click", (ev) => ev.stopPropagation());
  document.addEventListener("click", () => {
    setUserMenuOpen(false);
    setFilterMenuOpen(false);
  });
  document.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape") {
      setUserMenuOpen(false);
      setFilterMenuOpen(false);
    }
  });
  $("btn-add").addEventListener("click", () => {
    setUserMenuOpen(false);
    openAddForm();
  });
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
  $("filter-grain").addEventListener("change", refreshList);
  $("filter-target").addEventListener("change", refreshList);
  $("filter-mode").addEventListener("change", () => {
    state.mode = $("filter-mode").value;
    syncFilterBadge();
    if (state.selected) selectKpi(state.selected);
    refreshList();
  });

  boot();
})();
