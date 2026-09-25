(() => {
  const $ = (id) => document.getElementById(id);
  const state = {
    framework: null,
    entities: [],
    entity: null,
    score: null,
    selected: null,
    me: null,
  };

  function esc(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  async function api(path, options = {}) {
    const response = await fetch(path, {
      credentials: "same-origin",
      ...options,
      headers: {
        ...(options.body ? { "Content-Type": "application/json" } : {}),
        ...(options.headers || {}),
      },
    });
    let payload = null;
    try {
      payload = await response.json();
    } catch (_) {
      payload = null;
    }
    if (!response.ok) {
      throw new Error((payload && payload.error) || `${response.status} ${response.statusText}`);
    }
    return payload;
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

  function renderUser(me) {
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
    const badgeInitials = $("user-badge-initials");
    if (me.picture) {
      img.src = me.picture;
      img.hidden = false;
      badgeInitials.hidden = true;
    } else {
      img.removeAttribute("src");
      img.hidden = true;
      badgeInitials.hidden = false;
      badgeInitials.textContent = userInitials(me);
    }
    setUserMenuOpen(false);
  }

  function showLogin(status, message) {
    setUserMenuOpen(false);
    $("session").classList.add("hidden");
    $("hs-app").classList.add("hidden");
    $("hs-login").classList.remove("hidden");
    const actions = $("hs-login-actions");
    actions.innerHTML = "";
    if (status.google_configured) {
      actions.innerHTML += '<a href="/auth/login?next=/healthscore"><button type="button">Sign in with Google</button></a>';
    }
    if (status.dev_auth_enabled) {
      actions.innerHTML += '<a href="/auth/dev-login?next=/healthscore"><button type="button">Dev login</button></a>';
    }
    if (message) {
      $("hs-login-error").hidden = false;
      $("hs-login-error").textContent = message;
    }
  }

  function showError(message) {
    const error = $("hs-global-error");
    error.hidden = !message;
    error.textContent = message || "";
  }

  function sourceBadge(mode) {
    const value = String(mode || "unknown");
    return `<span class="hs-source-badge"><i class="dot ${esc(value)}"></i>${esc(value)}</span>`;
  }

  function renderEntities(source) {
    const select = $("hs-entity");
    select.innerHTML = '<option value="">Choose an active entity…</option>';
    for (const entity of state.entities) {
      const option = document.createElement("option");
      option.value = entity.id;
      option.textContent = entity.name;
      select.appendChild(option);
    }
    $("hs-entity-source").textContent = `${source} · ${state.entities.length} active`;
  }

  function influenceCell(component) {
    const contribution = component.contribution;
    const weight = component.weight;
    if (contribution == null || weight == null) return '<span class="muted">Unscored</span>';
    const pct = weight ? Math.max(0, Math.min(100, (contribution / weight) * 100)) : 0;
    return `<div class="hs-influence">${contribution.toFixed(2)} / ${weight}
      <div class="hs-influence-track"><span class="hs-influence-fill" style="width:${pct}%"></span></div>
    </div>`;
  }

  function renderScore() {
    const score = state.score;
    const components = score ? score.components : state.framework.inputs.map((row) => ({ ...row, latest: null, contribution: null }));
    $("hs-components-body").innerHTML = components
      .map((component) => {
        const latest = component.latest;
        const points =
          latest && latest.effective_points != null
            ? `${latest.effective_points}${component.max_points != null ? ` / ${component.max_points}` : ""}`
            : "—";
        return `<tr data-component="${esc(component.key)}">
          <td><div class="hs-component-name">${esc(component.name)}</div><span class="hs-status">${esc(component.signal)}</span></td>
          <td>${esc(component.pillar)}</td>
          <td>${component.weight == null ? '<span class="hs-status">TBD</span>' : `${component.weight}%`}</td>
          <td>${points}</td>
          <td>${influenceCell(component)}</td>
          <td>${sourceBadge(latest ? latest.source_mode : component.automation)}</td>
        </tr>`;
      })
      .join("");
    for (const row of $("hs-components-body").querySelectorAll("tr")) {
      row.addEventListener("click", () => selectComponent(row.dataset.component));
    }
    const overrides = score ? score.overrides : state.framework.overrides.map((row) => ({ ...row, latest: null }));
    $("hs-overrides-list").innerHTML = overrides
      .map((item) => {
        const raised = Boolean(item.latest && item.latest.effective_value);
        return `<button type="button" class="hs-override${raised ? " raised" : ""}" data-component="${esc(item.key)}">${esc(item.name)}${raised ? " · raised" : ""}</button>`;
      })
      .join("");
    for (const button of $("hs-overrides-list").querySelectorAll("button")) {
      button.addEventListener("click", () => selectComponent(button.dataset.component));
    }
    if (state.selected) selectComponent(state.selected, false);
  }

  function componentDefinition(key) {
    return [...state.framework.inputs, ...state.framework.overrides].find((row) => row.key === key);
  }

  function componentLatest(key) {
    if (!state.score) return null;
    const rows = [...state.score.components, ...state.score.overrides];
    return rows.find((row) => row.key === key)?.latest || null;
  }

  function componentHistory(key) {
    const observations = state.score?.observations || [];
    return observations.filter((row) => row.metric_name === key);
  }

  function sparkline(rows, field) {
    const values = (rows || [])
      .map((row) => ({ date: row.period_key, value: Number(row[field] ?? row.effective_points) }))
      .filter((row) => row.date && Number.isFinite(row.value))
      .sort((a, b) => a.date.localeCompare(b.date));
    if (values.length < 2) return "";
    const width = 280;
    const height = 88;
    const min = Math.min(...values.map((row) => row.value));
    const max = Math.max(...values.map((row) => row.value));
    const span = max === min ? 1 : max - min;
    const points = values
      .map((row, index) => {
        const x = 8 + (index / (values.length - 1)) * (width - 16);
        const y = 8 + (1 - (row.value - min) / span) * (height - 24);
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(" ");
    return `<svg class="hs-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Score history">
      <polyline class="hs-chart-line" points="${points}"></polyline>
      <text x="8" y="${height - 3}" class="chart-label">${esc(values[0].date)}</text>
      <text x="${width - 8}" y="${height - 3}" text-anchor="end" class="chart-label">${esc(values[values.length - 1].date)}</text>
    </svg>`;
  }

  function selectComponent(key, markActive = true) {
    state.selected = key;
    const definition = componentDefinition(key);
    if (!definition) return;
    if (markActive) {
      for (const row of $("hs-components-body").querySelectorAll("tr")) {
        row.classList.toggle("active", row.dataset.component === key);
      }
    }
    const latest = componentLatest(key);
    const history = componentHistory(key);
    const isOverride = !Object.prototype.hasOwnProperty.call(definition, "pillar");
    const editBtn = isAdmin()
      ? `<button type="button" id="hs-detail-edit" class="icon-btn" title="${isOverride ? "Edit name" : "Edit input, pillar, and weight"}" aria-label="Edit ${esc(definition.name)}">
           <svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M3 17.25V21h3.75L17.8 9.94l-3.75-3.75L3 17.25zM20.7 7.04a1 1 0 0 0 0-1.41l-2.34-2.34a1 1 0 0 0-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg>
         </button>`
      : "";
    $("hs-detail").innerHTML = `
      <div class="detail-head">
        <h2>${esc(definition.name)}</h2>
        ${editBtn}
      </div>
      <div class="hs-meta-row">
        ${definition.signal ? `<span class="hs-status">${esc(definition.signal)}</span>` : '<span class="hs-status">override</span>'}
        ${sourceBadge(definition.automation)}
        <span class="hs-status">${esc(definition.status)}</span>
        ${isOverride ? "" : definition.weight == null ? '<span class="hs-status">weight TBD</span>' : `<span class="hs-status">${definition.weight}% weight</span>`}
      </div>
      <dl class="hs-detail-grid">
        ${isOverride ? "" : `<dt>Pillar</dt><dd>${esc(definition.pillar)}</dd>`}
        <dt>Description</dt><dd>${esc(definition.description)}</dd>
        ${definition.metric ? `<dt>Measurable metric</dt><dd>${esc(definition.metric)}</dd>` : ""}
        ${definition.scoring ? `<dt>Scoring rule</dt><dd>${esc(definition.scoring)}</dd>` : ""}
        <dt>Data source</dt><dd>${esc(definition.data_source)}</dd>
        <dt>Owner</dt><dd>${esc(definition.owner)}</dd>
        <dt>Grain</dt><dd>${esc(definition.grain || definition.cadence_note || "not defined")}</dd>
        ${definition.notes ? `<dt>Open issue</dt><dd class="error">${esc(definition.notes)}</dd>` : ""}
        ${definition.grain ? `<dt>${isOverride ? "Flag raised" : "Points"}${latest ? ` <span class="hs-period">${esc(latest.period_key)}</span>` : ""}</dt><dd id="hs-points-cell">${pointsCell(definition, latest)}</dd>` : ""}
      </dl>
      ${sparkline(history, "effective_points")}
      ${history.length ? `<table class="hs-history"><thead><tr><th>Period</th><th>Value</th><th>Points</th><th>Mode</th></tr></thead><tbody>${history.map((row) => `<tr><td>${esc(row.period_key)}</td><td>${esc(String(row.effective_value ?? "—"))}</td><td>${row.effective_points ?? "—"}</td><td>${esc(row.source_mode)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${definition.grain ? "" : `<p class="muted">No grain yet — the framework defines this as ${esc(definition.cadence_note || "an undefined cadence")}, so readings cannot be stored against a period.</p>`}
      <p id="hs-entry-error" class="error" hidden></p>`;
    const edit = $("hs-detail-edit");
    if (edit) edit.addEventListener("click", () => openComponentForm(definition));
    if (definition.grain) wirePointsCell(definition, latest);
  }

  function isAdmin() {
    return Boolean(state.me && state.me.is_catalog_admin);
  }

  function overrideField(definition) {
    return Object.prototype.hasOwnProperty.call(definition, "pillar") ? "points" : "value";
  }

  function fmtNum(value) {
    if (value == null || value === "") return "—";
    const n = Number(value);
    return Number.isFinite(n) ? String(n) : String(value);
  }

  function overrideTip(definition, latest) {
    const field = overrideField(definition);
    const generated = latest[field];
    const generatedText = generated == null ? (latest.error ? `error: ${latest.error}` : "none") : fmtNum(generated);
    const who = latest.override_by ? ` by ${latest.override_by}` : "";
    const when = latest.override_at ? ` on ${String(latest.override_at).slice(0, 10)}` : "";
    const note = latest.override_note ? ` — ${latest.override_note}` : "";
    return `Manual override${who}${when} — generated ${field}: ${generatedText}${note}`;
  }

  function pointsCell(definition, latest) {
    const field = overrideField(definition);
    const max = field === "points" && definition.max_points != null ? ` / ${definition.max_points}` : "";
    if (!state.entity) return '<span class="muted">Choose an entity to record a reading</span>';
    if (latest && latest.overridden) {
      return `<span class="value-wrap"><span class="value-override hs-points-value" title="${esc(overrideTip(definition, latest))}">${esc(fmtNum(latest[`override_${field}`]))}${max}</span><button type="button" class="value-restore" title="Show the generated reading again">Restore</button></span>`;
    }
    const current = latest ? latest[field] : null;
    const shown = current == null ? "—" : `${fmtNum(current)}${max}`;
    const tip = latest && latest.generator ? `Generated by ${latest.generator} · click to override` : "Click to record a manual reading";
    return `<span class="value-wrap"><span class="value-ok hs-points-value" title="${esc(tip)}">${esc(shown)}</span></span>`;
  }

  function wirePointsCell(definition, latest) {
    const cell = $("hs-points-cell");
    if (!cell || !state.entity) return;
    const value = cell.querySelector(".hs-points-value");
    if (value) value.addEventListener("click", () => beginPointsEdit(cell, definition, latest));
    const restore = cell.querySelector(".value-restore");
    if (restore) restore.addEventListener("click", () => savePoints(definition, latest, null));
  }

  function beginPointsEdit(cell, definition, latest) {
    if (cell.querySelector("input")) return;
    const field = overrideField(definition);
    const prior = latest && latest.overridden ? fmtNum(latest[`override_${field}`]) : latest && latest[field] != null ? fmtNum(latest[field]) : "";
    const max = field === "points" && definition.max_points != null ? definition.max_points : field === "value" ? 1 : null;
    cell.innerHTML = `<input class="value-input" type="text" inputmode="decimal" aria-label="Override ${field} for ${esc(definition.name)}" title="Clear the box to drop the override and show the generated reading" /><span class="muted hs-points-hint">${max != null ? `0–${max} · ` : ""}Enter saves · Esc cancels</span>`;
    const input = cell.querySelector("input");
    input.value = prior === "—" ? "" : prior;
    input.focus();
    input.select();
    let done = false;
    const finish = async (save) => {
      if (done) return;
      done = true;
      const typed = input.value.trim();
      if (!save || typed === (prior === "—" ? "" : prior)) {
        cell.innerHTML = pointsCell(definition, latest);
        wirePointsCell(definition, latest);
        return;
      }
      let n = null;
      if (typed) {
        n = Number(typed);
        if (!Number.isFinite(n)) {
          cell.innerHTML = pointsCell(definition, latest);
          wirePointsCell(definition, latest);
          showEntryError(`Invalid number: ${typed}`);
          return;
        }
      }
      await savePoints(definition, latest, n);
    };
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        finish(true);
      } else if (event.key === "Escape") {
        event.preventDefault();
        finish(false);
      }
    });
    input.addEventListener("blur", () => finish(true));
  }

  function showEntryError(message) {
    const error = $("hs-entry-error");
    if (!error) return;
    error.hidden = !message;
    error.textContent = message || "";
  }

  async function savePoints(definition, latest, number) {
    if (!state.entity) return;
    const field = overrideField(definition);
    const body = { note: null };
    if (latest && latest.period_key) body.period_key = latest.period_key;
    else body.as_of = new Date().toISOString().slice(0, 10);
    body[field] = number;
    showEntryError("");
    try {
      await api(`/healthscore/api/entities/${encodeURIComponent(state.entity.id)}/components/${encodeURIComponent(definition.key)}`, {
        method: "PUT",
        body: JSON.stringify(body),
      });
      await loadScore();
    } catch (saveError) {
      const cell = $("hs-points-cell");
      if (cell) {
        cell.innerHTML = pointsCell(definition, latest);
        wirePointsCell(definition, latest);
      }
      showEntryError(saveError.message);
    }
  }

  function pillarOptions() {
    const names = [...new Set(state.framework.inputs.map((row) => row.pillar).filter(Boolean))];
    return names.map((name) => `<option value="${esc(name)}"></option>`).join("");
  }

  function openComponentForm(definition) {
    if (!isAdmin()) return;
    const isOverride = !Object.prototype.hasOwnProperty.call(definition, "pillar");
    const dialog = $("hs-component-dialog");
    $("hs-form-title").textContent = `Edit: ${definition.name}`;
    $("hs-form-error").hidden = true;
    $("hs-f-name").value = definition.name || "";
    $("hs-f-pillar").value = definition.pillar || "";
    $("hs-f-weight").value = definition.weight == null ? "" : String(definition.weight);
    $("hs-f-pillar-wrap").hidden = isOverride;
    $("hs-f-weight-wrap").hidden = isOverride;
    $("hs-pillar-options").innerHTML = pillarOptions();
    dialog.dataset.component = definition.key;
    dialog.showModal();
  }

  async function submitComponentForm(event) {
    event.preventDefault();
    const dialog = $("hs-component-dialog");
    const key = dialog.dataset.component;
    const definition = componentDefinition(key);
    if (!definition) return;
    const isOverride = !Object.prototype.hasOwnProperty.call(definition, "pillar");
    const payload = { name: $("hs-f-name").value.trim() };
    if (!isOverride) {
      payload.pillar = $("hs-f-pillar").value.trim();
      const weight = $("hs-f-weight").value.trim();
      payload.weight = weight === "" ? null : Number(weight);
      if (weight !== "" && !Number.isFinite(payload.weight)) {
        $("hs-form-error").hidden = false;
        $("hs-form-error").textContent = `Invalid weight: ${weight}`;
        return;
      }
    }
    try {
      const result = await api(`/healthscore/api/framework/components/${encodeURIComponent(key)}`, {
        method: "PUT",
        body: JSON.stringify(payload),
      });
      state.framework = result.framework;
      dialog.close();
      await loadScore();
    } catch (saveError) {
      $("hs-form-error").hidden = false;
      $("hs-form-error").textContent = saveError.message;
    }
  }

  async function loadScore() {
    if (!state.entity) {
      state.score = null;
      renderScore();
      return;
    }
    showError("");
    try {
      const payload = await api(`/healthscore/api/entities/${encodeURIComponent(state.entity.id)}/score`);
      state.score = payload;
      renderScore();
    } catch (error) {
      showError(error.message);
    }
  }

  async function boot() {
    let auth;
    try {
      auth = await api("/auth/status");
    } catch (error) {
      showLogin({ google_configured: false, dev_auth_enabled: false }, error.message);
      return;
    }
    if (!auth.authenticated) {
      showLogin(auth, auth.error);
      return;
    }
    $("hs-login").classList.add("hidden");
    $("hs-app").classList.remove("hidden");
    $("session").classList.remove("hidden");
    try {
      state.me = await api("/api/me");
      renderUser(state.me);
      const [frameworkPayload, entitiesPayload] = await Promise.all([
        api("/healthscore/api/framework"),
        api("/healthscore/api/entities"),
      ]);
      state.framework = frameworkPayload.framework;
      state.entities = entitiesPayload.entities;
      renderEntities(entitiesPayload.source);
      restoreEntitySelection();
      await loadScore();
    } catch (error) {
      showError(error.message);
    }
  }

  const ENTITY_STORAGE_KEY = "cortex.healthscore.entity";

  function rememberEntity(entityId) {
    const url = new URL(window.location.href);
    if (entityId) {
      url.searchParams.set("entity", entityId);
      try {
        window.localStorage.setItem(ENTITY_STORAGE_KEY, entityId);
      } catch (_) {
        /* storage unavailable */
      }
    } else {
      url.searchParams.delete("entity");
      try {
        window.localStorage.removeItem(ENTITY_STORAGE_KEY);
      } catch (_) {
        /* storage unavailable */
      }
    }
    window.history.replaceState(null, "", url);
  }

  function restoreEntitySelection() {
    let wanted = new URL(window.location.href).searchParams.get("entity");
    if (!wanted) {
      try {
        wanted = window.localStorage.getItem(ENTITY_STORAGE_KEY);
      } catch (_) {
        wanted = null;
      }
    }
    const entity = wanted ? state.entities.find((row) => row.id === wanted) : null;
    state.entity = entity || null;
    $("hs-entity").value = entity ? entity.id : "";
    rememberEntity(entity ? entity.id : null);
  }

  $("hs-entity").addEventListener("change", async (event) => {
    state.entity = state.entities.find((row) => row.id === event.target.value) || null;
    state.selected = null;
    rememberEntity(state.entity ? state.entity.id : null);
    await loadScore();
  });
  $("hs-component-form").addEventListener("submit", submitComponentForm);
  $("hs-form-cancel").addEventListener("click", () => $("hs-component-dialog").close());
  $("user-badge").addEventListener("click", (event) => {
    event.stopPropagation();
    setUserMenuOpen($("user-menu").classList.contains("hidden"));
  });
  $("user-menu").addEventListener("click", (event) => event.stopPropagation());
  document.addEventListener("click", () => setUserMenuOpen(false));
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") setUserMenuOpen(false);
  });

  boot();
})();
