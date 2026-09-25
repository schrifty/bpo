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
    const pointsField = isOverride
      ? ""
      : `<label>Points${definition.max_points != null ? ` (0–${definition.max_points})` : " (rule unresolved)"}
          <input id="hs-entry-points" type="number" min="0" ${definition.max_points != null ? `max="${definition.max_points}"` : ""} step="0.01" value="${latest?.effective_points ?? ""}" />
        </label>`;
    $("hs-detail").innerHTML = `
      <h2>${esc(definition.name)}</h2>
      <div class="hs-meta-row">
        ${definition.signal ? `<span class="hs-status">${esc(definition.signal)}</span>` : '<span class="hs-status">override</span>'}
        ${sourceBadge(definition.automation)}
        <span class="hs-status">${esc(definition.status)}</span>
        ${definition.weight == null ? '<span class="hs-status">weight TBD</span>' : `<span class="hs-status">${definition.weight}% weight</span>`}
      </div>
      <dl class="hs-detail-grid">
        <dt>Description</dt><dd>${esc(definition.description)}</dd>
        ${definition.metric ? `<dt>Measurable metric</dt><dd>${esc(definition.metric)}</dd>` : ""}
        ${definition.scoring ? `<dt>Scoring rule</dt><dd>${esc(definition.scoring)}</dd>` : ""}
        <dt>Data source</dt><dd>${esc(definition.data_source)}</dd>
        <dt>Owner</dt><dd>${esc(definition.owner)}</dd>
        <dt>Grain</dt><dd>${esc(definition.grain || definition.cadence_note || "not defined")}</dd>
        ${definition["metric-generator"] ? `<dt>Generator</dt><dd><code>${esc(definition["metric-generator"])}</code></dd>` : ""}
        ${definition.notes ? `<dt>Open issue</dt><dd class="error">${esc(definition.notes)}</dd>` : ""}
        <dt>Latest reading</dt><dd>${latest ? `${esc(String(latest.effective_value ?? "—"))} · ${latest.effective_points ?? "unscored"} points · ${esc(latest.source_mode)}<br><span class="muted">${esc(latest.period_key)} by ${esc(latest.entered_by || "unknown")}</span>` : '<span class="muted">No reading yet</span>'}</dd>
      </dl>
      ${sparkline(history, "effective_points")}
      ${history.length ? `<table class="hs-history"><thead><tr><th>Period</th><th>Value</th><th>Points</th><th>Mode</th></tr></thead><tbody>${history.map((row) => `<tr><td>${esc(row.period_key)}</td><td>${esc(String(row.effective_value ?? "—"))}</td><td>${row.effective_points ?? "—"}</td><td>${esc(row.source_mode)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${definition.grain ? `<form id="hs-entry-form" class="hs-entry-form">
        <h3>Record manual reading</h3>
        <p class="muted">Overrides the generated reading for the ${esc(definition.grain)} period containing this date; the generated value is kept.</p>
        <label>Date in period<input id="hs-entry-date" type="date" value="${new Date().toISOString().slice(0, 10)}" required /></label>
        <label>${isOverride ? "Flag raised (1/0)" : "Value"}<input id="hs-entry-value" type="number" step="any" value="${esc(latest?.override_value ?? "")}" /></label>
        ${pointsField}
        <label>Note<textarea id="hs-entry-note">${esc(latest?.note ?? "")}</textarea></label>
        <button type="submit" ${state.entity ? "" : "disabled"}>Save manual reading</button>
        <p id="hs-entry-error" class="error" hidden></p>
      </form>` : `<p class="muted">No grain yet — the framework defines this as ${esc(definition.cadence_note || "an undefined cadence")}, so readings cannot be stored against a period.</p>`}`;
    if (!definition.grain) return;
    $("hs-entry-form").addEventListener("submit", saveReading);
  }

  async function saveReading(event) {
    event.preventDefault();
    if (!state.entity || !state.selected) return;
    const error = $("hs-entry-error");
    error.hidden = true;
    const pointsInput = $("hs-entry-points");
    try {
      await api(`/healthscore/api/entities/${encodeURIComponent(state.entity.id)}/components/${encodeURIComponent(state.selected)}`, {
        method: "PUT",
        body: JSON.stringify({
          as_of: $("hs-entry-date").value,
          value: $("hs-entry-value").value === "" ? null : Number($("hs-entry-value").value),
          points: pointsInput && pointsInput.value !== "" ? Number(pointsInput.value) : null,
          note: $("hs-entry-note").value,
        }),
      });
      await loadScore();
    } catch (saveError) {
      error.hidden = false;
      error.textContent = saveError.message;
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
