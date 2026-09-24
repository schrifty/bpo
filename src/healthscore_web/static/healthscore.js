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

  function initials(me) {
    const name = String(me.name || "").trim();
    if (name) return name.split(/\s+/).slice(0, 2).map((part) => part[0]).join("").toUpperCase();
    return String(me.email || "?").slice(0, 2).toUpperCase();
  }

  function showLogin(status, message) {
    $("hs-session").classList.add("hidden");
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

  function renderFrameworkSummary() {
    const framework = state.framework;
    $("hs-input-count").textContent = `${framework.input_count} inputs`;
    $("hs-override-count").textContent = `${framework.override_count} override flags`;
    $("hs-coverage-detail").textContent = `0 of ${framework.configured_weight} configured`;
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
    $("hs-score").textContent = score && score.score != null ? `${score.score}%` : "—";
    $("hs-coverage").textContent = score ? `${score.coverage_pct}%` : "0%";
    $("hs-coverage-detail").textContent = `${score ? score.covered_weight : 0} of ${state.framework.configured_weight} configured`;
    $("hs-score-trend").innerHTML = score ? sparkline(score.history, "score") : "";
    $("hs-components-body").innerHTML = components
      .map((component) => {
        const latest = component.latest;
        const points =
          latest && latest.points != null
            ? `${latest.points}${component.max_points != null ? ` / ${component.max_points}` : ""}`
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
        const raised = Boolean(item.latest && item.latest.raw_value);
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
    return observations.filter((row) => row.component_key === key);
  }

  function sparkline(rows, field) {
    const values = (rows || [])
      .map((row) => ({ date: row.date || row.period_date, value: Number(row[field] ?? row.points) }))
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
          <input id="hs-entry-points" type="number" min="0" ${definition.max_points != null ? `max="${definition.max_points}"` : ""} step="0.01" value="${latest?.points ?? ""}" />
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
        <dt>Definition</dt><dd>${esc(definition.definition)}</dd>
        ${definition.metric ? `<dt>Measurable metric</dt><dd>${esc(definition.metric)}</dd>` : ""}
        ${definition.scoring ? `<dt>Scoring rule</dt><dd>${esc(definition.scoring)}</dd>` : ""}
        <dt>Data source</dt><dd>${esc(definition.data_source)}</dd>
        <dt>Refresh cadence</dt><dd>${esc(definition.cadence)}</dd>
        ${definition.notes ? `<dt>Open issue</dt><dd class="error">${esc(definition.notes)}</dd>` : ""}
        <dt>Latest reading</dt><dd>${latest ? `${esc(String(latest.raw_value ?? "—"))} · ${latest.points ?? "unscored"} points · ${esc(latest.source_mode)}<br><span class="muted">${esc(latest.period_date)} by ${esc(latest.entered_by || "unknown")}</span>` : '<span class="muted">No reading yet</span>'}</dd>
      </dl>
      ${sparkline(history, "points")}
      ${history.length ? `<table class="hs-history"><thead><tr><th>Date</th><th>Value</th><th>Points</th><th>Mode</th></tr></thead><tbody>${history.map((row) => `<tr><td>${esc(row.period_date)}</td><td>${esc(String(row.raw_value ?? "—"))}</td><td>${row.points ?? "—"}</td><td>${esc(row.source_mode)}</td></tr>`).join("")}</tbody></table>` : ""}
      <form id="hs-entry-form" class="hs-entry-form">
        <h3>Record manual reading</h3>
        <label>Period date<input id="hs-entry-date" type="date" value="${new Date().toISOString().slice(0, 10)}" required /></label>
        <label>${isOverride ? "Flag raised (true/false)" : "Raw value"}<input id="hs-entry-value" type="text" value="${esc(latest?.raw_value ?? "")}" /></label>
        ${pointsField}
        <label>Note<textarea id="hs-entry-note">${esc(latest?.note ?? "")}</textarea></label>
        <button type="submit" ${state.entity ? "" : "disabled"}>Save manual reading</button>
        <p id="hs-entry-error" class="error" hidden></p>
      </form>`;
    $("hs-entry-form").addEventListener("submit", saveReading);
  }

  function parseRawValue(value) {
    const text = String(value || "").trim();
    if (/^(true|false)$/i.test(text)) return text.toLowerCase() === "true";
    const number = Number(text);
    return text && Number.isFinite(number) ? number : text || null;
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
          period_date: $("hs-entry-date").value,
          raw_value: parseRawValue($("hs-entry-value").value),
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
    $("hs-session").classList.remove("hidden");
    try {
      state.me = await api("/api/me");
      $("hs-user-initials").textContent = initials(state.me);
      const [frameworkPayload, entitiesPayload] = await Promise.all([
        api("/healthscore/api/framework"),
        api("/healthscore/api/entities"),
      ]);
      state.framework = frameworkPayload.framework;
      state.entities = entitiesPayload.entities;
      renderFrameworkSummary();
      renderEntities(entitiesPayload.source);
      renderScore();
    } catch (error) {
      showError(error.message);
    }
  }

  $("hs-entity").addEventListener("change", async (event) => {
    state.entity = state.entities.find((row) => row.id === event.target.value) || null;
    state.selected = null;
    await loadScore();
  });

  boot();
})();
